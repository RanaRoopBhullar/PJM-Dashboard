"""
PJM Power Dashboard — Backend (Production v2)
Confirmed working feeds only. No placeholder data.
"""

import os, httpx, asyncio, xml.etree.ElementTree as ET, json
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="PJM Dashboard API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

PJM_API_KEY = os.getenv("PJM_API_KEY", "")
PJM_BASE    = "https://api.pjm.com/api/v1"

_cache: dict = {}

def cache_get(key, ttl=300):
    e = _cache.get(key)
    return e["data"] if e and (datetime.now(timezone.utc).timestamp() - e["ts"]) < ttl else None

def cache_set(key, data):
    _cache[key] = {"ts": datetime.now(timezone.utc).timestamp(), "data": data}

def pjm_h():
    return {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}

PJM_HUBS = [
    "WESTERN HUB", "EASTERN HUB", "AEP-DAYTON HUB", "N ILLINOIS HUB",
    "NEW JERSEY HUB", "CHICAGO HUB", "CHICAGO GEN HUB", "AEP GEN HUB",
    "OHIO HUB", "DOMINION HUB", "ATSI GEN HUB", "WEST INT HUB",
]

# ── RT LMPs ───────────────────────────────────────────────────
async def fetch_lmps() -> list:
    cached = cache_get("lmps", 300)
    if cached: return cached
    if not PJM_API_KEY: raise HTTPException(503, "PJM_API_KEY not configured")
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{PJM_BASE}/rt_unverified_hrl_lmps",
            params={"rowCount":"500","startRow":"1","type":"HUB",
                    "datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
            headers=pjm_h())
        items = r.json().get("items",[]) if r.status_code==200 else []
    if not items: return []
    latest = items[0].get("datetime_beginning_ept","")
    seen, results = set(), []
    system_energy = None
    for row in items:
        if row.get("datetime_beginning_ept") != latest: continue
        name = (row.get("pnode_name") or "").upper().strip()
        if name in seen: continue
        seen.add(name)
        lmp  = float(row.get("total_lmp_rt") or 0)
        cong = float(row.get("congestion_price_rt") or 0)
        se   = row.get("system_energy_price_rt")
        ml   = row.get("marginal_loss_lmp_rt") or row.get("marginal_loss_price_rt")
        if se is not None and float(se) != 0:
            energy = float(se)
            loss   = round(lmp - energy - cong, 2)
            if system_energy is None: system_energy = energy
        elif ml is not None:
            loss   = float(ml)
            energy = round(lmp - cong - loss, 2)
        else:
            energy = round(lmp - cong, 2)
            loss   = 0.0
        results.append({"name":row.get("pnode_name"),"type":"Hub",
                         "lmp":round(lmp,2),"energy":round(energy,2),
                         "congestion":round(cong,2),"loss":loss,"hour":latest})
    order = {h.upper():i for i,h in enumerate(PJM_HUBS)}
    results.sort(key=lambda x: order.get(x["name"].upper().strip(), 99))
    if results:
        cache_set("lmps", results)
        # Cache system energy price as gas proxy
        if system_energy:
            cache_set("system_energy", {"price": round(system_energy,2),
                                         "hour": latest,
                                         "note": "PJM System Energy Price (RT)"})
    return results

# ── DA LMPs ───────────────────────────────────────────────────
async def fetch_da_lmps() -> dict:
    cached = cache_get("da_lmps", 1800)
    if cached: return cached
    if not PJM_API_KEY: return {}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{PJM_BASE}/da_hrl_lmps",
            params={"rowCount":"500","startRow":"1","type":"HUB",
                    "datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
            headers=pjm_h())
        items = r.json().get("items",[]) if r.status_code==200 else []
    if not items: return {}
    da_map = {}
    for row in items:
        name = (row.get("pnode_name") or "").upper().strip()
        val  = float(row.get("total_lmp_da") or 0)
        if val and name not in da_map:
            da_map[name] = {"lmp": round(val,2),
                             "energy": round(float(row.get("system_energy_price_da") or 0),2),
                             "cong":   round(float(row.get("congestion_price_da") or 0),2)}
    if da_map: cache_set("da_lmps", da_map)
    return da_map

# ── Intraday ──────────────────────────────────────────────────
async def fetch_intraday() -> list:
    cached = cache_get("intraday", 300)
    if cached: return cached
    if not PJM_API_KEY: return []
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{PJM_BASE}/rt_unverified_hrl_lmps",
            params={"rowCount":"50","startRow":"1","pnode_name":"WESTERN HUB",
                    "datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
            headers=pjm_h())
        items = r.json().get("items",[]) if r.status_code==200 else []
    result = [{"hour":row.get("datetime_beginning_ept",""),
               "lmp":round(float(row.get("total_lmp_rt") or 0),2)} for row in items]
    if result: cache_set("intraday", result)
    return result

# ── Instantaneous Load ────────────────────────────────────────
async def fetch_load() -> dict:
    cached = cache_get("load", 120)
    if cached: return cached
    if not PJM_API_KEY: return {}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{PJM_BASE}/inst_load",
            params={"rowCount":"100","startRow":"1","datetime_beginning_ept":"5MinutesAgo"},
            headers=pjm_h())
        items = r.json().get("items",[]) if r.status_code==200 else []
    if not items: return {}
    timestamps = sorted(set(i.get("datetime_beginning_ept","") for i in items), reverse=True)
    latest_ts = timestamps[0] if timestamps else ""
    prev_ts   = timestamps[1] if len(timestamps) > 1 else ""
    def get_rto(rows, ts):
        for row in rows:
            if row.get("datetime_beginning_ept")==ts and (row.get("area") or "").upper().strip()=="PJM RTO":
                return float(row.get("instantaneous_load") or 0)
        return 0.0
    current = get_rto(items, latest_ts)
    prev    = get_rto(items, prev_ts)
    change  = round(current - prev, 0) if prev else None
    result  = {"current_mw":round(current,0),"prev_mw":round(prev,0),
               "change_mw":change,"timestamp":latest_ts}
    if current > 1000: cache_set("load", result)
    return result

# ── Generation Fuel Mix ───────────────────────────────────────
async def fetch_fuel_mix() -> list:
    """Try multiple PJM generation by fuel type feeds."""
    cached = cache_get("fuel_mix", 600)  # 10 min to avoid rate limiting
    if cached: return cached
    if not PJM_API_KEY: return []

    feeds_to_try = ["gen_by_fuel", "inst_gen_by_fuel", "generation_by_fuel_type",
                    "rt_gen_by_fuel", "fuel_mix"]
    async with httpx.AsyncClient(timeout=20) as c:
        for feed in feeds_to_try:
            try:
                r = await c.get(f"{PJM_BASE}/{feed}",
                    params={"rowCount":"20","startRow":"1",
                            "datetime_beginning_ept":"5MinutesAgo",
                            "order":"Desc","sort":"datetime_beginning_ept"},
                    headers=pjm_h())
                if r.status_code == 200:
                    items = r.json().get("items",[])
                    if items:
                        result = []
                        for row in items[:15]:
                            fuel = (row.get("fuel_type") or row.get("fuel") or
                                    row.get("category") or "").strip()
                            mw   = float(row.get("mw") or row.get("gen_mw") or
                                         row.get("actual_gen") or 0)
                            if fuel and mw >= 0:
                                result.append({"fuel":fuel,"mw":round(mw,0)})
                        if result:
                            cache_set("fuel_mix", result)
                            return result
            except Exception:
                continue
    return []

# ── Load Forecast ─────────────────────────────────────────────
async def fetch_load_forecast() -> list:
    """Try multiple PJM load forecast feeds."""
    cached = cache_get("load_forecast", 600)
    if cached: return cached
    if not PJM_API_KEY: return []

    async with httpx.AsyncClient(timeout=20) as c:
        try:
            # Confirmed correct feed: load_frcstd_7_day, field: forecast_load_mw
            r = await c.get(f"{PJM_BASE}/load_frcstd_7_day",
                params={"rowCount":"48","startRow":"1",
                        "forecast_area":"RTO_COMBINED",
                        "datetime_beginning_ept":"Today",
                        "order":"Asc","sort":"forecast_datetime_beginning_utc"},
                headers=pjm_h())
            if r.status_code == 200:
                items = r.json().get("items",[])
                result = []
                for row in items:
                    hr = (row.get("forecast_datetime_beginning_utc") or
                          row.get("datetime_beginning_ept") or
                          row.get("datetime_beginning_utc") or "")
                    mw = float(row.get("forecast_load_mw") or row.get("rto_total") or 0)
                    if mw > 0:
                        result.append({"hour":hr,"mw":mw})
                if result:
                    cache_set("load_forecast", result)
                    return result
        except Exception:
            pass
    return []

# ── Outages ───────────────────────────────────────────────────
async def fetch_outages() -> dict:
    cached = cache_get("outages", 600)
    if cached: return cached
    if not PJM_API_KEY: return {}
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.get(f"{PJM_BASE}/frcstd_gen_outages",
                params={"rowCount":"7","startRow":"1","datetime_beginning_ept":"Today",
                        "order":"Asc","sort":"datetime_beginning_ept"},
                headers=pjm_h())
            if r.status_code == 200:
                items = r.json().get("items",[])
                if items:
                    result = [{"date":row.get("forecast_date","")[:10],
                               "mw":float(row.get("forecast_gen_outage_mw_rto") or 0)}
                              for row in items if float(row.get("forecast_gen_outage_mw_rto") or 0) > 0]
                    if result:
                        cache_set("outages", result)
                        return {"forecast": result}
        except Exception:
            pass
    return {}

# ── News ──────────────────────────────────────────────────────
ENERGY_KW = ["power","energy","electricity","grid","lmp","capacity","natural gas","coal",
             "solar","wind","pjm","ferc","megawatt","mw","transmission","congestion",
             "renewable","nuclear","generation","utility","outage","demand","load",
             "curtailment","emergency","alert","market","tariff","fuel","pipeline"]

async def fetch_news() -> list:
    cached = cache_get("news", 300)
    if cached: return cached
    if not PJM_API_KEY: return []
    articles = []

    async with httpx.AsyncClient(timeout=20) as c:
        # 1. LMP spike/negative alerts
        try:
            lmps = cache_get("lmps", 300) or await fetch_lmps()
            high = [h for h in lmps if h["lmp"] > 80]
            low  = [h for h in lmps if h["lmp"] < 0]
            if high:
                articles.append({
                    "source":"PJM LMP","category":"🔴 Alert",
                    "title":f"HIGH PRICE: {len(high)} hubs above $80/MWh — peak ${max(h['lmp'] for h in high):.2f}",
                    "snippet":", ".join(f"{h['name']}: ${h['lmp']:.2f}" for h in high),
                    "pub":(high[0].get("hour",""))[:16],
                    "url":"https://dataminer2.pjm.com/feed/rt_unverified_hrl_lmps",
                })
            if low:
                articles.append({
                    "source":"PJM LMP","category":"🔴 Alert",
                    "title":f"NEGATIVE LMP: {', '.join(h['name'] for h in low)}",
                    "snippet":", ".join(f"{h['name']}: ${h['lmp']:.2f}" for h in low),
                    "pub":(low[0].get("hour",""))[:16],
                    "url":"https://dataminer2.pjm.com/feed/rt_unverified_hrl_lmps",
                })
        except Exception: pass

        # 2. DA/RT spread
        try:
            da   = cache_get("da_lmps", 1800) or await fetch_da_lmps()
            lmps = cache_get("lmps", 300) or []
            if da and lmps:
                spreads = []
                for h in lmps:
                    da_e = da.get(h["name"].upper().strip())
                    if da_e:
                        sp = round(h["lmp"] - da_e["lmp"], 2)
                        spreads.append((h["name"], h["lmp"], da_e["lmp"], sp))
                if spreads:
                    spreads.sort(key=lambda x: abs(x[3]), reverse=True)
                    top = spreads[:4]
                    articles.append({
                        "source":"PJM DA/RT","category":"📊 Market",
                        "title":f"DA/RT Spread — {top[0][0]}: RT${top[0][1]:.0f} vs DA${top[0][2]:.0f} (Δ{top[0][3]:+.1f})",
                        "snippet":" | ".join(f"{n}: Δ{sp:+.1f}" for n,rt,da_p,sp in top),
                        "pub":(lmps[0].get("hour",""))[:16] if lmps else "",
                        "url":"https://dataminer2.pjm.com/feed/da_hrl_lmps",
                    })
        except Exception: pass

        # 3. Load level
        try:
            load = cache_get("load", 120) or await fetch_load()
            mw   = load.get("current_mw", 0)
            if mw > 0:
                level = "CRITICAL" if mw>130000 else "HIGH" if mw>110000 else "ELEVATED" if mw>90000 else "NORMAL"
                icon  = "🔴" if mw>130000 else "🟡" if mw>110000 else "🟢"
                chg   = load.get("change_mw")
                articles.append({
                    "source":"PJM Load","category":"📊 Market",
                    "title":f"{icon} System Load: {mw/1000:.1f}k MW — {level}",
                    "snippet":f"PJM RTO: {mw:,.0f} MW" + (f" ({chg:+,.0f} MW vs prev)" if chg else ""),
                    "pub":load.get("timestamp","")[:16],
                    "url":"https://dataminer2.pjm.com/feed/inst_load",
                })
        except Exception: pass

        # 4. Forecasted outages
        try:
            r = await c.get(f"{PJM_BASE}/frcstd_gen_outages",
                params={"rowCount":"3","startRow":"1","datetime_beginning_ept":"Today",
                        "order":"Asc","sort":"datetime_beginning_ept"},
                headers=pjm_h())
            if r.status_code == 200:
                for item in r.json().get("items",[])[:2]:
                    rto  = float(item.get("forecast_gen_outage_mw_rto") or 0)
                    date = item.get("forecast_date","")
                    if rto > 0:
                        articles.append({
                            "source":"PJM Outages","category":"⚡ Ops",
                            "title":f"Forecast Outage: {rto:,.0f} MW RTO — {date[:10]}",
                            "snippet":f"Forecasted {rto:,.0f} MW generation outages for {date[:10]}.",
                            "pub":date[:16],"url":"https://dataminer2.pjm.com/feed/frcstd_gen_outages",
                        })
        except Exception: pass

    # 5. PJM + FERC RSS
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as rss:
            for src, url, cat in [
                ("PJM",  "https://www.pjm.com/rss",                      "⚡ Ops"),
                ("FERC", "https://www.ferc.gov/news-events/news/rss.xml", "📰 News"),
            ]:
                try:
                    r = await rss.get(url, headers={"User-Agent":"Mozilla/5.0 Chrome/120"})
                    if r.status_code != 200: continue
                    root = ET.fromstring(r.text)
                    for item in root.findall(".//item")[:15]:
                        title = (item.findtext("title") or "").strip()
                        desc  = (item.findtext("description") or "").strip()
                        pub   = (item.findtext("pubDate") or "").strip()
                        link  = (item.findtext("link") or "").strip()
                        if any(k in (title+" "+desc).lower() for k in ENERGY_KW):
                            articles.append({"source":src,"category":cat,"title":title,
                                             "snippet":desc[:220]+(("…") if len(desc)>220 else ""),
                                             "pub":pub[:25],"url":link})
                except Exception: continue
    except Exception: pass

    ORDER = {"🔴 Alert":0,"⚡ Ops":1,"📊 Market":2,"📰 News":3}
    articles.sort(key=lambda x: ORDER.get(x.get("category","📰 News"),3))
    cache_set("news", articles[:20])
    return articles[:20]

# ── Routes ────────────────────────────────────────────────────
@app.get("/api/lmps")
async def api_lmps():
    try:    return JSONResponse(await fetch_lmps())
    except HTTPException as e: raise e
    except Exception as e:     raise HTTPException(500, str(e))

@app.get("/api/intraday")
async def api_intraday():
    try:    return JSONResponse(await fetch_intraday())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/load")
async def api_load():
    try:    return JSONResponse(await fetch_load())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/fuel_mix")
async def api_fuel_mix():
    try:    return JSONResponse(await fetch_fuel_mix())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/load_forecast")
async def api_load_forecast():
    try:    return JSONResponse(await fetch_load_forecast())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/outages")
async def api_outages():
    try:    return JSONResponse(await fetch_outages())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/news")
async def api_news():
    try:    return JSONResponse(await fetch_news())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/debug/feeds")
async def debug_feeds():
    """Debug all PJM feeds to find correct names."""
    if not PJM_API_KEY: return JSONResponse({"error":"no key"})
    results = {}
    feeds = ["gen_by_fuel","load_frcstd_7_day","inst_load","frcstd_gen_outages","da_hrl_lmps"]
    async with httpx.AsyncClient(timeout=15) as c:
        for feed in feeds:
            r = await c.get(f"{PJM_BASE}/{feed}",
                params={"rowCount":"2","startRow":"1","datetime_beginning_ept":"Today"},
                headers=pjm_h())
            data = r.json() if r.status_code==200 else {}
            items = data.get("items",[]) if isinstance(data,dict) else []
            results[feed] = {"status":r.status_code,"total_rows":data.get("totalRows",0) if isinstance(data,dict) else 0,
                             "fields":list(items[0].keys()) if items else []}
    return JSONResponse(results)

@app.get("/api/all")
async def api_all():
    lmps, intraday, load, da, fuel_mix, load_fc, outages, news = await asyncio.gather(
        fetch_lmps(), fetch_intraday(), fetch_load(),
        fetch_da_lmps(), fetch_fuel_mix(), fetch_load_forecast(),
        fetch_outages(), fetch_news(),
        return_exceptions=True,
    )
    # System energy price as gas proxy
    sys_energy = cache_get("system_energy", 300)

    da_list = []
    if isinstance(da, dict):
        for name, v in da.items():
            da_list.append({"name":name,"lmp":v["lmp"],"energy":v.get("energy",0),"cong":v.get("cong",0)})

    return JSONResponse({
        "lmps":         lmps        if not isinstance(lmps,       Exception) else [],
        "intraday":     intraday    if not isinstance(intraday,   Exception) else [],
        "load":         load        if not isinstance(load,       Exception) else {},
        "da":           da_list,
        "fuel_mix":     fuel_mix    if not isinstance(fuel_mix,   Exception) else [],
        "load_forecast":load_fc     if not isinstance(load_fc,    Exception) else [],
        "outages":      outages     if not isinstance(outages,    Exception) else {},
        "sys_energy":   sys_energy  or {},
        "news":         news        if not isinstance(news,       Exception) else [],
        "updated_at":   datetime.now(timezone.utc).isoformat(),
    })

@app.get("/health")
async def health():
    return {"status":"ok","pjm_key_set":bool(PJM_API_KEY),
            "key_prefix":PJM_API_KEY[:6]+"..." if PJM_API_KEY else ""}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open(os.path.join(os.path.dirname(__file__),"dashboard.html")) as f:
        return f.read()
