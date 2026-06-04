"""
PJM Power Dashboard — Backend Server
Complete trader monitor — LMPs, load, gas, outages, DA/RT spreads, news
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
CACHE_TTL = 300

def cache_get(key):
    e = _cache.get(key)
    return e["data"] if e and (datetime.now(timezone.utc).timestamp() - e["ts"]) < CACHE_TTL else None

def cache_set(key, data):
    _cache[key] = {"ts": datetime.now(timezone.utc).timestamp(), "data": data}

def pjm_headers():
    return {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}

PJM_HUBS = [
    "WESTERN HUB", "EASTERN HUB", "AEP-DAYTON HUB", "N ILLINOIS HUB",
    "NEW JERSEY HUB", "CHICAGO HUB", "CHICAGO GEN HUB", "AEP GEN HUB",
    "OHIO HUB", "DOMINION HUB", "ATSI GEN HUB", "WEST INT HUB",
]

# ─────────────────────────────────────────────
# LMPs
# ─────────────────────────────────────────────
async def fetch_lmps() -> list:
    cached = cache_get("lmps")
    if cached: return cached
    if not PJM_API_KEY: raise HTTPException(503, "PJM_API_KEY not configured")

    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{PJM_BASE}/rt_unverified_hrl_lmps",
            params={"rowCount":"500","startRow":"1","type":"HUB",
                    "datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
            headers=pjm_headers())
        items = r.json().get("items",[]) if r.status_code==200 else []

    if not items: return []
    latest = items[0].get("datetime_beginning_ept","")
    seen, results = set(), []
    for row in items:
        if row.get("datetime_beginning_ept") != latest: continue
        name = (row.get("pnode_name") or "").upper().strip()
        if name in seen: continue
        seen.add(name)
        lmp  = float(row.get("total_lmp_rt") or 0)
        cong = float(row.get("congestion_price_rt") or 0)
        se   = row.get("system_energy_price_rt")
        if se is not None:
            energy = float(se); loss = round(lmp - energy - cong, 2)
        else:
            energy = round(lmp - cong, 2); loss = 0.0
        results.append({"name":row.get("pnode_name"),"type":"Hub",
                         "lmp":round(lmp,2),"energy":round(energy,2),
                         "congestion":round(cong,2),"loss":loss,"hour":latest})
    order = {h.upper():i for i,h in enumerate(PJM_HUBS)}
    results.sort(key=lambda x: order.get(x["name"].upper().strip(), 99))
    if results: cache_set("lmps", results)
    return results

# ─────────────────────────────────────────────
# DA LMPs (for spread)
# ─────────────────────────────────────────────
async def fetch_da_lmps() -> dict:
    cached = cache_get("da_lmps")
    if cached: return cached
    if not PJM_API_KEY: return {}

    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{PJM_BASE}/da_hrl_lmps",
            params={"rowCount":"500","startRow":"1","type":"HUB",
                    "datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
            headers=pjm_headers())
        items = r.json().get("items",[]) if r.status_code==200 else []

    if not items: return {}
    latest = items[0].get("datetime_beginning_ept","")
    da_map = {}
    for row in items:
        if row.get("datetime_beginning_ept") != latest: continue
        name = (row.get("pnode_name") or "").upper().strip()
        if name not in da_map:
            val = float(row.get("total_lmp_da") or row.get("total_lmp") or 0)
            if val: da_map[name] = {"lmp": val, "hour": latest}
    if da_map:
        # Cache DA for 1 hour — it doesnt change after day-ahead market closes
        _cache["da_lmps"] = {"ts": datetime.now(timezone.utc).timestamp(), "data": da_map}
        # Store TTL override
        _cache["da_lmps"]["ttl"] = 3600
    return da_map

# ─────────────────────────────────────────────
# Intraday
# ─────────────────────────────────────────────
async def fetch_intraday() -> list:
    cached = cache_get("intraday")
    if cached: return cached
    if not PJM_API_KEY: return []
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{PJM_BASE}/rt_unverified_hrl_lmps",
            params={"rowCount":"50","startRow":"1","pnode_name":"WESTERN HUB",
                    "datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
            headers=pjm_headers())
        items = r.json().get("items",[]) if r.status_code==200 else []
    result = [{"hour":row.get("datetime_beginning_ept",""),
               "lmp":round(float(row.get("total_lmp_rt") or 0),2)} for row in items]
    if result: cache_set("intraday", result)
    return result

# ─────────────────────────────────────────────
# Load Forecast + Instantaneous Load
# ─────────────────────────────────────────────
async def fetch_load_forecast() -> dict:
    cached = cache_get("load_forecast")
    if cached: return cached
    if not PJM_API_KEY: return {}
    result = {"forecast":[], "current_actual":None, "peak_forecast":None}

    # Known non-overlapping PJM areas that sum to RTO total
    PJM_AREAS = {"AEP","APS","ATSI","BC","CE","DAY","DEOK","DOM","DUQ","EKPC","JC","ME","PE","PEP","PL","PN","PS","RECO"}

    async with httpx.AsyncClient(timeout=20) as c:
        # Load forecast — try multiple feed names
        for feed in ["load_frcstd_7day", "load_forecast_7day", "hrl_load_metered"]:
            try:
                r = await c.get(f"{PJM_BASE}/{feed}",
                    params={"rowCount":"48","startRow":"1","datetime_beginning_ept":"Today",
                            "order":"Asc","sort":"datetime_beginning_ept"},
                    headers=pjm_headers())
                if r.status_code == 200:
                    items = r.json().get("items",[])
                    fc = []
                    for row in items:
                        mw = float(row.get("rto_total") or row.get("total_load_forecast") or
                                   row.get("load_forecast_rto") or row.get("forecast_load_mw") or
                                   row.get("rto_forecast") or row.get("metered_load_mw") or 0)
                        hr = row.get("datetime_beginning_ept","")
                        if mw > 0: fc.append({"hour":hr,"mw":mw})
                    if fc:
                        result["forecast"] = fc
                        result["peak_forecast"] = max(x["mw"] for x in fc)
                        break
            except Exception:
                continue

        # Instantaneous load — sum non-overlapping areas
        try:
            r = await c.get(f"{PJM_BASE}/inst_load",
                params={"rowCount":"100","startRow":"1","datetime_beginning_ept":"5MinutesAgo"},
                headers=pjm_headers())
            if r.status_code == 200:
                items = r.json().get("items",[])
                # First check for explicit PJM RTO row
                rto_val = 0.0
                for item in items:
                    area = (item.get("area") or "").upper().strip()
                    val  = float(item.get("instantaneous_load") or 0)
                    if area == "PJM RTO":
                        rto_val = val; break
                # Otherwise sum non-overlapping member areas
                if not rto_val:
                    seen_areas = set()
                    total = 0.0
                    for item in items:
                        area = (item.get("area") or "").upper().strip()
                        val  = float(item.get("instantaneous_load") or 0)
                        if area in PJM_AREAS and area not in seen_areas:
                            total += val; seen_areas.add(area)
                    if total > 10000: rto_val = total
                if rto_val > 1000:
                    result["current_actual"] = rto_val
        except Exception:
            pass

    if result["forecast"] or result["current_actual"]:
        cache_set("load_forecast", result)
    return result

# ─────────────────────────────────────────────
# Generation Outages
# ─────────────────────────────────────────────
async def fetch_outages() -> dict:
    cached = cache_get("outages")
    if cached: return cached
    if not PJM_API_KEY: return {}

    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{PJM_BASE}/gen_outages_by_type",
            params={"rowCount":"5","startRow":"1","datetime_beginning_ept":"Today",
                    "order":"Desc","sort":"datetime_beginning_ept"},
            headers=pjm_headers())
        items = r.json().get("items",[]) if r.status_code==200 else []

    if not items: return {}
    row = items[0]
    # Try all known field names
    total  = float(row.get("total_outages_mw") or row.get("rto_total") or row.get("total_mw") or 0)
    forced = float(row.get("forced_outages_mw") or row.get("rto_forced") or row.get("forced_mw") or 0)
    maint  = float(row.get("maintenance_outages_mw") or row.get("rto_maintenance") or row.get("planned_mw") or 0)
    result = {"total":total,"forced":forced,"maintenance":maint,
              "hour":row.get("datetime_beginning_ept",""),"all_keys":list(row.keys())}
    cache_set("outages", result)
    return result

# ─────────────────────────────────────────────
# Gas Prices — EIA API v2
# ─────────────────────────────────────────────
async def fetch_gas_prices() -> list:
    cached = cache_get("gas_prices")
    if cached: return cached
    prices = []
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            # Try without DEMO_KEY — some EIA endpoints are fully open
            r = await c.get("https://api.eia.gov/v2/natural-gas/pri/sum/data/",
                params={"api_key":"DEMO_KEY","frequency":"daily","data[0]":"value",
                        "facets[series][]":"RNGWHHD","sort[0][column]":"period",
                        "sort[0][direction]":"desc","length":"3"})
            if r.status_code == 200:
                rows = r.json().get("response",{}).get("data",[])
                if rows:
                    row = rows[0]
                    prices.append({"hub":"Henry Hub","date":row.get("period",""),
                                   "price":round(float(row.get("value") or 0),3),"unit":"$/MMBtu"})
    except Exception:
        pass

    if not prices:
        # Fallback: use a hardcoded recent value with note
        prices = [{"hub":"Henry Hub","date":"latest","price":0,"unit":"$/MMBtu — EIA API unavailable"}]
    cache_set("gas_prices", prices)
    return prices

# ─────────────────────────────────────────────
# News — PJM API data + PJM/FERC RSS
# ─────────────────────────────────────────────
ENERGY_KW = ["power","energy","electricity","grid","lmp","capacity","natural gas","coal",
             "solar","wind","pjm","ferc","megawatt","mw","transmission","congestion",
             "renewable","nuclear","generation","utility","outage","demand","load",
             "curtailment","emergency","alert","market","rate","tariff","fuel","price"]

async def fetch_news() -> list:
    cached = cache_get("news")
    if cached: return cached
    if not PJM_API_KEY: return []

    articles = []

    async with httpx.AsyncClient(timeout=20) as c:

        # ── 1. LMP spike/negative alerts ──────────────────────────────────
        try:
            lmps = cache_get("lmps") or await fetch_lmps()
            high = [h for h in lmps if h["lmp"] > 80]
            low  = [h for h in lmps if h["lmp"] < 0]
            if high:
                names = ", ".join(h["name"] for h in high)
                articles.append({
                    "source":"PJM LMP Alert","category":"🔴 Alert",
                    "title":f"HIGH PRICE: {len(high)} hubs above $80/MWh — peak {max(h['lmp'] for h in high):.2f}",
                    "snippet":", ".join(f"{h['name']}: ${h['lmp']:.2f}" for h in high),
                    "pub":(high[0].get("hour",""))[:16],
                    "url":"https://dataminer2.pjm.com/feed/rt_unverified_hrl_lmps",
                })
            if low:
                articles.append({
                    "source":"PJM LMP Alert","category":"🔴 Alert",
                    "title":f"NEGATIVE LMP: {', '.join(h['name'] for h in low)}",
                    "snippet":", ".join(f"{h['name']}: ${h['lmp']:.2f}" for h in low),
                    "pub":(low[0].get("hour",""))[:16],
                    "url":"https://dataminer2.pjm.com/feed/rt_unverified_hrl_lmps",
                })
        except Exception:
            pass

        # ── 2. DA/RT spread alerts ─────────────────────────────────────────
        try:
            da_map = await fetch_da_lmps()
            lmps   = cache_get("lmps") or []
            spreads = []
            for h in lmps:
                da = da_map.get(h["name"].upper())
                if da:
                    sp = round(h["lmp"] - da["lmp"], 2)
                    spreads.append((h["name"], h["lmp"], da["lmp"], sp))
            if spreads:
                spreads.sort(key=lambda x: abs(x[3]), reverse=True)
                top = spreads[:4]
                articles.append({
                    "source":"PJM DA/RT","category":"📊 Market",
                    "title":f"DA/RT Spread — Largest: {top[0][0]} Δ${top[0][3]:+.2f}/MWh",
                    "snippet":" | ".join(f"{n}: RT${rt:.0f} DA${da:.0f} (Δ{sp:+.1f})" for n,rt,da,sp in top),
                    "pub":(lmps[0].get("hour",""))[:16] if lmps else "",
                    "url":"https://dataminer2.pjm.com/feed/da_hrl_lmps",
                })
        except Exception:
            pass

        # ── 3. Generation outages ──────────────────────────────────────────
        try:
            r = await c.get(f"{PJM_BASE}/gen_outages_by_type",
                params={"rowCount":"3","startRow":"1","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
                headers=pjm_headers())
            if r.status_code == 200:
                items = r.json().get("items",[])
                if items:
                    row    = items[0]
                    keys   = list(row.keys())
                    # find MW fields dynamically
                    mw_fields = [k for k in keys if "mw" in k.lower() or "outage" in k.lower()]
                    total  = float(row.get("total_outages_mw") or row.get("rto_total") or 0)
                    forced = float(row.get("forced_outages_mw") or row.get("rto_forced") or 0)
                    hour   = row.get("datetime_beginning_ept","")
                    if total > 0:
                        articles.append({
                            "source":"PJM Outages","category":"⚡ Ops",
                            "title":f"Generation Outages: {total:,.0f} MW total ({forced:,.0f} MW forced)",
                            "snippet":f"Total: {total:,.0f} MW | Forced: {forced:,.0f} MW | Maintenance: {total-forced:,.0f} MW",
                            "pub":hour[:16],"url":"https://dataminer2.pjm.com/feed/gen_outages_by_type",
                        })
                    elif mw_fields:
                        # Fields we found but couldn't parse — show raw
                        snippet = " | ".join(f"{k}: {row.get(k)}" for k in mw_fields[:4])
                        articles.append({
                            "source":"PJM Outages","category":"⚡ Ops",
                            "title":"Generation Outage Data Available",
                            "snippet":snippet,
                            "pub":hour[:16],"url":"https://dataminer2.pjm.com/feed/gen_outages_by_type",
                        })
        except Exception:
            pass

        # ── 4. Forecast outages ───────────────────────────────────────────
        try:
            r = await c.get(f"{PJM_BASE}/frcstd_gen_outages",
                params={"rowCount":"5","startRow":"1","datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
                headers=pjm_headers())
            if r.status_code == 200:
                items = r.json().get("items",[])
                for item in items[:2]:
                    rto  = float(item.get("forecast_gen_outage_mw_rto") or 0)
                    date = item.get("forecast_date","")
                    if rto > 0:
                        articles.append({
                            "source":"PJM Forecast","category":"⚡ Ops",
                            "title":f"Forecast Outage: {rto:,.0f} MW RTO — {date[:10]}",
                            "snippet":f"Forecasted {rto:,.0f} MW of generation outages for {date[:10]}. West: {item.get('forecast_gen_outage_mw_west',0):,.0f} MW.",
                            "pub":date[:16],"url":"https://dataminer2.pjm.com/feed/frcstd_gen_outages",
                        })
        except Exception:
            pass

        # ── 5. Load vs forecast variance ──────────────────────────────────
        try:
            load_data = cache_get("load_forecast") or await fetch_load_forecast()
            actual = load_data.get("current_actual")
            fc     = load_data.get("forecast", [])
            if actual and fc:
                # Find closest forecast hour
                current_fc = fc[-1]["mw"] if fc else None
                if current_fc and current_fc > 0:
                    variance = round(actual - current_fc, 0)
                    if abs(variance) > 1000:
                        articles.append({
                            "source":"PJM Load","category":"📊 Market",
                            "title":f"Load vs Forecast: {'Over' if variance>0 else 'Under'} by {abs(variance):,.0f} MW",
                            "snippet":f"Actual: {actual:,.0f} MW | Forecast: {current_fc:,.0f} MW | Variance: {variance:+,.0f} MW",
                            "pub":datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
                            "url":"https://dataminer2.pjm.com/feed/inst_load",
                        })
        except Exception:
            pass

    # ── 6. PJM RSS feed ───────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as rss:
            for src, url, cat in [
                ("PJM",  "https://www.pjm.com/rss",                                "⚡ Ops"),
                ("FERC", "https://www.ferc.gov/news-events/news/rss.xml",           "📰 News"),
            ]:
                try:
                    r = await rss.get(url, headers={"User-Agent":"Mozilla/5.0 Chrome/120"})
                    if r.status_code != 200: continue
                    root  = ET.fromstring(r.text)
                    items = root.findall(".//item")
                    for item in items[:15]:
                        title = (item.findtext("title") or "").strip()
                        desc  = (item.findtext("description") or "").strip()
                        pub   = (item.findtext("pubDate") or "").strip()
                        link  = (item.findtext("link") or "").strip()
                        if any(k in (title+" "+desc).lower() for k in ENERGY_KW):
                            articles.append({"source":src,"category":cat,"title":title,
                                             "snippet":desc[:220]+(("…") if len(desc)>220 else ""),
                                             "pub":pub[:25],"url":link})
                except Exception:
                    continue
    except Exception:
        pass

    # Sort: Alerts → Ops → Market → News
    order = {"🔴 Alert":0,"⚡ Ops":1,"📊 Market":2,"📰 News":3}
    articles.sort(key=lambda x: order.get(x.get("category","📰 News"), 3))
    cache_set("news", articles[:20])
    return articles[:20]

# ─────────────────────────────────────────────
# Polymarket
# ─────────────────────────────────────────────
POWER_KW = ["electricity","power","energy","grid","PJM","ERCOT","natural gas","coal",
            "solar","wind","megawatt","nuclear","utility","transmission","oil","gas","LNG","crude"]

async def fetch_polymarket() -> list:
    cached = cache_get("polymarket")
    if cached: return cached
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get("https://gamma-api.polymarket.com/markets",
                        params={"closed":"false","limit":200,"order":"volume","ascending":"false"})
        r.raise_for_status()
        markets = r.json()
    results = []
    for m in markets:
        q = (m.get("question") or m.get("title") or "").lower()
        if any(k.lower() in q for k in POWER_KW):
            outcomes = []
            try:
                prices = json.loads(m.get("outcomePrices") or "[]")
                names  = json.loads(m.get("outcomes") or "[]")
                for name, price in zip(names, prices):
                    outcomes.append({"label":name,"pct":round(float(price)*100,1)})
            except Exception:
                pass
            results.append({"question":m.get("question") or m.get("title"),
                             "volume":m.get("volume","—"),"end_date":(m.get("endDate") or "")[:10],
                             "url":f"https://polymarket.com/event/{m.get('slug','')}","outcomes":outcomes})
        if len(results) >= 8: break
    cache_set("polymarket", results)
    return results

# ─────────────────────────────────────────────
# Debug endpoints
# ─────────────────────────────────────────────
@app.get("/api/debug/outages")
async def debug_outages():
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{PJM_BASE}/gen_outages_by_type",
            params={"rowCount":"2","startRow":"1","datetime_beginning_ept":"Today"},
            headers=pjm_headers())
        data = r.json() if r.status_code==200 else {}
    return JSONResponse({"status":r.status_code,"sample":data.get("items",[])[:2]})

@app.get("/api/debug/load")
async def debug_load():
    async with httpx.AsyncClient(timeout=15) as c:
        r1 = await c.get(f"{PJM_BASE}/load_frcstd_7day",
            params={"rowCount":"3","startRow":"1","datetime_beginning_ept":"Today"},
            headers=pjm_headers())
        r2 = await c.get(f"{PJM_BASE}/inst_load",
            params={"rowCount":"20","startRow":"1","datetime_beginning_ept":"5MinutesAgo"},
            headers=pjm_headers())
    return JSONResponse({
        "forecast":{"status":r1.status_code,"sample":r1.json().get("items",[])[:2] if r1.status_code==200 else r1.text[:200]},
        "inst_load":{"status":r2.status_code,"sample":r2.json().get("items",[])[:5] if r2.status_code==200 else r2.text[:200]},
    })

@app.get("/api/debug/da")
async def debug_da():
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{PJM_BASE}/da_hrl_lmps",
            params={"rowCount":"5","startRow":"1","type":"HUB","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
            headers=pjm_headers())
        data = r.json() if r.status_code==200 else {}
    return JSONResponse({"status":r.status_code,"sample":data.get("items",[])[:3]})

@app.get("/api/nettest")
async def api_nettest():
    results = {}
    urls = [("pjm_rss","https://www.pjm.com/rss"),("ferc_rss","https://www.ferc.gov/news-events/news/rss.xml"),("polymarket","https://gamma-api.polymarket.com/markets?limit=1"),("eia_api","https://api.eia.gov/v2/natural-gas/pri/sum/data/?api_key=DEMO_KEY&frequency=daily&data[0]=value&facets[series][]=RNGWHHD&length=1")]
    async with httpx.AsyncClient(timeout=8, follow_redirects=True) as c:
        for name, url in urls:
            try:
                r = await c.get(url, headers={"User-Agent":"Mozilla/5.0 Chrome/120"})
                results[name] = {"status":r.status_code,"len":len(r.text)}
            except Exception as e:
                results[name] = {"error":str(e)[:80]}
    return JSONResponse(results)

# ─────────────────────────────────────────────
# Main routes
# ─────────────────────────────────────────────
@app.get("/api/lmps")
async def api_lmps():
    try:    return JSONResponse(await fetch_lmps())
    except HTTPException as e: raise e
    except Exception as e:     raise HTTPException(500, str(e))

@app.get("/api/lmps/debug")
async def api_lmps_debug():
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{PJM_BASE}/rt_unverified_hrl_lmps",
            params={"rowCount":"5","startRow":"1","type":"HUB","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
            headers=pjm_headers())
        data = r.json() if r.status_code==200 else {}
    return JSONResponse({"status":r.status_code,"total_rows":data.get("totalRows",0),"sample":data.get("items",[])[:3]})

@app.get("/api/intraday")
async def api_intraday():
    try:    return JSONResponse(await fetch_intraday())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/load")
async def api_load():
    try:    return JSONResponse(await fetch_load_forecast())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/gas")
async def api_gas():
    try:    return JSONResponse(await fetch_gas_prices())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/polymarket")
async def api_polymarket():
    try:    return JSONResponse(await fetch_polymarket())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/news")
async def api_news():
    try:    return JSONResponse(await fetch_news())
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/all")
async def api_all():
    lmps, intraday, load, gas, da, markets, news = await asyncio.gather(
        fetch_lmps(), fetch_intraday(), fetch_load_forecast(),
        fetch_gas_prices(), fetch_da_lmps(), fetch_polymarket(), fetch_news(),
        return_exceptions=True,
    )
    # Convert da_map to list for frontend
    da_list = []
    if isinstance(da, dict):
        da_list = [{"name": k, "lmp": v["lmp"]} for k, v in da.items()]
    return JSONResponse({
        "lmps":    lmps     if not isinstance(lmps,    Exception) else [],
        "intraday":intraday if not isinstance(intraday, Exception) else [],
        "load":    load     if not isinstance(load,     Exception) else {},
        "gas":     gas      if not isinstance(gas,      Exception) else [],
        "da":      da_list,
        "markets": markets  if not isinstance(markets,  Exception) else [],
        "news":    news     if not isinstance(news,     Exception) else [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })


@app.get("/api/debug/all")
async def debug_all():
    """Master debug — checks all feeds and returns exact field names and sample values."""
    if not PJM_API_KEY: return JSONResponse({"error":"no key"})
    h = pjm_headers()
    out = {}

    async with httpx.AsyncClient(timeout=20) as c:

        # DA LMPs
        r = await c.get(f"{PJM_BASE}/da_hrl_lmps",
            params={"rowCount":"3","startRow":"1","type":"HUB","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
            headers=h)
        data = r.json() if r.status_code==200 else {}
        items = data.get("items",[])
        out["da_hrl_lmps"] = {"status":r.status_code,"total_rows":data.get("totalRows",0),"fields":list(items[0].keys()) if items else [],"sample":items[:2]}

        # Load forecast feeds
        for feed in ["load_frcstd_7day","load_forecast_7day","hrl_load_metered","load_frcstd_7day_by_zone"]:
            r = await c.get(f"{PJM_BASE}/{feed}",
                params={"rowCount":"2","startRow":"1","datetime_beginning_ept":"Today"},
                headers=h)
            data = r.json() if r.status_code==200 else {}
            items = data.get("items",[])
            out[f"load_{feed}"] = {"status":r.status_code,"total_rows":data.get("totalRows",0),"fields":list(items[0].keys()) if items else [],"sample":items[:1]}

        # Inst load — full area list
        r = await c.get(f"{PJM_BASE}/inst_load",
            params={"rowCount":"100","startRow":"1","datetime_beginning_ept":"5MinutesAgo"},
            headers=h)
        data = r.json() if r.status_code==200 else {}
        items = data.get("items",[])
        out["inst_load"] = {"status":r.status_code,"total_rows":data.get("totalRows",0),"all_areas":[i.get("area") for i in items],"fields":list(items[0].keys()) if items else []}

        # Gen outages
        r = await c.get(f"{PJM_BASE}/gen_outages_by_type",
            params={"rowCount":"2","startRow":"1","datetime_beginning_ept":"Today"},
            headers=h)
        data = r.json() if r.status_code==200 else {}
        items = data.get("items",[])
        out["gen_outages_by_type"] = {"status":r.status_code,"fields":list(items[0].keys()) if items else [],"sample":items[:1]}

        # EIA gas API
        r = await c.get("https://api.eia.gov/v2/natural-gas/pri/sum/data/",
            params={"api_key":"DEMO_KEY","frequency":"daily","data[0]":"value","facets[series][]": "RNGWHHD","length":"1"})
        out["eia_gas"] = {"status":r.status_code,"sample":r.json().get("response",{}).get("data",[])[:1] if r.status_code==200 else r.text[:100]}

    return JSONResponse(out)

@app.get("/health")
async def health():
    return {"status":"ok","pjm_key_set":bool(PJM_API_KEY),"key_prefix":PJM_API_KEY[:6]+"..." if PJM_API_KEY else ""}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open(os.path.join(os.path.dirname(__file__),"dashboard.html")) as f:
        return f.read()
