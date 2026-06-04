"""
PJM Power Dashboard — Backend Server (Final)
All field names verified from live API debug.
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

# ─────────────────────────────────────────────────────────────
# RT LMPs
# ─────────────────────────────────────────────────────────────
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
    for row in items:
        if row.get("datetime_beginning_ept") != latest: continue
        name = (row.get("pnode_name") or "").upper().strip()
        if name in seen: continue
        seen.add(name)
        lmp  = float(row.get("total_lmp_rt") or 0)
        cong = float(row.get("congestion_price_rt") or 0)
        se   = row.get("system_energy_price_rt")
        energy = float(se) if se is not None else round(lmp - cong, 2)
        loss   = round(lmp - energy - cong, 2) if se is not None else 0.0
        results.append({"name":row.get("pnode_name"),"type":"Hub",
                         "lmp":round(lmp,2),"energy":round(energy,2),
                         "congestion":round(cong,2),"loss":loss,"hour":latest})
    order = {h.upper():i for i,h in enumerate(PJM_HUBS)}
    results.sort(key=lambda x: order.get(x["name"].upper().strip(), 99))
    if results: cache_set("lmps", results)
    return results

# ─────────────────────────────────────────────────────────────
# DA LMPs — verified fields: total_lmp_da, system_energy_price_da, congestion_price_da
# ─────────────────────────────────────────────────────────────
async def fetch_da_lmps() -> dict:
    cached = cache_get("da_lmps", 3600)  # cache 1 hour
    if cached: return cached
    if not PJM_API_KEY: return {}

    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{PJM_BASE}/da_hrl_lmps",
            params={"rowCount":"500","startRow":"1","type":"HUB",
                    "datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
            headers=pjm_h())
        items = r.json().get("items",[]) if r.status_code==200 else []

    if not items: return {}
    # Find the most common/latest hour — DA is published for all 24hrs so take latest available
    # Actually for RT vs DA comparison, we want matching hours
    # Group by pnode, keep latest hour per pnode
    da_map = {}
    for row in items:
        name = (row.get("pnode_name") or "").upper().strip()
        val  = float(row.get("total_lmp_da") or 0)
        if val and name not in da_map:
            da_map[name] = {
                "lmp":    round(val, 2),
                "energy": round(float(row.get("system_energy_price_da") or 0), 2),
                "cong":   round(float(row.get("congestion_price_da") or 0), 2),
                "hour":   row.get("datetime_beginning_ept",""),
            }
    # Build a per-hour map for better RT/DA matching
    by_hour = {}
    for row in items:
        hour = row.get("datetime_beginning_ept","")
        name = (row.get("pnode_name") or "").upper().strip()
        val  = float(row.get("total_lmp_da") or 0)
        if hour not in by_hour: by_hour[hour] = {}
        if val: by_hour[hour][name] = {"lmp":round(val,2),"energy":round(float(row.get("system_energy_price_da") or 0),2),"cong":round(float(row.get("congestion_price_da") or 0),2)}

    result = {"by_name": da_map, "by_hour": by_hour, "hours": sorted(by_hour.keys())}
    cache_set("da_lmps", result)
    return result

# ─────────────────────────────────────────────────────────────
# Intraday
# ─────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────
# Inst Load — "PJM RTO" area confirmed in data
# ─────────────────────────────────────────────────────────────
async def fetch_load() -> dict:
    cached = cache_get("load", 120)  # 2 min cache
    if cached: return cached
    if not PJM_API_KEY: return {}

    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{PJM_BASE}/inst_load",
            params={"rowCount":"100","startRow":"1","datetime_beginning_ept":"5MinutesAgo"},
            headers=pjm_h())
        items = r.json().get("items",[]) if r.status_code==200 else []

    if not items: return {}

    # Get two timestamps — find latest
    timestamps = sorted(set(i.get("datetime_beginning_ept","") for i in items), reverse=True)
    latest_ts = timestamps[0] if timestamps else ""
    prev_ts   = timestamps[1] if len(timestamps) > 1 else ""

    def sum_rto(rows, ts):
        for row in rows:
            if row.get("datetime_beginning_ept") == ts and (row.get("area") or "").upper().strip() == "PJM RTO":
                return float(row.get("instantaneous_load") or 0)
        return 0.0

    current = sum_rto(items, latest_ts)
    prev    = sum_rto(items, prev_ts)
    change  = round(current - prev, 0) if prev else None

    result = {
        "current_mw":  round(current, 0),
        "prev_mw":     round(prev, 0),
        "change_mw":   change,
        "timestamp":   latest_ts,
    }
    if current > 1000: cache_set("load", result)
    return result

# ─────────────────────────────────────────────────────────────
# Gas Prices — EIA verified: frequency must be "monthly" or "annual"
# ─────────────────────────────────────────────────────────────
async def fetch_gas() -> list:
    cached = cache_get("gas", 3600)  # 1hr cache
    if cached: return cached
    prices = []
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://api.eia.gov/v2/natural-gas/pri/sum/data/",
                params={"api_key":"DEMO_KEY","frequency":"monthly","data[0]":"value",
                        "facets[series][]":"RNGWHHD","sort[0][column]":"period",
                        "sort[0][direction]":"desc","length":"2"})
            if r.status_code == 200:
                rows = r.json().get("response",{}).get("data",[])
                for row in rows[:1]:
                    prices.append({"hub":"Henry Hub","date":row.get("period",""),
                                   "price":round(float(row.get("value") or 0),3),
                                   "unit":"$/MMBtu (monthly avg)"})
    except Exception:
        pass
    if not prices:
        prices = [{"hub":"Henry Hub","date":"—","price":0,"unit":"$/MMBtu"}]
    cache_set("gas", prices)
    return prices

# ─────────────────────────────────────────────────────────────
# News — PJM API data + PJM/FERC RSS
# ─────────────────────────────────────────────────────────────
ENERGY_KW = ["power","energy","electricity","grid","lmp","capacity","natural gas","coal",
             "solar","wind","pjm","ferc","megawatt","mw","transmission","congestion",
             "renewable","nuclear","generation","utility","outage","demand","load",
             "curtailment","emergency","alert","market","rate","tariff","fuel","price"]

async def fetch_news() -> list:
    cached = cache_get("news", 300)
    if cached: return cached
    if not PJM_API_KEY: return []
    articles = []

    async with httpx.AsyncClient(timeout=20) as c:

        # 1. LMP spike alerts
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
            da  = cache_get("da_lmps", 3600) or await fetch_da_lmps()
            lmps = cache_get("lmps", 300) or []
            if da and lmps:
                # Match RT hour to closest DA hour
                rt_hour = lmps[0].get("hour","") if lmps else ""
                da_hour_data = da.get("by_hour",{}).get(rt_hour) or da.get("by_name",{})
                spreads = []
                for h in lmps:
                    name = h["name"].upper()
                    da_entry = da_hour_data.get(name) if isinstance(da_hour_data, dict) and name in da_hour_data else da.get("by_name",{}).get(name)
                    if da_entry:
                        sp = round(h["lmp"] - da_entry["lmp"], 2)
                        spreads.append((h["name"], h["lmp"], da_entry["lmp"], sp))
                if spreads:
                    spreads.sort(key=lambda x: abs(x[3]), reverse=True)
                    top = spreads[:4]
                    articles.append({
                        "source":"PJM DA/RT","category":"📊 Market",
                        "title":f"DA/RT Spread — {top[0][0]}: RT${top[0][1]:.0f} vs DA${top[0][2]:.0f} (Δ{top[0][3]:+.1f})",
                        "snippet":" | ".join(f"{n}: Δ{sp:+.1f}" for n,rt,da_p,sp in top),
                        "pub":rt_hour[:16],
                        "url":"https://dataminer2.pjm.com/feed/da_hrl_lmps",
                    })
        except Exception: pass

        # 3. Load vs normal
        try:
            load = cache_get("load", 120) or await fetch_load()
            mw = load.get("current_mw",0)
            if mw > 0:
                level = "CRITICAL" if mw>120000 else "HIGH" if mw>100000 else "ELEVATED" if mw>85000 else "NORMAL"
                color = "🔴" if mw>120000 else "🟡" if mw>100000 else "🟢"
                articles.append({
                    "source":"PJM Load","category":"📊 Market",
                    "title":f"{color} System Load: {mw/1000:.1f}k MW — {level}",
                    "snippet":f"PJM RTO instantaneous load: {mw:,.0f} MW" + (f" ({load['change_mw']:+,.0f} MW vs prev interval)" if load.get('change_mw') else ""),
                    "pub":load.get("timestamp","")[:16],
                    "url":"https://dataminer2.pjm.com/feed/inst_load",
                })
        except Exception: pass

        # 4. Forecasted outages
        try:
            r = await c.get(f"{PJM_BASE}/frcstd_gen_outages",
                params={"rowCount":"3","startRow":"1","datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
                headers=pjm_h())
            if r.status_code == 200:
                items = r.json().get("items",[])
                for item in items[:2]:
                    rto  = float(item.get("forecast_gen_outage_mw_rto") or 0)
                    west = float(item.get("forecast_gen_outage_mw_west") or 0)
                    date = item.get("forecast_date","")
                    if rto > 0:
                        articles.append({
                            "source":"PJM Outages","category":"⚡ Ops",
                            "title":f"Forecast Outage: {rto:,.0f} MW RTO — {date[:10]}",
                            "snippet":f"Forecasted generation outages: {rto:,.0f} MW RTO total, {west:,.0f} MW West.",
                            "pub":date[:16],"url":"https://dataminer2.pjm.com/feed/frcstd_gen_outages",
                        })
        except Exception: pass

    # 5. PJM RSS + FERC RSS
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as rss:
            for src, url, cat in [
                ("PJM",  "https://www.pjm.com/rss",                       "⚡ Ops"),
                ("FERC", "https://www.ferc.gov/news-events/news/rss.xml",  "📰 News"),
            ]:
                try:
                    r = await rss.get(url, headers={"User-Agent":"Mozilla/5.0 Chrome/120"})
                    if r.status_code != 200: continue
                    root  = ET.fromstring(r.text)
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

# ─────────────────────────────────────────────────────────────
# Polymarket
# ─────────────────────────────────────────────────────────────
POWER_KW = ["electricity","power","energy","grid","PJM","ERCOT","natural gas","coal",
            "solar","wind","megawatt","nuclear","utility","transmission","oil","gas","LNG","crude"]

async def fetch_polymarket() -> list:
    cached = cache_get("polymarket", 300)
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
            except Exception: pass
            results.append({"question":m.get("question") or m.get("title"),
                             "volume":m.get("volume","—"),"end_date":(m.get("endDate") or "")[:10],
                             "url":f"https://polymarket.com/event/{m.get('slug','')}","outcomes":outcomes})
        if len(results) >= 8: break
    cache_set("polymarket", results)
    return results

# ─────────────────────────────────────────────────────────────
# Debug
# ─────────────────────────────────────────────────────────────
@app.get("/api/debug/all")
async def debug_all():
    if not PJM_API_KEY: return JSONResponse({"error":"no key"})
    h = pjm_h()
    out = {}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{PJM_BASE}/da_hrl_lmps",
            params={"rowCount":"3","startRow":"1","type":"HUB","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
            headers=h)
        data = r.json() if r.status_code==200 else {}
        items = data.get("items",[])
        out["da_hrl_lmps"] = {"status":r.status_code,"total_rows":data.get("totalRows",0),"fields":list(items[0].keys()) if items else [],"sample":items[:2]}
        r = await c.get(f"{PJM_BASE}/inst_load",
            params={"rowCount":"100","startRow":"1","datetime_beginning_ept":"5MinutesAgo"},
            headers=h)
        data = r.json() if r.status_code==200 else {}
        items = data.get("items",[])
        out["inst_load"] = {"status":r.status_code,"total_rows":data.get("totalRows",0),"all_areas":[i.get("area") for i in items],"fields":list(items[0].keys()) if items else []}
    return JSONResponse(out)

# ─────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────
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

@app.get("/api/gas")
async def api_gas():
    try:    return JSONResponse(await fetch_gas())
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
        fetch_lmps(), fetch_intraday(), fetch_load(),
        fetch_gas(), fetch_da_lmps(), fetch_polymarket(), fetch_news(),
        return_exceptions=True,
    )
    da_list = []
    if isinstance(da, dict):
        for name, v in da.get("by_name",{}).items():
            da_list.append({"name":name,"lmp":v["lmp"],"energy":v.get("energy",0),"cong":v.get("cong",0)})
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

@app.get("/health")
async def health():
    return {"status":"ok","pjm_key_set":bool(PJM_API_KEY),"key_prefix":PJM_API_KEY[:6]+"..." if PJM_API_KEY else ""}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open(os.path.join(os.path.dirname(__file__),"dashboard.html")) as f:
        return f.read()
