"""
PJM Power Dashboard — Backend Server (Final Clean Version)
All field names verified from live API.
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
            energy = float(se); loss = round(lmp - energy - cong, 2)
        elif ml is not None:
            loss = float(ml); energy = round(lmp - cong - loss, 2)
        else:
            energy = round(lmp - cong, 2); loss = 0.0
        results.append({"name":row.get("pnode_name"),"type":"Hub",
                         "lmp":round(lmp,2),"energy":round(energy,2),
                         "congestion":round(cong,2),"loss":loss,"hour":latest})
    order = {h.upper():i for i,h in enumerate(PJM_HUBS)}
    results.sort(key=lambda x: order.get(x["name"].upper().strip(), 99))
    if results: cache_set("lmps", results)
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
            da_map[name] = {
                "lmp":    round(val, 2),
                "energy": round(float(row.get("system_energy_price_da") or 0), 2),
                "cong":   round(float(row.get("congestion_price_da") or 0), 2),
            }
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
    result  = {"current_mw":round(current,0),"prev_mw":round(prev,0),"change_mw":change,"timestamp":latest_ts}
    if current > 1000: cache_set("load", result)
    return result

# ── Gas Prices (EIA) ──────────────────────────────────────────
async def fetch_gas() -> list:
    cached = cache_get("gas", 3600)
    if cached: return cached
    prices = []
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            # Try EIA v1 API (older but more permissive)
            r = await c.get("https://api.eia.gov/series/",
                params={"api_key":"DEMO_KEY","series_id":"NG.RNGWHHD.D","num":"1"})
            if r.status_code == 200:
                series = r.json().get("series",[])
                if series and series[0].get("data"):
                    pt = series[0]["data"][0]
                    prices.append({"hub":"Henry Hub","date":str(pt[0]),
                                   "price":round(float(pt[1]),3),"unit":"$/MMBtu"})
    except Exception: pass
    if not prices:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                # Try EIA v2 monthly
                r = await c.get("https://api.eia.gov/v2/natural-gas/pri/sum/data/",
                    params={"api_key":"DEMO_KEY","frequency":"monthly","data[0]":"value",
                            "facets[series][]":"RNGWHHD","sort[0][column]":"period",
                            "sort[0][direction]":"desc","length":"1"})
                if r.status_code == 200:
                    rows = r.json().get("response",{}).get("data",[])
                    if rows and float(rows[0].get("value") or 0) > 0:
                        prices.append({"hub":"Henry Hub","date":rows[0].get("period",""),
                                       "price":round(float(rows[0]["value"]),3),"unit":"$/MMBtu (monthly avg)"})
        except Exception: pass
    if not prices:
        prices = [{"hub":"Henry Hub","date":"—","price":0,"unit":"$/MMBtu"}]
    cache_set("gas", prices)
    return prices

# ── News ──────────────────────────────────────────────────────
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
        # LMP alerts
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

        # DA/RT spread
        try:
            da   = cache_get("da_lmps", 1800) or await fetch_da_lmps()
            lmps = cache_get("lmps", 300) or []
            if da and lmps:
                spreads = []
                for h in lmps:
                    name = h["name"].upper().strip()
                    da_e = da.get(name)
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

        # Load level
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
                    "snippet":f"PJM RTO instantaneous load: {mw:,.0f} MW" + (f" ({chg:+,.0f} MW vs prev interval)" if chg else ""),
                    "pub":load.get("timestamp","")[:16],
                    "url":"https://dataminer2.pjm.com/feed/inst_load",
                })
        except Exception: pass

        # Forecasted outages
        try:
            r = await c.get(f"{PJM_BASE}/frcstd_gen_outages",
                params={"rowCount":"3","startRow":"1","datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
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

    # PJM + FERC RSS
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

# ── Polymarket — energy only, strict filter ───────────────────
# Only show if question explicitly mentions energy/power/oil/gas commodities
# Weather only if mentions North American grid regions
ENERGY_EXACT = ["electricity","natural gas","crude oil","oil price","gas price","lng",
                "megawatt","nuclear power","coal","solar energy","wind energy","pipeline",
                "carbon price","emissions","energy crisis","power grid","electric grid",
                "barrel","opec","refinery","energy price","pjm","ercot","power market"]
WEATHER_NA   = ["texas heat","chicago heat","us heat","east coast heat","midwest heat",
                "gulf coast","hurricane texas","hurricane florida","hurricane louisiana",
                "new york temperature","chicago temperature","texas temperature",
                "florida temperature","california temperature"]

async def fetch_polymarket() -> list:
    cached = cache_get("polymarket", 300)
    if cached: return cached
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://gamma-api.polymarket.com/markets",
                            params={"closed":"false","limit":500,"order":"volume","ascending":"false"})
            r.raise_for_status()
            markets = r.json()
    except Exception:
        return []
    results = []
    for m in markets:
        q = (m.get("question") or m.get("title") or "").lower()
        is_energy_market = any(k in q for k in ENERGY_EXACT)
        is_na_weather    = any(k in q for k in WEATHER_NA)
        if not (is_energy_market or is_na_weather):
            continue
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

@app.get("/api/debug/polymarket")
async def debug_polymarket():
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://gamma-api.polymarket.com/markets",
                            params={"closed":"false","limit":30,"order":"volume","ascending":"false"})
        if r.status_code != 200:
            return JSONResponse({"status":r.status_code,"error":r.text[:200]})
        markets = r.json()
        return JSONResponse({
            "count": len(markets),
            "top_questions": [(m.get("question") or m.get("title",""))[:100] for m in markets[:30]]
        })
    except Exception as e:
        return JSONResponse({"error": str(e)})

@app.get("/api/all")
async def api_all():
    lmps, intraday, load, gas, da, markets, news = await asyncio.gather(
        fetch_lmps(), fetch_intraday(), fetch_load(),
        fetch_gas(), fetch_da_lmps(), fetch_polymarket(), fetch_news(),
        return_exceptions=True,
    )
    da_list = []
    if isinstance(da, dict):
        for name, v in da.items():
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
