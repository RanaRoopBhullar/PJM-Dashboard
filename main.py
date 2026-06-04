"""
PJM Power Dashboard — Backend Server
News via PJM API operational data + Claude-powered summaries via Anthropic API
"""

import os
import httpx
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import json

load_dotenv()

app = FastAPI(title="PJM Dashboard API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

PJM_API_KEY = os.getenv("PJM_API_KEY", "")
PJM_BASE    = "https://api.pjm.com/api/v1"

_cache: dict = {}
CACHE_TTL = 300

def cache_get(key):
    entry = _cache.get(key)
    if entry and (datetime.now(timezone.utc).timestamp() - entry["ts"]) < CACHE_TTL:
        return entry["data"]
    return None

def cache_set(key, data):
    _cache[key] = {"ts": datetime.now(timezone.utc).timestamp(), "data": data}

PJM_HUBS = [
    "WESTERN HUB", "EASTERN HUB", "AEP-DAYTON HUB", "N ILLINOIS HUB",
    "NEW JERSEY HUB", "CHICAGO HUB", "CHICAGO GEN HUB", "AEP GEN HUB",
    "OHIO HUB", "DOMINION HUB", "ATSI GEN HUB", "WEST INT HUB",
]

# ---------------------------------------------------------------------------
# LMPs
# ---------------------------------------------------------------------------
async def fetch_lmps() -> list:
    cached = cache_get("lmps")
    if cached:
        return cached
    if not PJM_API_KEY:
        raise HTTPException(status_code=503, detail="PJM_API_KEY not configured")
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    params  = {"rowCount":"500","startRow":"1","type":"HUB","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{PJM_BASE}/rt_unverified_hrl_lmps", params=params, headers=headers)
        if r.status_code != 200:
            return []
        items = r.json().get("items", [])
    if not items:
        return []
    latest_hour = items[0].get("datetime_beginning_ept", "")
    seen, results = set(), []
    for row in items:
        if row.get("datetime_beginning_ept") != latest_hour:
            continue
        name = (row.get("pnode_name") or "").upper().strip()
        if name in seen:
            continue
        seen.add(name)
        lmp  = float(row.get("total_lmp_rt") or 0)
        cong = float(row.get("congestion_price_rt") or 0)
        sys_e = row.get("system_energy_price_rt")
        if sys_e is not None:
            energy = float(sys_e)
            loss   = round(lmp - energy - cong, 2)
        else:
            energy = round(lmp - cong, 2)
            loss   = 0.0
        results.append({"name":row.get("pnode_name"),"type":"Hub","lmp":round(lmp,2),"energy":round(energy,2),"congestion":round(cong,2),"loss":loss,"hour":latest_hour})
    order = {h.upper():i for i,h in enumerate(PJM_HUBS)}
    results.sort(key=lambda x: order.get(x["name"].upper().strip(), 99))
    if results:
        cache_set("lmps", results)
    return results

# ---------------------------------------------------------------------------
# Intraday
# ---------------------------------------------------------------------------
async def fetch_intraday() -> list:
    cached = cache_get("intraday")
    if cached:
        return cached
    if not PJM_API_KEY:
        return []
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    params  = {"rowCount":"50","startRow":"1","pnode_name":"WESTERN HUB","datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{PJM_BASE}/rt_unverified_hrl_lmps", params=params, headers=headers)
        if r.status_code != 200:
            return []
        items = r.json().get("items", [])
    result = [{"hour":row.get("datetime_beginning_ept",""),"lmp":round(float(row.get("total_lmp_rt") or 0),2)} for row in items]
    if result:
        cache_set("intraday", result)
    return result

# ---------------------------------------------------------------------------
# Load Forecast
# ---------------------------------------------------------------------------
async def fetch_load_forecast() -> dict:
    cached = cache_get("load_forecast")
    if cached:
        return cached
    if not PJM_API_KEY:
        return {}
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    result = {"forecast":[], "current_actual":None, "peak_forecast":None}
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r = await client.get(f"{PJM_BASE}/load_frcstd_7day",
                params={"rowCount":"48","startRow":"1","datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
                headers=headers)
            if r.status_code == 200:
                items = r.json().get("items", [])
                result["forecast"] = [{"hour":row.get("datetime_beginning_ept",""),"mw":float(row.get("rto_total") or row.get("total_load_forecast") or 0)} for row in items]
                if result["forecast"]:
                    result["peak_forecast"] = max(r["mw"] for r in result["forecast"])
        except Exception:
            pass
        try:
            r = await client.get(f"{PJM_BASE}/inst_load",
                params={"rowCount":"50","startRow":"1","datetime_beginning_ept":"5MinutesAgo"},
                headers=headers)
            if r.status_code == 200:
                items = r.json().get("items", [])
                # Find PJM RTO total (largest value)
                rto_val = 0
                for item in items:
                    area = (item.get("area") or "").upper()
                    val  = float(item.get("instantaneous_load") or 0)
                    if "RTO" in area or "PJM RTO" in area:
                        rto_val = val
                        break
                    if val > rto_val and val > 50000:
                        rto_val = val
                if rto_val:
                    result["current_actual"] = rto_val
        except Exception:
            pass
    if result["forecast"] or result["current_actual"]:
        cache_set("load_forecast", result)
    return result

# ---------------------------------------------------------------------------
# Gas Prices — use PJM API Henry Hub proxy or EIA
# ---------------------------------------------------------------------------
async def fetch_gas_prices() -> list:
    cached = cache_get("gas_prices")
    if cached:
        return cached
    prices = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://api.eia.gov/v2/natural-gas/pri/sum/data/",
                params={"api_key":"DEMO_KEY","frequency":"daily","data[0]":"value","facets[series][]":"RNGWHHD","sort[0][column]":"period","sort[0][direction]":"desc","length":"2"}
            )
            if r.status_code == 200:
                rows = r.json().get("response",{}).get("data",[])
                for row in rows[:1]:
                    prices.append({"hub":"Henry Hub","date":row.get("period",""),"price":round(float(row.get("value") or 0),3),"unit":"$/MMBtu"})
    except Exception:
        pass
    if not prices:
        prices = [{"hub":"Henry Hub","date":"—","price":0,"unit":"$/MMBtu"}]
    cache_set("gas_prices", prices)
    return prices

# ---------------------------------------------------------------------------
# News — PJM API operational data (no external RSS needed)
# ---------------------------------------------------------------------------
async def fetch_news() -> list:
    cached = cache_get("news")
    if cached:
        return cached
    if not PJM_API_KEY:
        return []

    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    articles = []

    async with httpx.AsyncClient(timeout=20) as client:

        # 1. Generation Outages by Type — major outages affecting capacity
        try:
            r = await client.get(f"{PJM_BASE}/gen_outages_by_type",
                params={"rowCount":"10","startRow":"1","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
                headers=headers)
            if r.status_code == 200:
                items = r.json().get("items", [])
                for item in items[:5]:
                    total = float(item.get("total_outages_mw") or item.get("rto_total") or 0)
                    forced = float(item.get("forced_outages_mw") or item.get("rto_forced") or 0)
                    hour = item.get("datetime_beginning_ept","")
                    if total > 0:
                        articles.append({
                            "source": "PJM Outages",
                            "category": "⚡ Ops",
                            "title": f"PJM Generation Outages: {total:,.0f} MW total, {forced:,.0f} MW forced",
                            "snippet": f"Total generation outages: {total:,.0f} MW. Forced outages: {forced:,.0f} MW. Maintenance: {(total-forced):,.0f} MW.",
                            "pub": hour[:16] if hour else "",
                            "url": "https://dataminer2.pjm.com/feed/gen_outages_by_type",
                        })
                        break
        except Exception:
            pass

        # 2. Forecasted Generation Outages
        try:
            r = await client.get(f"{PJM_BASE}/frcstd_gen_outages",
                params={"rowCount":"5","startRow":"1","datetime_beginning_ept":"Today","order":"Asc","sort":"datetime_beginning_ept"},
                headers=headers)
            if r.status_code == 200:
                items = r.json().get("items", [])
                for item in items[:3]:
                    rto = float(item.get("forecast_gen_outage_mw_rto") or 0)
                    date = item.get("forecast_date","")
                    if rto > 0:
                        articles.append({
                            "source": "PJM Forecast",
                            "category": "⚡ Ops",
                            "title": f"Forecasted Outage: {rto:,.0f} MW RTO ({date[:10]})",
                            "snippet": f"PJM forecasts {rto:,.0f} MW of generation outages for {date[:10]}.",
                            "pub": date[:16] if date else "",
                            "url": "https://dataminer2.pjm.com/feed/frcstd_gen_outages",
                        })
        except Exception:
            pass

        # 3. Reserve Market / Capacity
        try:
            r = await client.get(f"{PJM_BASE}/reserve_market_results",
                params={"rowCount":"5","startRow":"1","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
                headers=headers)
            if r.status_code == 200:
                items = r.json().get("items", [])
                for item in items[:2]:
                    articles.append({
                        "source": "PJM Reserves",
                        "category": "⚡ Ops",
                        "title": f"Reserve Market Result: {item.get('reserve_zone','RTO')}",
                        "snippet": str({k:v for k,v in item.items() if v and k not in ("datetime_beginning_utc",)})[:200],
                        "pub": (item.get("datetime_beginning_ept",""))[:16],
                        "url": "https://dataminer2.pjm.com/feed/reserve_market_results",
                    })
        except Exception:
            pass

        # 4. LMP Spikes — flag any hub >$100 or <-$10 as a news item
        try:
            lmps = cache_get("lmps") or await fetch_lmps()
            high = [h for h in lmps if h["lmp"] > 80]
            low  = [h for h in lmps if h["lmp"] < 0]
            if high:
                names = ", ".join(h["name"] for h in high)
                articles.append({
                    "source": "PJM LMP Alert",
                    "category": "⚡ Ops",
                    "title": f"High LMP Alert: {names} above $80/MWh",
                    "snippet": ", ".join(f"{h['name']}: ${h['lmp']}" for h in high),
                    "pub": (high[0].get("hour",""))[:16],
                    "url": "https://dataminer2.pjm.com/feed/rt_unverified_hrl_lmps",
                })
            if low:
                names = ", ".join(h["name"] for h in low)
                articles.append({
                    "source": "PJM LMP Alert",
                    "category": "⚡ Ops",
                    "title": f"Negative LMP: {names}",
                    "snippet": ", ".join(f"{h['name']}: ${h['lmp']}" for h in low),
                    "pub": (low[0].get("hour",""))[:16],
                    "url": "https://dataminer2.pjm.com/feed/rt_unverified_hrl_lmps",
                })
        except Exception:
            pass

        # 5. DA vs RT spread context
        try:
            r = await client.get(f"{PJM_BASE}/da_hrl_lmps",
                params={"rowCount":"50","startRow":"1","type":"HUB","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"},
                headers=headers)
            if r.status_code == 200:
                da_items = r.json().get("items",[])
                if da_items:
                    latest = da_items[0].get("datetime_beginning_ept","")
                    da_map = {}
                    for row in da_items:
                        if row.get("datetime_beginning_ept") == latest:
                            da_map[row.get("pnode_name","").upper()] = float(row.get("total_lmp_da") or 0)
                    lmps = cache_get("lmps") or []
                    spreads = []
                    for h in lmps:
                        da = da_map.get(h["name"].upper())
                        if da:
                            spread = h["lmp"] - da
                            spreads.append((h["name"], h["lmp"], da, spread))
                    if spreads:
                        biggest = sorted(spreads, key=lambda x: abs(x[3]), reverse=True)[:3]
                        lines = [f"{n}: RT ${rt:.2f} vs DA ${da:.2f} (Δ{'+' if sp>0 else ''}{sp:.2f})" for n,rt,da,sp in biggest]
                        articles.append({
                            "source": "PJM DA/RT",
                            "category": "📰 News",
                            "title": f"DA/RT Spread: Largest divergence at {biggest[0][0]}",
                            "snippet": " | ".join(lines),
                            "pub": latest[:16],
                            "url": "https://dataminer2.pjm.com/feed/da_hrl_lmps",
                        })
        except Exception:
            pass

    # PJM RSS + FERC RSS -- confirmed accessible from Railway
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as rss_client:
            for src, url in [("PJM", "https://www.pjm.com/rss"), ("FERC", "https://www.ferc.gov/news-events/news/rss.xml")]:
                try:
                    r = await rss_client.get(url, headers={"User-Agent":"Mozilla/5.0 Chrome/120"})
                    if r.status_code != 200:
                        continue
                    root  = ET.fromstring(r.text)
                    items = root.findall(".//item")
                    ENERGY_KW = ["power","energy","electricity","grid","lmp","capacity","natural gas","coal","solar","wind","pjm","ferc","megawatt","mw","transmission","congestion","renewable","nuclear","generation","utility","outage","demand","load","curtailment","emergency","alert","market","rate","tariff"]
                    for item in items[:20]:
                        title = (item.findtext("title") or "").strip()
                        desc  = (item.findtext("description") or "").strip()
                        pub   = (item.findtext("pubDate") or "").strip()
                        link  = (item.findtext("link") or "").strip()
                        if any(k in (title+" "+desc).lower() for k in ENERGY_KW):
                            articles.append({"source":src,"category":"⚡ Ops" if src=="PJM" else "📰 News","title":title,"snippet":desc[:220].strip()+("…" if len(desc)>220 else ""),"pub":pub[:25],"url":link})
                except Exception:
                    continue
    except Exception:
        pass

    articles.sort(key=lambda x: (0 if "Ops" in x.get("category","") else 1))
    cache_set("news", articles[:20])
    return articles[:20]


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------
async def fetch_polymarket() -> list:
    cached = cache_get("polymarket")
    if cached:
        return cached
    POWER_KW = ["electricity","power","energy","grid","PJM","ERCOT","natural gas","coal","solar","wind","megawatt","nuclear","utility","transmission","oil","gas","LNG"]
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get("https://gamma-api.polymarket.com/markets",params={"closed":"false","limit":200,"order":"volume","ascending":"false"})
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
            results.append({"question":m.get("question") or m.get("title"),"volume":m.get("volume","—"),"end_date":(m.get("endDate") or "")[:10],"url":f"https://polymarket.com/event/{m.get('slug','')}","outcomes":outcomes})
        if len(results) >= 8:
            break
    cache_set("polymarket", results)
    return results

# ---------------------------------------------------------------------------
# Network test
# ---------------------------------------------------------------------------
@app.get("/api/nettest")
async def api_nettest():
    results = {}
    urls = [
        ("eia_rss",   "https://www.eia.gov/rss/press_releases.xml"),
        ("ferc_rss",  "https://www.ferc.gov/news-events/news/rss.xml"),
        ("nrc_rss",   "https://www.nrc.gov/reading-rm/doc-collections/event-status/rss/en.xml"),
        ("pjm_rss",   "https://www.pjm.com/rss"),
        ("polymarket","https://gamma-api.polymarket.com/markets?limit=1"),
    ]
    async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
        for name, url in urls:
            try:
                r = await client.get(url, headers={"User-Agent":"Mozilla/5.0 Chrome/120"})
                results[name] = {"status": r.status_code, "len": len(r.text)}
            except Exception as e:
                results[name] = {"error": str(e)[:80]}
    return JSONResponse(results)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/api/lmps")
async def api_lmps():
    try:    return JSONResponse(await fetch_lmps())
    except HTTPException as e: raise e
    except Exception as e:     raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lmps/debug")
async def api_lmps_debug():
    if not PJM_API_KEY: return JSONResponse({"error":"no key"})
    headers = {"Ocp-Apim-Subscription-Key":PJM_API_KEY,"Accept":"application/json"}
    params  = {"rowCount":"5","startRow":"1","type":"HUB","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{PJM_BASE}/rt_unverified_hrl_lmps",params=params,headers=headers)
        data = r.json() if r.status_code==200 else {}
    return JSONResponse({"status":r.status_code,"total_rows":data.get("totalRows",0),"sample":data.get("items",[])[:3]})

@app.get("/api/load/debug")
async def api_load_debug():
    if not PJM_API_KEY: return JSONResponse({"error":"no key"})
    headers = {"Ocp-Apim-Subscription-Key":PJM_API_KEY,"Accept":"application/json"}
    debug = {}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{PJM_BASE}/inst_load",
            params={"rowCount":"10","startRow":"1","datetime_beginning_ept":"5MinutesAgo"},headers=headers)
        data = r.json() if r.status_code==200 else {}
        debug["inst_load"] = {"status":r.status_code,"total_rows":data.get("totalRows",0),"sample":data.get("items",[])[:5]}
        r = await client.get(f"{PJM_BASE}/load_frcstd_7day",
            params={"rowCount":"5","startRow":"1","datetime_beginning_ept":"Today"},headers=headers)
        data = r.json() if r.status_code==200 else {}
        debug["load_frcstd"] = {"status":r.status_code,"total_rows":data.get("totalRows",0),"sample":data.get("items",[])[:2]}
    return JSONResponse(debug)

@app.get("/api/intraday")
async def api_intraday():
    try:    return JSONResponse(await fetch_intraday())
    except Exception as e: raise HTTPException(status_code=500,detail=str(e))

@app.get("/api/load")
async def api_load():
    try:    return JSONResponse(await fetch_load_forecast())
    except Exception as e: raise HTTPException(status_code=500,detail=str(e))

@app.get("/api/gas")
async def api_gas():
    try:    return JSONResponse(await fetch_gas_prices())
    except Exception as e: raise HTTPException(status_code=500,detail=str(e))

@app.get("/api/polymarket")
async def api_polymarket():
    try:    return JSONResponse(await fetch_polymarket())
    except Exception as e: raise HTTPException(status_code=500,detail=str(e))

@app.get("/api/news")
async def api_news():
    try:    return JSONResponse(await fetch_news())
    except Exception as e: raise HTTPException(status_code=500,detail=str(e))

@app.get("/api/all")
async def api_all():
    lmps,intraday,load,gas,markets,news = await asyncio.gather(
        fetch_lmps(),fetch_intraday(),fetch_load_forecast(),fetch_gas_prices(),fetch_polymarket(),fetch_news(),
        return_exceptions=True,
    )
    return JSONResponse({
        "lmps":    lmps     if not isinstance(lmps,Exception)     else [],
        "intraday":intraday if not isinstance(intraday,Exception)  else [],
        "load":    load     if not isinstance(load,Exception)      else {},
        "gas":     gas      if not isinstance(gas,Exception)       else [],
        "markets": markets  if not isinstance(markets,Exception)   else [],
        "news":    news     if not isinstance(news,Exception)      else [],
        "updated_at":datetime.now(timezone.utc).isoformat(),
    })

@app.get("/health")
async def health():
    return {"status":"ok","pjm_key_set":bool(PJM_API_KEY),"key_prefix":PJM_API_KEY[:6]+"..." if PJM_API_KEY else ""}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open(os.path.join(os.path.dirname(__file__),"dashboard.html")) as f:
        return f.read()
