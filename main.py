"""
PJM Power Dashboard — Backend Server
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
    params  = {
        "rowCount": "500", "startRow": "1",
        "type": "HUB",
        "datetime_beginning_ept": "Today",
        "order": "Desc", "sort": "datetime_beginning_ept",
    }

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

        results.append({
            "name":       row.get("pnode_name"),
            "type":       "Hub",
            "lmp":        round(lmp, 2),
            "energy":     round(energy, 2),
            "congestion": round(cong, 2),
            "loss":       loss,
            "hour":       latest_hour,
        })

    order = {h.upper(): i for i, h in enumerate(PJM_HUBS)}
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
    params  = {"rowCount": "50", "startRow": "1", "pnode_name": "WESTERN HUB",
               "datetime_beginning_ept": "Today", "order": "Asc", "sort": "datetime_beginning_ept"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{PJM_BASE}/rt_unverified_hrl_lmps", params=params, headers=headers)
        if r.status_code != 200:
            return []
        items = r.json().get("items", [])
    result = [{"hour": row.get("datetime_beginning_ept",""), "lmp": round(float(row.get("total_lmp_rt") or 0),2)} for row in items]
    if result:
        cache_set("intraday", result)
    return result


# ---------------------------------------------------------------------------
# Load Forecast — PJM inst_load feed for actual + load_frcstd_7day for forecast
# ---------------------------------------------------------------------------
async def fetch_load_forecast() -> dict:
    cached = cache_get("load_forecast")
    if cached:
        return cached
    if not PJM_API_KEY:
        return {}
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}

    result = {"forecast": [], "actual": [], "peak_forecast": None, "current_actual": None}

    async with httpx.AsyncClient(timeout=20) as client:
        # Forecast — today's hourly forecast
        try:
            r = await client.get(f"{PJM_BASE}/load_frcstd_7day",
                params={"rowCount":"48","startRow":"1","datetime_beginning_ept":"Today",
                        "order":"Asc","sort":"datetime_beginning_ept"}, headers=headers)
            if r.status_code == 200:
                items = r.json().get("items", [])
                result["forecast"] = [
                    {"hour": row.get("datetime_beginning_ept",""),
                     "mw":   float(row.get("rto_total") or row.get("total_load_forecast") or 0)}
                    for row in items
                ]
                if result["forecast"]:
                    result["peak_forecast"] = max(r["mw"] for r in result["forecast"])
        except Exception:
            pass

        # Actual instantaneous load
        try:
            r = await client.get(f"{PJM_BASE}/inst_load",
                params={"rowCount":"12","startRow":"1","datetime_beginning_ept":"CurrentHour",
                        "order":"Desc","sort":"datetime_beginning_ept"}, headers=headers)
            if r.status_code == 200:
                items = r.json().get("items", [])
                if items:
                    result["current_actual"] = float(items[0].get("rto_total") or items[0].get("mw") or 0)
                    result["actual"] = [
                        {"hour": row.get("datetime_beginning_ept",""),
                         "mw":   float(row.get("rto_total") or row.get("mw") or 0)}
                        for row in reversed(items)
                    ]
        except Exception:
            pass

    if result["forecast"] or result["actual"]:
        cache_set("load_forecast", result)
    return result


# ---------------------------------------------------------------------------
# Gas Prices — EIA free API
# ---------------------------------------------------------------------------
async def fetch_gas_prices() -> list:
    cached = cache_get("gas_prices")
    if cached:
        return cached

    prices = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # EIA open data v2 — Henry Hub daily spot price, no key needed for basic series
            r = await client.get(
                "https://api.eia.gov/v2/natural-gas/pri/sum/data/",
                params={
                    "api_key":             "DEMO_KEY",
                    "frequency":           "daily",
                    "data[0]":             "value",
                    "facets[series][]":    "RNGWHHD",
                    "sort[0][column]":     "period",
                    "sort[0][direction]":  "desc",
                    "length":              "3",
                }
            )
            if r.status_code == 200:
                rows = r.json().get("response", {}).get("data", [])
                for row in rows[:1]:
                    prices.append({
                        "hub":   "Henry Hub",
                        "date":  row.get("period",""),
                        "price": round(float(row.get("value") or 0), 3),
                        "unit":  "$/MMBtu",
                    })
    except Exception:
        pass

    if not prices:
        prices = [{"hub": "Henry Hub", "date": "—", "price": 0, "unit": "$/MMBtu"}]

    cache_set("gas_prices", prices)
    return prices


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------
RSS_FEEDS = [
    ("PJM Ops",   "https://www.pjm.com/rss/em-alerts",                                     True),
    ("PJM News",  "https://www.pjm.com/rss",                                               False),
    ("FERC",      "https://www.ferc.gov/news-events/news/rss.xml",                          False),
    ("EIA",       "https://www.eia.gov/rss/press_releases.xml",                             False),
    ("NRC",       "https://www.nrc.gov/reading-rm/doc-collections/event-status/rss/en.xml", False),
    ("NOAA",      "https://alerts.weather.gov/cap/us.php?x=1",                             False),
]

ENERGY_KW = [
    "power","energy","electricity","grid","lmp","capacity","natural gas","coal","solar","wind",
    "pjm","ferc","megawatt","mw","transmission","congestion","renewable","nuclear","generation",
    "utility","outage","demand","load","fuel","emissions","pipeline","curtailment","emergency",
    "alert","heat","storm","freeze","peak","reactor","substation","voltage","frequency",
]
WEATHER_KW = [
    "heat","storm","hurricane","tornado","blizzard","freeze","cold","extreme","warning","watch",
    "advisory","mid-atlantic","midwest","ohio","pennsylvania","virginia","illinois",
    "new jersey","maryland","indiana","michigan","delaware","chicago","pittsburgh","philadelphia",
]

async def fetch_news() -> list:
    cached = cache_get("news")
    if cached:
        return cached

    articles = []
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for source, url, always in RSS_FEEDS:
            try:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 PJMDashboard/1.0"}, timeout=8)
                if r.status_code != 200:
                    continue
                root  = ET.fromstring(r.text)
                ns    = {"atom": "http://www.w3.org/2005/Atom", "cap": "urn:oasis:names:tc:emergency:cap:1.1"}
                items = root.findall(".//item") or root.findall(".//atom:entry", ns)
                for item in items[:20]:
                    title   = (item.findtext("title") or item.findtext("atom:title", namespaces=ns) or "").strip()
                    desc    = (item.findtext("description") or item.findtext("atom:summary", namespaces=ns) or "").strip()
                    pub     = (item.findtext("pubDate") or item.findtext("atom:updated", namespaces=ns) or "").strip()
                    link_el = item.find("atom:link", ns)
                    link    = item.findtext("link") or (link_el.get("href") if link_el is not None else "") or ""
                    combined = (title + " " + desc).lower()

                    is_weather = source == "NOAA" or any(k in combined for k in WEATHER_KW)
                    is_nuclear = source == "NRC"  or "nuclear" in combined or "reactor" in combined
                    is_energy  = any(k in combined for k in ENERGY_KW)

                    if always or is_energy or is_weather or is_nuclear:
                        cat = "⚡ Ops" if source in ("PJM Ops","PJM News") else "🌩 Weather" if is_weather else "☢ Nuclear" if is_nuclear else "📰 News"
                        articles.append({
                            "source":   source,
                            "category": cat,
                            "title":    title,
                            "snippet":  desc[:220].strip() + ("…" if len(desc)>220 else ""),
                            "pub":      pub[:25],
                            "url":      link.strip(),
                        })
            except Exception:
                continue

    # Fallback
    if not articles:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            try:
                r    = await client.get("https://www.eia.gov/rss/press_releases.xml", headers={"User-Agent": "Mozilla/5.0"})
                root = ET.fromstring(r.text)
                for item in root.findall(".//item")[:10]:
                    articles.append({
                        "source": "EIA", "category": "📰 News",
                        "title":   (item.findtext("title") or "").strip(),
                        "snippet": (item.findtext("description") or "")[:220].strip(),
                        "pub":     (item.findtext("pubDate") or "")[:25],
                        "url":     (item.findtext("link") or "").strip(),
                    })
            except Exception:
                pass

    articles.sort(key=lambda x: (0 if "Ops" in x.get("category","") else 1 if "Weather" in x.get("category","") else 2 if "Nuclear" in x.get("category","") else 3))
    cache_set("news", articles[:20])
    return articles[:20]


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------
POWER_KW = ["electricity","power","energy","grid","PJM","ERCOT","natural gas","coal",
            "solar","wind","megawatt","nuclear","utility","transmission","oil","gas","LNG"]

async def fetch_polymarket() -> list:
    cached = cache_get("polymarket")
    if cached:
        return cached
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get("https://gamma-api.polymarket.com/markets",
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
                    outcomes.append({"label": name, "pct": round(float(price)*100,1)})
            except Exception:
                pass
            results.append({
                "question": m.get("question") or m.get("title"),
                "volume":   m.get("volume","—"),
                "end_date": (m.get("endDate") or "")[:10],
                "url":      f"https://polymarket.com/event/{m.get('slug','')}",
                "outcomes": outcomes,
            })
        if len(results) >= 8:
            break
    cache_set("polymarket", results)
    return results


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
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    params  = {"rowCount":"5","startRow":"1","type":"HUB","datetime_beginning_ept":"Today","order":"Desc","sort":"datetime_beginning_ept"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{PJM_BASE}/rt_unverified_hrl_lmps", params=params, headers=headers)
        data = r.json() if r.status_code==200 else {}
    items = data.get("items",[])
    return JSONResponse({"status":r.status_code,"total_rows":data.get("totalRows",0),"sample":items[:3]})

@app.get("/api/intraday")
async def api_intraday():
    try:    return JSONResponse(await fetch_intraday())
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/load")
async def api_load():
    try:    return JSONResponse(await fetch_load_forecast())
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/gas")
async def api_gas():
    try:    return JSONResponse(await fetch_gas_prices())
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/polymarket")
async def api_polymarket():
    try:    return JSONResponse(await fetch_polymarket())
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/news")
async def api_news():
    try:    return JSONResponse(await fetch_news())
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/all")
async def api_all():
    lmps, intraday, load, gas, markets, news = await asyncio.gather(
        fetch_lmps(), fetch_intraday(), fetch_load_forecast(),
        fetch_gas_prices(), fetch_polymarket(), fetch_news(),
        return_exceptions=True,
    )
    return JSONResponse({
        "lmps":     lmps     if not isinstance(lmps,Exception)     else [],
        "intraday": intraday if not isinstance(intraday,Exception)  else [],
        "load":     load     if not isinstance(load,Exception)      else {},
        "gas":      gas      if not isinstance(gas,Exception)       else [],
        "markets":  markets  if not isinstance(markets,Exception)   else [],
        "news":     news     if not isinstance(news,Exception)      else [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })

@app.get("/health")
async def health():
    return {"status":"ok","pjm_key_set":bool(PJM_API_KEY),"key_prefix":PJM_API_KEY[:6]+"..." if PJM_API_KEY else ""}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open(os.path.join(os.path.dirname(__file__), "dashboard.html")) as f:
        return f.read()
