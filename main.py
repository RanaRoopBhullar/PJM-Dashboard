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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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

def get_eastern_now():
    return datetime.now(timezone.utc) - timedelta(hours=4)

PJM_HUBS = [
    "WESTERN HUB", "EASTERN HUB", "AEP-DAYTON HUB",
    "N ILLINOIS HUB", "NI HUB", "PECO", "PPL", "BGE", "DOMINION"
]

async def fetch_lmps() -> list:
    cached = cache_get("lmps")
    if cached:
        return cached

    if not PJM_API_KEY:
        raise HTTPException(status_code=503, detail="PJM_API_KEY not configured")

    headers = {
        "Ocp-Apim-Subscription-Key": PJM_API_KEY,
        "Accept": "application/json",
    }

    now_et = get_eastern_now()
    rows = []

    # Try up to 3 hours back to find data
    async with httpx.AsyncClient(timeout=20) as client:
        for hours_back in range(0, 4):
            try_et = now_et - timedelta(hours=hours_back)
            dt_str = try_et.strftime("%Y-%m-%d %H:00")
            params = {
                "startRow": 1,
                "rowCount": 500,
                "datetime_beginning_ept": dt_str,
            }
            r = await client.get(f"{PJM_BASE}/rt_hrl_lmps", params=params, headers=headers)
            if r.status_code == 200:
                data = r.json()
                rows = data.get("items", [])
                if rows:
                    break

    if not rows:
        return []

    results = []
    seen = set()
    for row in rows:
        name = (row.get("pnode_name") or "").upper().strip()
        if name in PJM_HUBS and name not in seen:
            seen.add(name)
            lmp    = float(row.get("total_lmp_rt") or 0)
            energy = float(row.get("energy_lmp_rt") or 0)
            cong   = float(row.get("congestion_price_rt") or 0)
            loss   = float(row.get("marginal_loss_lmp_rt") or 0)
            results.append({
                "name":       row.get("pnode_name"),
                "type":       row.get("type", "Hub"),
                "lmp":        round(lmp, 2),
                "energy":     round(energy, 2),
                "congestion": round(cong, 2),
                "loss":       round(loss, 2),
                "hour":       row.get("datetime_beginning_ept", ""),
            })

    order = {h: i for i, h in enumerate(PJM_HUBS)}
    results.sort(key=lambda x: order.get(x["name"].upper().strip(), 99))

    if results:
        cache_set("lmps", results)
    return results


async def fetch_intraday() -> list:
    cached = cache_get("intraday")
    if cached:
        return cached

    if not PJM_API_KEY:
        return []

    now_et = get_eastern_now()
    start  = now_et.strftime("%Y-%m-%d 00:00")
    end    = now_et.strftime("%Y-%m-%d %H:00")

    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    params = {
        "startRow": 1,
        "rowCount": 24,
        "pnode_name": "WESTERN HUB",
        "datetime_beginning_ept": start,
        "datetime_ending_ept": end,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{PJM_BASE}/rt_hrl_lmps", params=params, headers=headers)
        if r.status_code != 200:
            return []
        data = r.json()

    rows = data.get("items", [])
    rows_sorted = sorted(rows, key=lambda x: x.get("datetime_beginning_ept", ""))
    result = [{"hour": row.get("datetime_beginning_ept", ""), "lmp": round(float(row.get("total_lmp_rt") or 0), 2)} for row in rows_sorted]

    if result:
        cache_set("intraday", result)
    return result


# Debug endpoint — shows raw PJM response
async def fetch_lmps_debug() -> dict:
    if not PJM_API_KEY:
        return {"error": "no key"}
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    now_et = get_eastern_now()
    debug_info = {}
    async with httpx.AsyncClient(timeout=20) as client:
        for hours_back in range(0, 4):
            try_et = now_et - timedelta(hours=hours_back)
            dt_str = try_et.strftime("%Y-%m-%d %H:00")
            params = {"startRow": 1, "rowCount": 5, "datetime_beginning_ept": dt_str}
            r = await client.get(f"{PJM_BASE}/rt_hrl_lmps", params=params, headers=headers)
            data = r.json() if r.status_code == 200 else {"error": r.status_code, "body": r.text[:500]}
            items = data.get("items", []) if isinstance(data, dict) else []
            debug_info[f"hour_minus_{hours_back}_{dt_str}"] = {
                "status": r.status_code,
                "item_count": len(items),
                "sample": items[:2] if items else data
            }
    return debug_info


POWER_KEYWORDS = ["electricity", "power", "energy", "grid", "PJM", "ERCOT", "utility",
                  "natural gas", "coal", "solar", "wind", "megawatt", "kilowatt", "nuclear"]

async def fetch_polymarket() -> list:
    cached = cache_get("polymarket")
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            "https://gamma-api.polymarket.com/markets",
            params={"closed": "false", "limit": 200, "order": "volume", "ascending": "false"},
        )
        r.raise_for_status()
        markets = r.json()

    results = []
    for m in markets:
        question = (m.get("question") or m.get("title") or "").lower()
        if any(kw.lower() in question for kw in POWER_KEYWORDS):
            outcomes = []
            try:
                prices = json.loads(m.get("outcomePrices") or "[]")
                names  = json.loads(m.get("outcomes") or "[]")
                for name, price in zip(names, prices):
                    outcomes.append({"label": name, "pct": round(float(price) * 100, 1)})
            except Exception:
                pass
            results.append({
                "question": m.get("question") or m.get("title"),
                "volume":   m.get("volume", "—"),
                "end_date": (m.get("endDate") or m.get("end_date_iso") or "")[:10],
                "url":      f"https://polymarket.com/event/{m.get('slug', '')}",
                "outcomes": outcomes,
            })
        if len(results) >= 6:
            break

    cache_set("polymarket", results)
    return results


RSS_FEEDS = [
    ("EIA",  "https://www.eia.gov/rss/press_releases.xml"),
    ("FERC", "https://www.ferc.gov/news-events/news/rss.xml"),
]

ENERGY_KEYWORDS = ["power", "energy", "electricity", "grid", "LMP", "capacity",
                   "natural gas", "coal", "solar", "wind", "PJM", "FERC", "utility",
                   "megawatt", "transmission", "congestion", "renewable", "nuclear", "generation"]

async def fetch_news() -> list:
    cached = cache_get("news")
    if cached:
        return cached

    articles = []
    async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
        for source, url in RSS_FEEDS:
            try:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 PJMDashboard/1.0"})
                if r.status_code != 200:
                    continue
                root = ET.fromstring(r.text)
                ns = {"atom": "http://www.w3.org/2005/Atom"}
                items = root.findall(".//item") or root.findall(".//atom:entry", ns)
                for item in items[:12]:
                    title   = (item.findtext("title") or item.findtext("atom:title", namespaces=ns) or "").strip()
                    desc    = (item.findtext("description") or item.findtext("atom:summary", namespaces=ns) or "").strip()
                    pub     = (item.findtext("pubDate") or item.findtext("atom:updated", namespaces=ns) or "").strip()
                    link_el = item.find("atom:link", ns)
                    link    = item.findtext("link") or (link_el.get("href") if link_el is not None else "") or ""
                    combined = (title + " " + desc).lower()
                    if any(kw.lower() in combined for kw in ENERGY_KEYWORDS):
                        articles.append({
                            "source":  source,
                            "title":   title,
                            "snippet": desc[:200].strip() + ("…" if len(desc) > 200 else ""),
                            "pub":     pub[:25],
                            "url":     link.strip(),
                        })
            except Exception:
                continue

    # Fallback — show all EIA items if nothing matched
    if not articles:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            try:
                r = await client.get("https://www.eia.gov/rss/press_releases.xml",
                                     headers={"User-Agent": "Mozilla/5.0"})
                root = ET.fromstring(r.text)
                for item in root.findall(".//item")[:10]:
                    articles.append({
                        "source":  "EIA",
                        "title":   (item.findtext("title") or "").strip(),
                        "snippet": (item.findtext("description") or "")[:200].strip(),
                        "pub":     (item.findtext("pubDate") or "")[:25],
                        "url":     (item.findtext("link") or "").strip(),
                    })
            except Exception:
                pass

    cache_set("news", articles[:12])
    return articles[:12]


@app.get("/api/lmps")
async def api_lmps():
    try:
        return JSONResponse(await fetch_lmps())
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lmps/debug")
async def api_lmps_debug():
    return JSONResponse(await fetch_lmps_debug())

@app.get("/api/intraday")
async def api_intraday():
    try:
        return JSONResponse(await fetch_intraday())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/polymarket")
async def api_polymarket():
    try:
        return JSONResponse(await fetch_polymarket())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/news")
async def api_news():
    try:
        return JSONResponse(await fetch_news())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/all")
async def api_all():
    lmps, intraday, markets, news = await asyncio.gather(
        fetch_lmps(),
        fetch_intraday(),
        fetch_polymarket(),
        fetch_news(),
        return_exceptions=True,
    )
    return JSONResponse({
        "lmps":      lmps     if not isinstance(lmps, Exception)     else [],
        "intraday":  intraday if not isinstance(intraday, Exception)  else [],
        "markets":   markets  if not isinstance(markets, Exception)   else [],
        "news":      news     if not isinstance(news, Exception)      else [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })

@app.get("/health")
async def health():
    return {"status": "ok", "pjm_key_set": bool(PJM_API_KEY), "key_prefix": PJM_API_KEY[:6] + "..." if PJM_API_KEY else ""}

@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    with open(html_path, "r") as f:
        return f.read()
