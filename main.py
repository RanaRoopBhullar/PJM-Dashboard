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

# Exact pnode_name values as they appear in PJM data
PJM_HUBS = [
    "WESTERN HUB", "EASTERN HUB", "AEP-DAYTON HUB",
    "N ILLINOIS HUB", "NI HUB", "PECO", "PPL", "BGE", "DOMINION"
]

async def fetch_all_lmp_rows(client, feed, dt_keyword, row_count=500, start_row=1):
    """Fetch a page of LMP rows from PJM."""
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    params  = {
        "rowCount":               str(row_count),
        "startRow":               str(start_row),
        "datetime_beginning_ept": dt_keyword,
        "order":                  "Asc",
        "sort":                   "pnode_name",
    }
    r = await client.get(f"{PJM_BASE}/{feed}", params=params, headers=headers)
    if r.status_code != 200:
        return [], 0
    data       = r.json()
    items      = data.get("items", [])
    total_rows = int(data.get("totalRows", 0))
    return items, total_rows


async def fetch_lmps() -> list:
    cached = cache_get("lmps")
    if cached:
        return cached

    if not PJM_API_KEY:
        raise HTTPException(status_code=503, detail="PJM_API_KEY not configured")

    hubs_upper = {h.upper(): h for h in PJM_HUBS}
    results    = {}

    async with httpx.AsyncClient(timeout=30) as client:
        # Page through results until we find all hubs or exhaust data
        start_row  = 1
        page_size  = 500
        total_rows = None

        while len(results) < len(PJM_HUBS):
            if total_rows is not None and start_row > total_rows:
                break

            items, total_rows = await fetch_all_lmp_rows(
                client, "rt_unverified_hrl_lmps", "LastHour", page_size, start_row
            )
            if not items:
                break

            for row in items:
                name = (row.get("pnode_name") or "").upper().strip()
                if name in hubs_upper and name not in results:
                    lmp    = float(row.get("total_lmp_rt") or row.get("total_lmp") or 0)
                    energy = float(row.get("energy_lmp_rt") or row.get("system_energy_price_rt") or 0)
                    cong   = float(row.get("congestion_price_rt") or row.get("congestion_price") or 0)
                    loss   = float(row.get("marginal_loss_lmp_rt") or row.get("marginal_loss_lmp") or 0)
                    results[name] = {
                        "name":       row.get("pnode_name"),
                        "type":       row.get("type", "Hub"),
                        "lmp":        round(lmp, 2),
                        "energy":     round(energy, 2),
                        "congestion": round(cong, 2),
                        "loss":       round(loss, 2),
                        "hour":       row.get("datetime_beginning_ept", ""),
                    }

            start_row += page_size

            # Stop if we found everything
            if len(results) == len(PJM_HUBS):
                break

    # Sort by preferred order
    order     = {h.upper(): i for i, h in enumerate(PJM_HUBS)}
    final     = sorted(results.values(), key=lambda x: order.get(x["name"].upper().strip(), 99))

    if final:
        cache_set("lmps", final)
    return final


async def fetch_intraday() -> list:
    cached = cache_get("intraday")
    if cached:
        return cached

    if not PJM_API_KEY:
        return []

    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    params  = {
        "rowCount":               "50",
        "startRow":               "1",
        "pnode_name":             "WESTERN HUB",
        "datetime_beginning_ept": "Today",
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{PJM_BASE}/rt_unverified_hrl_lmps", params=params, headers=headers)
        if r.status_code != 200:
            return []
        items = r.json().get("items", [])

    rows_sorted = sorted(items, key=lambda x: x.get("datetime_beginning_ept", ""))
    result = [
        {
            "hour": row.get("datetime_beginning_ept", ""),
            "lmp":  round(float(row.get("total_lmp_rt") or row.get("total_lmp") or 0), 2)
        }
        for row in rows_sorted
    ]

    if result:
        cache_set("intraday", result)
    return result


async def fetch_lmps_debug() -> dict:
    if not PJM_API_KEY:
        return {"error": "no key"}
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}
    debug   = {}

    async with httpx.AsyncClient(timeout=20) as client:
        # Check what pnode_names look like for hubs specifically
        for kw in ["LastHour", "Today"]:
            params = {
                "rowCount": "500", "startRow": "1",
                "datetime_beginning_ept": kw,
                "type": "HUB",
            }
            r = await client.get(f"{PJM_BASE}/rt_unverified_hrl_lmps", params=params, headers=headers)
            try:
                data = r.json()
            except Exception:
                data = {}
            items = data.get("items", []) if isinstance(data, dict) else []
            debug[f"hubs_only|{kw}"] = {
                "status":      r.status_code,
                "total_rows":  data.get("totalRows", "?"),
                "item_count":  len(items),
                "pnode_names": [i.get("pnode_name") for i in items[:20]],
            }
    return debug


POWER_KEYWORDS = ["electricity", "power", "energy", "grid", "PJM", "ERCOT",
                  "natural gas", "coal", "solar", "wind", "megawatt", "nuclear"]

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
                   "natural gas", "coal", "solar", "wind", "PJM", "FERC",
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
                root  = ET.fromstring(r.text)
                ns    = {"atom": "http://www.w3.org/2005/Atom"}
                items = root.findall(".//item") or root.findall(".//atom:entry", ns)
                for item in items[:12]:
                    title   = (item.findtext("title") or item.findtext("atom:title", namespaces=ns) or "").strip()
                    desc    = (item.findtext("description") or item.findtext("atom:summary", namespaces=ns) or "").strip()
                    pub     = (item.findtext("pubDate") or item.findtext("atom:updated", namespaces=ns) or "").strip()
                    link_el = item.find("atom:link", ns)
                    link    = item.findtext("link") or (link_el.get("href") if link_el is not None else "") or ""
                    if any(kw.lower() in (title + desc).lower() for kw in ENERGY_KEYWORDS):
                        articles.append({
                            "source":  source,
                            "title":   title,
                            "snippet": desc[:200].strip() + ("…" if len(desc) > 200 else ""),
                            "pub":     pub[:25],
                            "url":     link.strip(),
                        })
            except Exception:
                continue

    if not articles:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            try:
                r    = await client.get("https://www.eia.gov/rss/press_releases.xml", headers={"User-Agent": "Mozilla/5.0"})
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
        fetch_lmps(), fetch_intraday(), fetch_polymarket(), fetch_news(),
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
