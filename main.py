"""
PJM Power Dashboard — Backend Server
=====================================
Fetches live data from:
  - PJM Data Miner 2 API (real-time LMPs)
  - Polymarket API (prediction markets)
  - PJM + EIA RSS feeds (news)

Usage:
  pip install -r requirements.txt
  Set PJM_API_KEY in .env or as environment variable
  uvicorn main:app --host 0.0.0.0 --port 8080 --reload
"""

import os
import httpx
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional
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

# Cache to avoid hammering APIs
_cache: dict = {}
CACHE_TTL = 300  # seconds

def cache_get(key: str):
    entry = _cache.get(key)
    if entry and (datetime.now(timezone.utc).timestamp() - entry["ts"]) < CACHE_TTL:
        return entry["data"]
    return None

def cache_set(key: str, data):
    _cache[key] = {"ts": datetime.now(timezone.utc).timestamp(), "data": data}


# ---------------------------------------------------------------------------
# PJM LMP
# ---------------------------------------------------------------------------
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

    now = datetime.now(timezone.utc)
    # Round down to current hour
    dt_str = now.strftime("%Y-%m-%d %H:00")

    params = {
        "startRow": 1,
        "rowCount": 200,
        "fields": "datetime_beginning_ept,pnode_name,type,total_lmp_rt,energy_lmp_rt,congestion_price_rt,marginal_loss_lmp_rt",
        "datetime_beginning_ept": dt_str,
    }
    headers = {
        "Ocp-Apim-Subscription-Key": PJM_API_KEY,
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(f"{PJM_BASE}/rt_hrl_lmps", params=params, headers=headers)
        r.raise_for_status()
        data = r.json()

    rows = data.get("items", data) if isinstance(data, dict) else data

    # Filter to our target hubs/zones
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
                "name":   row.get("pnode_name"),
                "type":   row.get("type", "Hub"),
                "lmp":    round(lmp, 2),
                "energy": round(energy, 2),
                "congestion": round(cong, 2),
                "loss":   round(loss, 2),
            })

    # Sort by our preferred order
    order = {h: i for i, h in enumerate(PJM_HUBS)}
    results.sort(key=lambda x: order.get(x["name"].upper().strip(), 99))

    cache_set("lmps", results)
    return results


async def fetch_intraday() -> list:
    """Fetch today's hourly LMPs for Western Hub for the intraday chart."""
    cached = cache_get("intraday")
    if cached:
        return cached

    if not PJM_API_KEY:
        return []

    now = datetime.now(timezone.utc)
    start = now.strftime("%Y-%m-%d 00:00")
    end   = now.strftime("%Y-%m-%d %H:00")

    params = {
        "startRow": 1,
        "rowCount": 24,
        "fields": "datetime_beginning_ept,pnode_name,total_lmp_rt",
        "pnode_name": "WESTERN HUB",
        "datetime_beginning_ept": start,
        "datetime_ending_ept": end,
    }
    headers = {"Ocp-Apim-Subscription-Key": PJM_API_KEY, "Accept": "application/json"}

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(f"{PJM_BASE}/rt_hrl_lmps", params=params, headers=headers)
        r.raise_for_status()
        data = r.json()

    rows = data.get("items", data) if isinstance(data, dict) else data
    rows_sorted = sorted(rows, key=lambda x: x.get("datetime_beginning_ept", ""))

    result = [{"hour": row.get("datetime_beginning_ept", ""), "lmp": round(float(row.get("total_lmp_rt") or 0), 2)} for row in rows_sorted]
    cache_set("intraday", result)
    return result


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------
POWER_KEYWORDS = ["electricity", "power", "energy", "grid", "PJM", "ERCOT", "utility",
                  "natural gas", "coal", "solar", "wind", "megawatt", "kilowatt"]

async def fetch_polymarket() -> list:
    cached = cache_get("polymarket")
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            "https://gamma-api.polymarket.com/markets",
            params={"closed": "false", "limit": 100, "order": "volume", "ascending": "false"},
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


# ---------------------------------------------------------------------------
# News (RSS — free, no key)
# ---------------------------------------------------------------------------
RSS_FEEDS = [
    ("PJM",        "https://www.pjm.com/rss"),
    ("EIA",        "https://www.eia.gov/rss/press_releases.xml"),
    ("EPA",        "https://www.epa.gov/newsreleases/search/rss/field_press_office/headquarters"),
]

ENERGY_KEYWORDS = ["power", "energy", "electricity", "grid", "LMP", "capacity",
                   "natural gas", "coal", "solar", "wind", "PJM", "FERC", "utility",
                   "megawatt", "transmission", "congestion", "renewable"]

async def fetch_news() -> list:
    cached = cache_get("news")
    if cached:
        return cached

    articles = []
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        for source, url in RSS_FEEDS:
            try:
                r = await client.get(url, headers={"User-Agent": "PJMDashboard/1.0"})
                root = ET.fromstring(r.text)
                ns = {"atom": "http://www.w3.org/2005/Atom"}
                items = root.findall(".//item") or root.findall(".//atom:entry", ns)
                for item in items[:8]:
                    title   = (item.findtext("title") or item.findtext("atom:title", namespaces=ns) or "").strip()
                    desc    = (item.findtext("description") or item.findtext("atom:summary", namespaces=ns) or "").strip()
                    pub     = (item.findtext("pubDate") or item.findtext("atom:updated", namespaces=ns) or "").strip()
                    link    = (item.findtext("link") or item.findtext("atom:link", namespaces=ns) or "").strip()
                    combined = (title + " " + desc).lower()
                    if any(kw.lower() in combined for kw in ENERGY_KEYWORDS):
                        articles.append({
                            "source":  source,
                            "title":   title,
                            "snippet": desc[:180].strip() + ("…" if len(desc) > 180 else ""),
                            "pub":     pub[:22],
                            "url":     link,
                        })
            except Exception:
                continue

    cache_set("news", articles[:12])
    return articles[:12]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/api/lmps")
async def api_lmps():
    try:
        return JSONResponse(await fetch_lmps())
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
    """Single endpoint — fetches all data in parallel for fast page loads."""
    lmps, intraday, markets, news = await asyncio.gather(
        fetch_lmps(),
        fetch_intraday(),
        fetch_polymarket(),
        fetch_news(),
        return_exceptions=True,
    )
    return JSONResponse({
        "lmps":      lmps      if not isinstance(lmps, Exception)      else [],
        "intraday":  intraday  if not isinstance(intraday, Exception)   else [],
        "markets":   markets   if not isinstance(markets, Exception)    else [],
        "news":      news      if not isinstance(news, Exception)       else [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })

@app.get("/health")
async def health():
    return {"status": "ok", "pjm_key_set": bool(PJM_API_KEY)}

@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    with open(html_path, "r") as f:
        return f.read()
