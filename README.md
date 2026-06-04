# PJM Power Dashboard

Real-time LMP dashboard powered by PJM Data Miner API + Polymarket + RSS news feeds.

## Files
```
pjm_dashboard/
├── main.py           # FastAPI backend
├── dashboard.html    # Frontend (served by the backend)
├── requirements.txt  # Python dependencies
├── .env.example      # Environment variable template
├── Procfile          # Railway deployment
└── railway.toml      # Railway config
```

---

## Step 1 — Get your PJM API key
1. Go to https://apiportal.pjm.com
2. Sign up / sign in
3. Subscribe to **Data Miner 2**
4. Copy your API key from your profile

---

## Step 2 — Run locally (test it works)

```bash
# Install Python dependencies
pip install -r requirements.txt

# Create your .env file
cp .env.example .env
# Edit .env and paste your PJM API key

# Start the server
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

Open http://localhost:8080 — you should see live data.

---

## Step 3 — Deploy to Railway (permanent public URL)

### Option A: Deploy via GitHub (recommended)
1. Create a free account at https://github.com
2. Create a new repo, upload these files
3. Go to https://railway.app — sign up (free)
4. Click **New Project** → **Deploy from GitHub repo**
5. Select your repo
6. Go to **Variables** tab → add `PJM_API_KEY` = your key
7. Railway auto-deploys. You get a URL like `https://pjm-dashboard.up.railway.app`

### Option B: Deploy via Railway CLI
```bash
npm install -g @railway/cli
railway login
railway init
railway up
railway variables set PJM_API_KEY=your_key_here
```

---

## Data sources
| Data | Source | Cost |
|------|--------|------|
| Real-time LMPs | PJM Data Miner 2 API | Free (with key) |
| Prediction markets | Polymarket public API | Free |
| News | PJM + EIA RSS feeds | Free |

## Notes
- LMP data refreshes every 5 minutes in the browser
- Backend caches API responses for 5 minutes to stay within PJM rate limits
- Non-members are limited to 6 API calls/minute on PJM Data Miner
