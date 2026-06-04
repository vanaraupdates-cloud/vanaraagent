# 🤖 Autonomous AI News Media Agent

> Research → Write → Schedule → Publish → Analyze → Improve  
> **30 Twitter/X posts + 10 LinkedIn posts daily — fully automated before 4 PM**

---

## Quick Start

### 1. Set up API Keys

```
Copy .env.example to .env and fill in your credentials:
```

| Key | Where to get it | Required? |
|-----|----------------|-----------|
| `GEMINI_API_KEY` | [Google AI Studio](https://aistudio.google.com/app/apikey) | **Yes** (for content) |
| `TWITTER_API_KEY` etc. | [Twitter Developer Portal](https://developer.twitter.com) | Optional (manual mode works) |
| `REDDIT_CLIENT_ID` | [Reddit Apps](https://www.reddit.com/prefs/apps/) | Optional (enhances research) |
| `LINKEDIN_ACCESS_TOKEN` | [LinkedIn Developer](https://developer.linkedin.com) | Optional (manual export mode) |

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Test the System (No API Keys Needed)

```bash
python dry_run.py
```

### 4. Start the Dashboard

```bash
python main.py
# OR double-click START.bat
```

Open **http://localhost:3000** in your browser.

---

## How It Works

```
5:30 AM  → Research Cycle 1 (RSS feeds + Reddit)
7:15 AM  → Score all stories (virality, business value, relevance, trend)
7:45 AM  → AI generates all 40 posts (30 Twitter + 10 LinkedIn)
8:00 AM  → Posts queued with dynamic schedule
9:15 AM  → Publishing window opens
9:15 AM–3:55 PM → Posts at randomized 22–47 min intervals
12:15 PM → Research Cycle 2 (afternoon refresh)
4:00 PM  → Analytics pulled, learning loop runs
```

---

## Post Mix

### Twitter (30/day)
- 12 Short posts (Hook → Insight → Question)
- 6 Medium posts (Headline → Summary → Opinion)
- 6 Threads (5–8 tweets each)
- 6 Engagement posts (Polls, Predictions, Hot Takes, Questions)

### LinkedIn (10/day)
- 3 Industry Insights
- 2 AI Tool Analysis
- 2 Launch Breakdowns
- 2 Trend Reports
- 1 Founder Opinion

---

## Dashboard Tabs

| Tab | What it shows |
|-----|--------------|
| Overview | Live stats, connection status, quick actions |
| Twitter | 30 scheduled posts, edit/skip/publish now |
| LinkedIn | 10 posts, manual copy-paste export |
| Research | Scored news articles from RSS + Reddit |
| Analytics | Charts, engagement metrics, best performers |
| Logs | System activity feed |

---

## Anti-Spam Features

- ✅ Randomized posting intervals (22–47 min, never fixed)
- ✅ 3-pass duplicate detection (hash + TF-IDF + semantic)
- ✅ Never repeats hooks, CTAs, or opening lines same day
- ✅ 7-day content history for near-duplicate check
- ✅ Learning loop improves next day's content

---

## File Structure

```
├── main.py              # FastAPI app + dashboard server
├── config.py            # All settings from .env
├── database.py          # SQLAlchemy models
├── scheduler.py         # APScheduler (cron + dynamic posting)
├── dry_run.py           # Test without API calls
├── START.bat            # Windows double-click launcher
├── agents/
│   ├── research_agent.py   # RSS + Reddit + scoring
│   ├── content_agent.py    # AI post generation
│   ├── twitter_agent.py    # Tweepy v2 wrapper
│   └── linkedin_agent.py   # LinkedIn REST API
├── services/
│   ├── deduplication.py    # Hash + TF-IDF duplicate check
│   └── analytics.py        # Metrics + learning loop
├── templates/
│   └── index.html          # Dashboard UI
├── data/
│   ├── companies.json      # 150+ AI companies
│   ├── rss_feeds.json      # 30 RSS sources
│   └── content_library.json# Hooks, CTAs, templates
└── logs/
    └── agent.log           # Daily logs
```

---

## Twitter API Notes

- **Free tier**: Read-only. Use `TWITTER_MODE=manual` for export.
- **Basic tier ($200/mo)**: Full write access for 30 posts/day.
- The system randomizes intervals and varies content to minimize suspension risk.

## LinkedIn API Notes

- API requires app approval (can take weeks). 
- Use `LINKEDIN_MODE=manual` for copy-paste export mode (works without API).
- The dashboard has a one-click "Copy All Posts" button for manual posting.
