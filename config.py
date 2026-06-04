"""
config.py — Central configuration loaded from .env
"""
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# ── AI Provider ─────────────────────────────────────────────
AI_PROVIDER      = os.getenv("AI_PROVIDER", "gemini").lower()
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY", "")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
OLLAMA_MODEL     = os.getenv("OLLAMA_MODEL", "llama3.3")
OLLAMA_BASE_URL  = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

# ── Twitter ──────────────────────────────────────────────────
TWITTER_API_KEY       = os.getenv("TWITTER_API_KEY", "")
TWITTER_API_SECRET    = os.getenv("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN  = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET", "")
TWITTER_BEARER_TOKEN  = os.getenv("TWITTER_BEARER_TOKEN", "")
TWITTER_MODE          = os.getenv("TWITTER_MODE", "manual")  # api | playwright | manual

# ── LinkedIn ─────────────────────────────────────────────────
LINKEDIN_ACCESS_TOKEN = os.getenv("LINKEDIN_ACCESS_TOKEN", "")
LINKEDIN_PERSON_URN   = os.getenv("LINKEDIN_PERSON_URN", "")
LINKEDIN_MODE         = os.getenv("LINKEDIN_MODE", "manual")  # api | manual

# ── Reddit ───────────────────────────────────────────────────
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "AINewsAgent/1.0")

# ── News API ─────────────────────────────────────────────────
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# ── Posting Schedule ────────────────────────────────────────
DAILY_TWITTER_LIMIT  = int(os.getenv("DAILY_TWITTER_LIMIT", "30"))
DAILY_LINKEDIN_LIMIT = int(os.getenv("DAILY_LINKEDIN_LIMIT", "30"))

TWITTER_WINDOW_START = os.getenv("TWITTER_POST_WINDOW_START", "09:00")
TWITTER_WINDOW_END   = os.getenv("TWITTER_POST_WINDOW_END", "15:58")
LINKEDIN_WINDOW_START = os.getenv("LINKEDIN_POST_WINDOW_START", "09:00")
LINKEDIN_WINDOW_END   = os.getenv("LINKEDIN_POST_WINDOW_END", "15:58")

RESEARCH_CYCLE_1     = os.getenv("RESEARCH_CYCLE_1", "08:00")
RESEARCH_CYCLE_2     = os.getenv("RESEARCH_CYCLE_2", "12:15")
ANALYTICS_PULL_TIME  = os.getenv("ANALYTICS_PULL_TIME", "16:00")

POST_INTERVAL_MIN    = int(os.getenv("POST_INTERVAL_MIN", "22"))
POST_INTERVAL_MAX    = int(os.getenv("POST_INTERVAL_MAX", "47"))

# ── System ───────────────────────────────────────────────────
DRY_RUN          = os.getenv("DRY_RUN", "false").lower() == "true"
DASHBOARD_PORT   = int(os.getenv("PORT", os.getenv("DASHBOARD_PORT", "3000")))
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")
DATABASE_URL     = f"sqlite+aiosqlite:///{BASE_DIR / 'data' / 'agent.db'}"
DATABASE_SYNC_URL = f"sqlite:///{BASE_DIR / 'data' / 'agent.db'}"

# ── Directories ──────────────────────────────────────────────
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

def is_twitter_configured() -> bool:
    return bool(TWITTER_API_KEY and TWITTER_API_SECRET and
                TWITTER_ACCESS_TOKEN and TWITTER_ACCESS_SECRET)

def is_linkedin_configured() -> bool:
    return bool(LINKEDIN_ACCESS_TOKEN and LINKEDIN_PERSON_URN)

def is_gemini_configured() -> bool:
    return bool(GEMINI_API_KEY)

def is_openai_configured() -> bool:
    return bool(OPENAI_API_KEY)

def is_reddit_configured() -> bool:
    return bool(REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET)
