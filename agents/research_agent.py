"""
agents/research_agent.py — AI News Research Agent
===================================================
Fetches articles from RSS feeds and Reddit, scores them across four
dimensions (virality, business, relevance, trend), filters to only
keep content mentioning tracked AI companies, and persists the top 20
articles per cycle into the database.

Run directly for a quick test:
    python agents/research_agent.py
"""

import asyncio
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

# ── Path bootstrap (allows running as __main__ or as a module) ───────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import AsyncSessionLocal, Article, Company, log_to_db
from config import (
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
    is_reddit_configured,
    DATA_DIR,
)

# ── Logging setup ─────────────────────────────────────────────────────────────
logger = logging.getLogger("research_agent")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ── Constants ──────────────────────────────────────────────────────────────────
RSS_FEEDS_PATH = DATA_DIR / "rss_feeds.json"
COMPANIES_PATH = DATA_DIR / "companies.json"

# Business-signal keywords used for business_score calculation
BUSINESS_KEYWORDS = [
    "launch", "launches", "launched",
    "funding", "fundraising", "raised", "series a", "series b", "series c",
    "ipo", "acquisition", "acquires", "acquired", "merger",
    "partnership", "partners", "partnered",
    "revenue", "profit", "valuation",
    "enterprise", "enterprise-grade",
    "product", "release", "released", "announces", "announced",
    "breakthrough", "milestone", "expansion", "expands",
    "contract", "deal", "investment",
]

# How many top articles to persist per cycle
TOP_N_TO_SAVE = 20

# Maximum age of an article to be considered (hours)
MAX_ARTICLE_AGE_HOURS = 48

# Reddit posts to fetch per subreddit
REDDIT_POST_LIMIT = 25

# HTTP request timeout in seconds
HTTP_TIMEOUT = 15


# ── Data loaders ───────────────────────────────────────────────────────────────

def load_rss_config() -> dict:
    """Load RSS feeds config from data/rss_feeds.json."""
    try:
        with open(RSS_FEEDS_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        logger.info(
            "Loaded %d RSS feeds and %d subreddits from config.",
            len(data.get("rss_feeds", [])),
            len(data.get("reddit_subreddits", [])),
        )
        return data
    except FileNotFoundError:
        logger.error("rss_feeds.json not found at %s", RSS_FEEDS_PATH)
        return {"rss_feeds": [], "reddit_subreddits": []}
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse rss_feeds.json: %s", exc)
        return {"rss_feeds": [], "reddit_subreddits": []}


def load_companies() -> list[dict]:
    """Load tracked company list from data/companies.json."""
    try:
        with open(COMPANIES_PATH, "r", encoding="utf-8") as fh:
            companies = json.load(fh)
        logger.info("Loaded %d companies from companies.json.", len(companies))
        return companies
    except FileNotFoundError:
        logger.error("companies.json not found at %s", COMPANIES_PATH)
        return []
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse companies.json: %s", exc)
        return []


# ── Helper utilities ───────────────────────────────────────────────────────────

def get_company_mentions(text: str, companies: list[dict]) -> list[str]:
    """
    Return a list of company names that appear in the given text string.

    Matching is case-insensitive and uses word-boundary anchoring where
    possible so that e.g. 'Meta' does not match 'Metadata'.

    Args:
        text:      The text to scan (title + summary combined by the caller).
        companies: List of company dicts loaded from companies.json.

    Returns:
        Deduplicated list of matched company names.
    """
    if not text:
        return []

    text_lower = text.lower()
    found: list[str] = []

    for company in companies:
        name: str = company.get("name", "")
        if not name:
            continue

        # Build a simple search term — strip any parenthetical qualifiers
        # e.g. "Sora (OpenAI)" → also try "sora"
        search_name = re.sub(r"\s*\(.*?\)", "", name).strip().lower()

        # Use word-boundary regex for short names to avoid false positives,
        # plain substring for longer names (less ambiguity)
        if len(search_name) <= 4:
            pattern = r"\b" + re.escape(search_name) + r"\b"
            if re.search(pattern, text_lower):
                found.append(name)
        else:
            if search_name in text_lower:
                found.append(name)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for item in found:
        if item not in seen:
            seen.add(item)
            unique.append(item)

    return unique


def parse_published_date(entry) -> datetime | None:
    """
    Try to extract a timezone-aware datetime from a feedparser entry.
    Falls back to None if parsing fails.
    """
    # feedparser populates published_parsed as a time.struct_time in UTC
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    # Try updated_parsed as a fallback
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        try:
            dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    return None


def is_article_fresh(published: datetime | None, max_hours: int = MAX_ARTICLE_AGE_HOURS) -> bool:
    """Return True if the article is within the allowed age window."""
    if published is None:
        # If we have no date we allow it through (better than discarding)
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_hours)
    return published >= cutoff


def safe_get_text(entry, field: str, default: str = "") -> str:
    """Safely extract a string field from a feedparser entry."""
    value = getattr(entry, field, None)
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip()
    # Some feedparser fields are dicts with a 'value' key
    if isinstance(value, dict):
        return str(value.get("value", default)).strip()
    return str(value).strip()


def strip_html(html: str) -> str:
    """Strip HTML tags and return plain text using BeautifulSoup."""
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator=" ", strip=True)
    except Exception:
        # Fallback: crude regex strip
        return re.sub(r"<[^>]+>", " ", html).strip()


# ── Scoring engine ─────────────────────────────────────────────────────────────

def score_article(
    title: str,
    summary: str,
    url: str,
    upvotes: int = 0,
    published: datetime | None = None,
    companies: list[dict] | None = None,
) -> dict[str, float]:
    """
    Score an article across four dimensions (each 0–100).

    Args:
        title:     Article title.
        summary:   Article summary / description text.
        url:       Article URL (used for source-tier hints).
        upvotes:   Reddit upvote count (0 for RSS-only articles).
        published: Publication datetime (timezone-aware preferred).
        companies: Full company list for relevance scoring.

    Returns:
        Dict with keys: virality_score, business_score, relevance_score,
        trend_score, total_score.
    """
    if companies is None:
        companies = []

    combined_text = f"{title} {summary}".lower()

    # ── 1. Virality score (0-100) ─────────────────────────────────────────
    # Primarily driven by Reddit upvotes; capped at 100.
    if upvotes > 0:
        # Logarithmic scale: 1000 upvotes → ~75, 10000 → ~100
        import math
        raw = min(math.log10(upvotes + 1) / math.log10(10001) * 100, 100)
        virality_score = round(raw, 2)
    else:
        # RSS articles get a small base virality (they were curated/published)
        virality_score = 10.0

    # ── 2. Business score (0-100) ─────────────────────────────────────────
    # Count how many business-signal keywords appear in the combined text.
    keyword_hits = sum(1 for kw in BUSINESS_KEYWORDS if kw in combined_text)
    # Max expected hits before saturating at 100
    max_expected_hits = 6
    business_score = round(min(keyword_hits / max_expected_hits * 100, 100), 2)

    # ── 3. Relevance score (0-100) ────────────────────────────────────────
    # Based on how many tracked companies are mentioned.
    mentioned = get_company_mentions(combined_text, companies)
    mention_count = len(mentioned)
    # 1 mention → 40, 2 → 70, 3+ → 90-100
    if mention_count == 0:
        relevance_score = 0.0
    elif mention_count == 1:
        relevance_score = 40.0
    elif mention_count == 2:
        relevance_score = 70.0
    elif mention_count == 3:
        relevance_score = 85.0
    else:
        # 4+ mentions saturates at 100
        relevance_score = round(min(85 + (mention_count - 3) * 5, 100), 2)

    # ── 4. Trend score (0-100) ────────────────────────────────────────────
    # Higher for more recent articles.
    if published is None:
        trend_score = 50.0  # Unknown age → neutral
    else:
        now = datetime.now(timezone.utc)
        # Make published timezone-aware if it isn't
        if published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)
        age_hours = (now - published).total_seconds() / 3600
        if age_hours <= 1:
            trend_score = 100.0
        elif age_hours <= 6:
            trend_score = 90.0
        elif age_hours <= 12:
            trend_score = 75.0
        elif age_hours <= 24:
            trend_score = 55.0
        elif age_hours <= 48:
            trend_score = 30.0
        else:
            trend_score = 10.0

    # ── Total weighted score ──────────────────────────────────────────────
    total_score = round(
        virality_score * 0.30
        + business_score * 0.30
        + relevance_score * 0.20
        + trend_score * 0.20,
        4,
    )

    return {
        "virality_score": virality_score,
        "business_score": business_score,
        "relevance_score": relevance_score,
        "trend_score": trend_score,
        "total_score": total_score,
    }


# ── RSS fetching ───────────────────────────────────────────────────────────────

async def fetch_rss_articles(
    feeds: list[dict],
    companies: list[dict],
) -> list[dict]:
    """
    Fetch articles from all configured RSS feeds using feedparser.

    Runs feedparser synchronously (it is not async-native) but wraps each
    call in asyncio.to_thread() so the event loop stays unblocked.

    Args:
        feeds:     List of feed dicts from rss_feeds.json.
        companies: Tracked company list for mention filtering.

    Returns:
        List of article dicts ready for scoring and DB insertion.
    """
    articles: list[dict] = []

    for feed_cfg in feeds:
        feed_name: str = feed_cfg.get("name", "Unknown Feed")
        feed_url: str = feed_cfg.get("url", "")
        feed_company: str | None = feed_cfg.get("company")

        if not feed_url:
            logger.warning("Skipping feed '%s': no URL configured.", feed_name)
            continue

        try:
            logger.info("Fetching RSS feed: %s (%s)", feed_name, feed_url)
            # feedparser.parse() is blocking I/O — offload to thread
            parsed = await asyncio.to_thread(feedparser.parse, feed_url)

            if parsed.bozo and not parsed.entries:
                logger.warning(
                    "Feed '%s' returned a bozo error: %s",
                    feed_name,
                    getattr(parsed, "bozo_exception", "unknown"),
                )
                continue

            feed_articles_found = 0
            for entry in parsed.entries:
                title = safe_get_text(entry, "title")
                url = safe_get_text(entry, "link")
                if not title or not url:
                    continue  # Skip entries without title or URL

                # Extract summary — try multiple feedparser fields
                summary_raw = (
                    safe_get_text(entry, "summary")
                    or safe_get_text(entry, "description")
                    or safe_get_text(entry, "content")
                )
                summary = strip_html(summary_raw)[:1000]  # Cap at 1000 chars

                published = parse_published_date(entry)

                # Skip articles that are too old
                if not is_article_fresh(published):
                    continue

                articles.append(
                    {
                        "title": title,
                        "url": url,
                        "source": feed_name,
                        "feed_company": feed_company,
                        "summary": summary,
                        "published": published,
                        "upvotes": 0,       # RSS articles have no upvote count
                        "from_reddit": False,
                    }
                )
                feed_articles_found += 1

            logger.info(
                "Feed '%s': found %d fresh articles.", feed_name, feed_articles_found
            )

        except Exception as exc:
            # Never let one bad feed abort the whole research cycle
            logger.error(
                "Error fetching feed '%s': %s", feed_name, exc, exc_info=False
            )
            continue

    logger.info("RSS phase complete — %d raw articles collected.", len(articles))
    return articles


# ── Reddit fetching ────────────────────────────────────────────────────────────

async def fetch_reddit_posts(
    subreddits: list[dict],
    companies: list[dict],
) -> list[dict]:
    """
    Fetch top Reddit posts from configured subreddits using PRAW.

    PRAW's networking calls are synchronous, so each subreddit fetch is
    run in a thread via asyncio.to_thread().

    Args:
        subreddits: List of subreddit config dicts from rss_feeds.json.
        companies:  Tracked company list for mention context.

    Returns:
        List of article dicts (same schema as RSS articles).
    """
    if not is_reddit_configured():
        logger.info("Reddit not configured — skipping Reddit fetch.")
        return []

    try:
        import praw  # Lazy import: only needed when Reddit is configured
    except ImportError:
        logger.warning("praw package not installed — skipping Reddit fetch.")
        return []

    articles: list[dict] = []

    def _fetch_subreddit(subreddit_name: str) -> list[dict]:
        """Blocking function to fetch posts from a single subreddit."""
        results: list[dict] = []
        try:
            reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT,
            )
            sub = reddit.subreddit(subreddit_name)
            for post in sub.hot(limit=REDDIT_POST_LIMIT):
                # Skip pinned/stickied meta posts
                if post.stickied:
                    continue

                title = post.title or ""
                url = post.url or f"https://www.reddit.com{post.permalink}"
                upvotes = int(post.score) if post.score else 0
                # Use self-text as summary if available
                summary_raw = post.selftext or ""
                summary = summary_raw[:1000]

                # Reddit timestamps are Unix epoch (UTC)
                published: datetime | None = None
                if post.created_utc:
                    published = datetime.fromtimestamp(
                        post.created_utc, tz=timezone.utc
                    )

                if not is_article_fresh(published):
                    continue

                results.append(
                    {
                        "title": title,
                        "url": url,
                        "source": f"Reddit/r/{subreddit_name}",
                        "feed_company": None,
                        "summary": summary,
                        "published": published,
                        "upvotes": upvotes,
                        "from_reddit": True,
                    }
                )
        except Exception as exc:
            logger.error(
                "Error fetching r/%s: %s", subreddit_name, exc, exc_info=False
            )
        return results

    # Fetch each subreddit concurrently via threads
    tasks = [
        asyncio.to_thread(_fetch_subreddit, sub_cfg.get("name", ""))
        for sub_cfg in subreddits
        if sub_cfg.get("name")
    ]

    if not tasks:
        return []

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logger.error("Reddit fetch task raised: %s", result)
            continue
        if isinstance(result, list):
            articles.extend(result)

    logger.info("Reddit phase complete — %d raw posts collected.", len(articles))
    return articles


# ── Company filtering ──────────────────────────────────────────────────────────

def filter_by_company_mention(
    articles: list[dict],
    companies: list[dict],
) -> list[dict]:
    """
    Remove articles that mention none of the tracked companies.

    Also injects a 'mentioned_companies' key into each passing article
    for use by the scoring and DB-save steps.

    Args:
        articles:  Raw article dicts.
        companies: Tracked company list.

    Returns:
        Filtered list with only company-relevant articles.
    """
    filtered: list[dict] = []
    for art in articles:
        combined = f"{art.get('title', '')} {art.get('summary', '')}"
        mentioned = get_company_mentions(combined, companies)
        if mentioned:
            art["mentioned_companies"] = mentioned
            filtered.append(art)
        elif art.get("feed_company"):
            # If the feed itself is company-specific, keep it even without
            # a match in body text (e.g. OpenAI blog about an internal update)
            art["mentioned_companies"] = [art["feed_company"]]
            filtered.append(art)

    logger.info(
        "Company filter: %d → %d articles (%.0f%% kept).",
        len(articles),
        len(filtered),
        (len(filtered) / max(len(articles), 1)) * 100,
    )
    return filtered


# ── Database persistence ───────────────────────────────────────────────────────

async def save_articles_to_db(
    articles: list[dict],
    cycle: str,
) -> int:
    """
    Persist the given articles into the database, skipping any duplicates
    (detected by unique URL constraint).

    Args:
        articles: Scored article dicts ready for insertion.
        cycle:    Cycle label: 'morning' or 'afternoon'.

    Returns:
        Count of newly inserted articles.
    """
    saved = 0
    skipped = 0

    async with AsyncSessionLocal() as session:
        for art in articles:
            url = art.get("url", "").strip()
            if not url:
                continue

            # Check for existing article with same URL to avoid a DB round-trip
            # on every insert (IntegrityError is still the safety net)
            try:
                existing = await session.execute(
                    select(Article).where(Article.url == url)
                )
                if existing.scalar_one_or_none() is not None:
                    skipped += 1
                    continue
            except Exception as exc:
                logger.warning("Pre-check query failed for URL %s: %s", url, exc)

            # Build the Article ORM object
            article_obj = Article(
                title=art.get("title", "")[:500],        # guard against very long titles
                url=url[:1000],
                source=art.get("source", "")[:200],
                summary=art.get("summary", ""),
                virality_score=art.get("virality_score", 0.0),
                business_score=art.get("business_score", 0.0),
                relevance_score=art.get("relevance_score", 0.0),
                trend_score=art.get("trend_score", 0.0),
                total_score=art.get("total_score", 0.0),
                fetched_at=datetime.utcnow(),
                cycle=cycle,
                used=False,
            )

            try:
                session.add(article_obj)
                await session.flush()  # Flush to catch IntegrityError early
                saved += 1
                logger.debug("Saved article: %s", art.get("title", "")[:80])
            except IntegrityError:
                # URL already exists — another process may have inserted it
                await session.rollback()
                skipped += 1
                logger.debug("Duplicate skipped: %s", url)
                # Re-open session context (rollback above closed the transaction)
                continue
            except Exception as exc:
                await session.rollback()
                logger.error(
                    "Failed to save article '%s': %s",
                    art.get("title", "")[:60],
                    exc,
                    exc_info=False,
                )
                continue

        try:
            await session.commit()
        except Exception as exc:
            logger.error("Final commit failed: %s", exc)
            await session.rollback()

    logger.info(
        "DB save complete: %d new articles inserted, %d duplicates skipped.",
        saved,
        skipped,
    )
    return saved


# ── Main research cycle ────────────────────────────────────────────────────────

async def run_research_cycle(cycle: str = "morning") -> dict:
    """
    Execute a full research cycle: fetch → filter → score → save.

    Args:
        cycle: Label for this run — 'morning' or 'afternoon'.
               Controls which cycle tag is written to the DB records.

    Returns:
        Summary dict with counts: total_fetched, after_filter, saved.
    """
    cycle = cycle.lower().strip()
    if cycle not in ("morning", "afternoon"):
        logger.warning("Unknown cycle '%s', defaulting to 'morning'.", cycle)
        cycle = "morning"

    logger.info("=" * 60)
    logger.info("Starting %s research cycle at %s", cycle.upper(), datetime.now().isoformat())
    logger.info("=" * 60)

    await log_to_db("research_agent", "INFO", f"{cycle.capitalize()} research cycle started.")

    # ── Load config & company list ────────────────────────────────────────
    rss_config = load_rss_config()
    rss_feeds: list[dict] = rss_config.get("rss_feeds", [])
    subreddits: list[dict] = rss_config.get("reddit_subreddits", [])
    companies: list[dict] = load_companies()

    if not companies:
        logger.error("No companies loaded — cannot filter articles. Aborting cycle.")
        await log_to_db(
            "research_agent", "ERROR",
            "Research cycle aborted: companies.json is empty or missing."
        )
        return {"total_fetched": 0, "after_filter": 0, "saved": 0}

    # ── Phase 1: Fetch from all sources concurrently ──────────────────────
    logger.info("Phase 1: Fetching from RSS and Reddit concurrently…")
    rss_task = fetch_rss_articles(rss_feeds, companies)
    reddit_task = fetch_reddit_posts(subreddits, companies)

    rss_articles, reddit_articles = await asyncio.gather(
        rss_task, reddit_task, return_exceptions=True
    )

    # Handle top-level failures gracefully
    if isinstance(rss_articles, Exception):
        logger.error("RSS fetch raised an exception: %s", rss_articles)
        rss_articles = []
    if isinstance(reddit_articles, Exception):
        logger.error("Reddit fetch raised an exception: %s", reddit_articles)
        reddit_articles = []

    all_articles: list[dict] = rss_articles + reddit_articles  # type: ignore[operator]
    logger.info(
        "Phase 1 done: %d RSS + %d Reddit = %d total raw articles.",
        len(rss_articles),
        len(reddit_articles),
        len(all_articles),
    )

    if not all_articles:
        logger.warning("No articles collected — nothing to process.")
        await log_to_db(
            "research_agent", "WARNING",
            f"{cycle.capitalize()} cycle: zero articles collected from all sources."
        )
        return {"total_fetched": 0, "after_filter": 0, "saved": 0}

    # ── Phase 2: Filter to company-relevant articles ───────────────────────
    logger.info("Phase 2: Filtering by company mentions…")
    filtered_articles = filter_by_company_mention(all_articles, companies)

    if not filtered_articles:
        logger.warning(
            "No articles survived company filter — broadening acceptance threshold."
        )
        # Fallback: take all articles anyway (avoids completely empty cycles)
        filtered_articles = all_articles
        for art in filtered_articles:
            art.setdefault("mentioned_companies", [])

    # ── Phase 3: Score every article ──────────────────────────────────────
    logger.info("Phase 3: Scoring %d articles…", len(filtered_articles))
    for art in filtered_articles:
        scores = score_article(
            title=art.get("title", ""),
            summary=art.get("summary", ""),
            url=art.get("url", ""),
            upvotes=art.get("upvotes", 0),
            published=art.get("published"),
            companies=companies,
        )
        art.update(scores)

    # ── Phase 4: Sort and take top N ──────────────────────────────────────
    logger.info("Phase 4: Ranking and selecting top %d articles…", TOP_N_TO_SAVE)
    filtered_articles.sort(key=lambda a: a.get("total_score", 0.0), reverse=True)
    top_articles = filtered_articles[:TOP_N_TO_SAVE]

    # Log the top 5 for visibility
    logger.info("── Top 5 articles by score ──")
    for i, art in enumerate(top_articles[:5], 1):
        logger.info(
            "  #%d [%.2f] %s — %s",
            i,
            art.get("total_score", 0.0),
            art.get("source", "?"),
            art.get("title", "")[:80],
        )

    # ── Phase 5: Persist to database ──────────────────────────────────────
    logger.info("Phase 5: Saving top %d articles to database…", len(top_articles))
    saved_count = await save_articles_to_db(top_articles, cycle)

    # ── Completion summary ─────────────────────────────────────────────────
    summary = {
        "total_fetched": len(all_articles),
        "after_filter": len(filtered_articles),
        "top_selected": len(top_articles),
        "saved": saved_count,
        "cycle": cycle,
        "completed_at": datetime.now().isoformat(),
    }

    logger.info(
        "Research cycle complete: %d fetched → %d filtered → %d selected → %d saved.",
        summary["total_fetched"],
        summary["after_filter"],
        summary["top_selected"],
        summary["saved"],
    )

    await log_to_db(
        "research_agent",
        "INFO",
        (
            f"{cycle.capitalize()} cycle complete: "
            f"{summary['total_fetched']} fetched, "
            f"{summary['after_filter']} filtered, "
            f"{summary['saved']} saved to DB."
        ),
    )

    return summary


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Quick test runner.
    Usage:
        python agents/research_agent.py               # morning cycle (default)
        python agents/research_agent.py afternoon     # afternoon cycle
    """
    import sys

    cycle_arg = sys.argv[1] if len(sys.argv) > 1 else "morning"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    result = asyncio.run(run_research_cycle(cycle=cycle_arg))
    print("\n── Research Cycle Summary ──")
    for key, value in result.items():
        print(f"  {key:20s}: {value}")
