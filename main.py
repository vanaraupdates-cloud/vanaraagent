"""
main.py — FastAPI application entry point
Serves the dashboard and manages the scheduler lifecycle
"""
import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, AsyncGenerator

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel
from sqlalchemy import select, and_, func, desc, update
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

sys.path.insert(0, str(Path(__file__).parent))
from config import DASHBOARD_PORT, LOG_LEVEL, DRY_RUN, DATA_DIR
from database import (
    init_db, get_db, AsyncSessionLocal,
    Post, Article, Company, PostAnalytics, LearningPreference, SystemLog,
    log_to_db
)
from scheduler import start_scheduler, stop_scheduler, schedule_todays_posts, generate_dynamic_schedule
from services.analytics import get_dashboard_stats, get_analytics_chart_data

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/agent.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# SSE event queue for real-time dashboard updates
sse_clients: list[asyncio.Queue] = []


async def broadcast_event(event_type: str, data: dict):
    """Broadcast an event to all connected SSE clients."""
    msg = json.dumps({"type": event_type, "data": data, "ts": datetime.utcnow().isoformat()})
    dead = []
    for q in sse_clients:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        if q in sse_clients:
            sse_clients.remove(q)


# ── App Lifecycle ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    logger.info("🚀 AI News Media Agent starting up...")

    # Initialize database
    await init_db()

    # Seed companies if empty
    await seed_companies_if_needed()

    # Start scheduler
    await start_scheduler()

    logger.info(f"✅ Dashboard ready at http://localhost:{DASHBOARD_PORT}")
    if DRY_RUN:
        logger.info("⚠️  DRY RUN MODE — no posts will be published")

    yield

    logger.info("🛑 Shutting down...")
    await stop_scheduler()


async def seed_companies_if_needed():
    """Seed the companies table from data/companies.json if empty."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count(Company.id)))
        count = result.scalar()
        if count > 0:
            return

        companies_path = DATA_DIR / "companies.json"
        if not companies_path.exists():
            logger.warning("companies.json not found, skipping seed")
            return

        with open(companies_path, "r") as f:
            companies_data = json.load(f)

        for c in companies_data:
            company = Company(
                name=c.get("name", ""),
                domain=c.get("domain"),
                twitter_handle=c.get("twitter_handle"),
                linkedin_url=c.get("linkedin_url"),
                category=c.get("category", ""),
                tier=c.get("tier", 2),
                active=True
            )
            session.add(company)

        await session.commit()
        logger.info(f"✅ Seeded {len(companies_data)} companies to database")


# ── FastAPI App ──────────────────────────────────────────────

app = FastAPI(
    title="AI News Media Agent",
    description="Autonomous AI social media publishing system",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files & templates
STATIC_DIR = Path(__file__).parent / "static"
TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR.mkdir(exist_ok=True)
TEMPLATES_DIR.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ── Dashboard Route ──────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request, "index.html")


# ── SSE Stream ───────────────────────────────────────────────

@app.get("/api/stream")
async def sse_stream(request: Request):
    """Server-Sent Events for real-time dashboard updates."""
    queue = asyncio.Queue(maxsize=100)
    sse_clients.append(queue)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            # Send initial ping
            yield f"data: {json.dumps({'type': 'connected', 'ts': datetime.utcnow().isoformat()})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
        finally:
            if queue in sse_clients:
                sse_clients.remove(queue)

    return EventSourceResponse(event_generator())


# ── Stats API ─────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    """Dashboard overview stats."""
    try:
        stats = await get_dashboard_stats()
        return JSONResponse(stats)
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/analytics/chart")
async def get_chart_data(days: int = 7):
    """Time-series data for analytics charts."""
    data = await get_analytics_chart_data(days=days)
    return JSONResponse(data)


# ── Posts API ─────────────────────────────────────────────────

class PostUpdate(BaseModel):
    content: Optional[str] = None
    status: Optional[str] = None
    scheduled_at: Optional[str] = None


@app.get("/api/posts")
async def get_posts(
    platform: Optional[str] = None,
    status: Optional[str] = None,
    today_only: bool = True,
    limit: int = 50,
    offset: int = 0
):
    """Get scheduled/posted posts with optional filters."""
    from sqlalchemy.orm import selectinload
    async with AsyncSessionLocal() as session:
        q = select(Post).options(selectinload(Post.source_article))
        conditions = []

        if platform:
            conditions.append(Post.platform == platform)
        if status:
            conditions.append(Post.status == status)
        if today_only:
            today = date.today()
            today_start = datetime(today.year, today.month, today.day)
            today_end = today_start + timedelta(days=1)
            conditions.append(Post.created_at >= today_start)
            conditions.append(Post.created_at < today_end)

        if conditions:
            q = q.where(and_(*conditions))

        q = q.order_by(Post.scheduled_at.asc().nullslast(), Post.id.asc()).limit(limit).offset(offset)
        result = await session.execute(q)
        posts = result.scalars().all()

    return JSONResponse([{
        "id": p.id,
        "content": p.content,
        "platform": p.platform,
        "post_type": p.post_type,
        "status": p.status,
        "scheduled_at": p.scheduled_at.isoformat() if p.scheduled_at else None,
        "posted_at": p.posted_at.isoformat() if p.posted_at else None,
        "thread_id": p.thread_id,
        "thread_position": p.thread_position,
        "thread_total": p.thread_total,
        "platform_post_id": p.platform_post_id,
        "created_at": p.created_at.isoformat() if p.created_at else None,
        "error_message": p.error_message,
        "source_name": p.source_article.source if p.source_article else None,
        "source_score": p.source_article.total_score if p.source_article else None
    } for p in posts])


async def sync_post_edits(session: AsyncSession, post: Post, updated_content: str):
    """
    Synchronizes content changes across platforms:
    - If a Twitter post is edited, update its corresponding LinkedIn post.
    - If a LinkedIn post is edited, update its corresponding Twitter post(s).
    """
    from services.deduplication import get_content_hash
    import hashlib

    # 1. Twitter -> LinkedIn Sync
    if post.platform == "twitter":
        if post.post_type == "thread" and post.thread_id is not None:
            # First, fetch all tweets in this thread from the DB
            result = await session.execute(
                select(Post)
                .where(Post.thread_id == post.thread_id, Post.platform == "twitter")
                .order_by(Post.thread_position.asc())
            )
            thread_tweets = result.scalars().all()
            
            # Map tweet contents, replacing the edited one with the updated content
            combined_content = "\n\n".join([
                t.content if t.id != post.id else updated_content
                for t in thread_tweets
            ])
            
            # Find the corresponding LinkedIn combined thread post by scheduled_at and source_article_id
            conditions = [Post.platform == "linkedin", Post.post_type == "thread_combined"]
            if post.scheduled_at:
                conditions.append(Post.scheduled_at == post.scheduled_at)
            if post.source_article_id:
                conditions.append(Post.source_article_id == post.source_article_id)
            
            li_result = await session.execute(select(Post).where(and_(*conditions)))
            li_post = li_result.scalars().first()
            if li_post:
                li_post.content = combined_content
                li_post.content_hash = hashlib.sha256(f"linkedin:{combined_content}".encode('utf-8')).hexdigest()
                session.add(li_post)
        else:
            # Standalone Twitter post -> find matching LinkedIn post
            conditions = [Post.platform == "linkedin"]
            logger.info(f"[SYNC] Twitter edit: ID={post.id}, scheduled_at={post.scheduled_at}, source_article_id={post.source_article_id}")
            if post.scheduled_at:
                conditions.append(Post.scheduled_at == post.scheduled_at)
            if post.source_article_id:
                conditions.append(Post.source_article_id == post.source_article_id)
            
            li_result = await session.execute(select(Post).where(and_(*conditions)))
            li_post = li_result.scalars().first()
            if li_post:
                logger.info(f"[SYNC] Matched LinkedIn post ID={li_post.id} for sync.")
                li_post.content = updated_content
                li_post.content_hash = hashlib.sha256(f"linkedin:{updated_content}".encode('utf-8')).hexdigest()
                session.add(li_post)
            else:
                logger.warning(f"[SYNC] No matching LinkedIn post found for scheduled_at={post.scheduled_at}, source_article_id={post.source_article_id}")

    # 2. LinkedIn -> Twitter Sync
    elif post.platform == "linkedin":
        if post.post_type == "thread_combined":
            # Split LinkedIn combined post by double newline
            paragraphs = [p.strip() for p in updated_content.split("\n\n") if p.strip()]
            
            # Find the corresponding Twitter thread tweets
            conditions = [Post.platform == "twitter", Post.post_type == "thread"]
            if post.scheduled_at:
                conditions.append(Post.scheduled_at == post.scheduled_at)
            if post.source_article_id:
                conditions.append(Post.source_article_id == post.source_article_id)
                
            tw_result = await session.execute(
                select(Post).where(and_(*conditions)).order_by(Post.thread_position.asc())
            )
            thread_tweets = tw_result.scalars().all()
            
            # Update each tweet in the thread with its corresponding paragraph
            for i, tweet in enumerate(thread_tweets):
                if i < len(paragraphs):
                    tweet.content = paragraphs[i]
                    tweet.content_hash = get_content_hash(paragraphs[i])
                    session.add(tweet)
        else:
            # Standalone LinkedIn post -> find matching Twitter post
            conditions = [Post.platform == "twitter"]
            if post.scheduled_at:
                conditions.append(Post.scheduled_at == post.scheduled_at)
            if post.source_article_id:
                conditions.append(Post.source_article_id == post.source_article_id)
                
            tw_result = await session.execute(select(Post).where(and_(*conditions)))
            tw_post = tw_result.scalars().first()
            if tw_post:
                tw_post.content = updated_content
                tw_post.content_hash = get_content_hash(updated_content)
                session.add(tw_post)


@app.patch("/api/posts/{post_id}")
async def update_post(post_id: int, update_data: PostUpdate):
    """Edit a post's content, status, or scheduled time."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Post).where(Post.id == post_id))
        post = result.scalar_one_or_none()
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")

        if update_data.content is not None:
            # Sync edits to other platform first
            await sync_post_edits(session, post, update_data.content)
            
            # Update current post content and hash
            post.content = update_data.content
            from services.deduplication import get_content_hash
            post.content_hash = get_content_hash(update_data.content)
            
        if update_data.status is not None:
            post.status = update_data.status
        if update_data.scheduled_at is not None:
            post.scheduled_at = datetime.fromisoformat(update_data.scheduled_at)

        await session.commit()

    await broadcast_event("post_updated", {"post_id": post_id})
    return {"success": True, "post_id": post_id}


@app.delete("/api/posts/{post_id}")
async def delete_post(post_id: int):
    """Delete/skip a post."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Post).where(Post.id == post_id))
        post = result.scalar_one_or_none()
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")
        post.status = "skipped"
        await session.commit()
    return {"success": True}


@app.post("/api/posts/{post_id}/publish-now")
async def publish_now(post_id: int, background_tasks: BackgroundTasks):
    """Manually trigger immediate publishing of a post."""
    from scheduler import publish_twitter_post, publish_linkedin_post

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Post).where(Post.id == post_id))
        post = result.scalar_one_or_none()
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")
        platform = post.platform

    if platform == "twitter":
        background_tasks.add_task(publish_twitter_post, post_id)
    else:
        background_tasks.add_task(publish_linkedin_post, post_id)

    return {"success": True, "message": f"Publishing {platform} post #{post_id}"}


# ── Research API ──────────────────────────────────────────────

@app.get("/api/research")
async def get_research(today_only: bool = True, limit: int = 30):
    """Get research articles."""
    async with AsyncSessionLocal() as session:
        q = select(Article).order_by(Article.total_score.desc())
        if today_only:
            today = date.today()
            today_start = datetime(today.year, today.month, today.day)
            q = q.where(Article.fetched_at >= today_start)
        q = q.limit(limit)
        result = await session.execute(q)
        articles = result.scalars().all()

    return JSONResponse([{
        "id": a.id,
        "title": a.title,
        "url": a.url,
        "source": a.source,
        "summary": a.summary,
        "virality_score": a.virality_score,
        "business_score": a.business_score,
        "relevance_score": a.relevance_score,
        "trend_score": a.trend_score,
        "total_score": a.total_score,
        "cycle": a.cycle,
        "fetched_at": a.fetched_at.isoformat() if a.fetched_at else None,
        "used": a.used
    } for a in articles])


@app.post("/api/research/run")
async def trigger_research(background_tasks: BackgroundTasks, cycle: str = "morning"):
    """Manually trigger a research cycle."""
    from agents.research_agent import run_research_cycle
    background_tasks.add_task(run_research_cycle, cycle)
    await broadcast_event("research_started", {"cycle": cycle})
    return {"success": True, "message": f"Research cycle '{cycle}' started"}


# ── Content Generation API ───────────────────────────────────

@app.post("/api/generate")
async def trigger_generation(background_tasks: BackgroundTasks):
    """Manually trigger content generation."""
    from scheduler import run_content_generation
    background_tasks.add_task(run_content_generation)
    await broadcast_event("generation_started", {})
    return {"success": True, "message": "Content generation started in background"}


@app.post("/api/schedule/rebuild")
async def rebuild_schedule(background_tasks: BackgroundTasks):
    """Rebuild today's posting schedule (reschedule all pending posts)."""
    async with AsyncSessionLocal() as session:
        today = date.today()
        today_start = datetime(today.year, today.month, today.day)
        today_end = today_start + timedelta(days=1)
        # Clear scheduled_at for all today's pending posts to allow recalculation and registration
        await session.execute(
            update(Post)
            .where(
                and_(
                    Post.status == "pending",
                    Post.created_at >= today_start,
                    Post.created_at < today_end
                )
            )
            .values(scheduled_at=None)
        )
        await session.commit()

    background_tasks.add_task(schedule_todays_posts)
    return {"success": True, "message": "Schedule rebuild started"}


# ── LinkedIn Export API ───────────────────────────────────────

@app.get("/api/linkedin/export")
async def export_linkedin_posts():
    """Get LinkedIn posts formatted for manual copy-paste."""
    from agents.linkedin_agent import linkedin_agent
    posts = await linkedin_agent.get_export_queue()
    export_text = linkedin_agent.export_posts_to_text(posts)
    return JSONResponse({
        "posts": posts,
        "export_text": export_text,
        "count": len(posts)
    })


@app.post("/api/linkedin/mark-exported/{post_id}")
async def mark_linkedin_exported(post_id: int):
    """Mark a LinkedIn post as manually exported."""
    from agents.linkedin_agent import linkedin_agent
    await linkedin_agent.mark_exported(post_id)
    return {"success": True}


# ── System Logs API ───────────────────────────────────────────

@app.get("/api/logs")
async def get_logs(limit: int = 50):
    """Get recent system log entries."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(SystemLog)
            .order_by(SystemLog.created_at.desc())
            .limit(limit)
        )
        logs = result.scalars().all()

    return JSONResponse([{
        "id": l.id,
        "level": l.level,
        "module": l.module,
        "message": l.message,
        "created_at": l.created_at.isoformat() if l.created_at else None
    } for l in logs])


# ── Companies API ─────────────────────────────────────────────

@app.get("/api/companies")
async def get_companies(limit: int = 200):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Company).where(Company.active == True).order_by(Company.tier.asc(), Company.name.asc()).limit(limit)
        )
        companies = result.scalars().all()
    return JSONResponse([{
        "id": c.id, "name": c.name, "category": c.category,
        "tier": c.tier, "twitter_handle": c.twitter_handle
    } for c in companies])


# ── Config Status API ─────────────────────────────────────────

@app.get("/api/status")
async def system_status():
    """Check API connection status for all platforms."""
    from config import is_twitter_configured, is_linkedin_configured, is_gemini_configured, is_openai_configured, is_reddit_configured, AI_PROVIDER, TWITTER_MODE, LINKEDIN_MODE
    return JSONResponse({
        "dry_run": DRY_RUN,
        "ai_provider": AI_PROVIDER,
        "twitter": {
            "configured": is_twitter_configured(),
            "mode": TWITTER_MODE
        },
        "linkedin": {
            "configured": is_linkedin_configured(),
            "mode": LINKEDIN_MODE
        },
        "gemini": is_gemini_configured(),
        "openai": is_openai_configured(),
        "reddit": is_reddit_configured(),
        "uptime": datetime.utcnow().isoformat()
    })


# ── Entry Point ───────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=DASHBOARD_PORT,
        reload=False,
        log_level=LOG_LEVEL.lower()
    )
