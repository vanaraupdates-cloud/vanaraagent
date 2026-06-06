"""
scheduler.py — APScheduler setup with dynamic posting intervals
"""
import asyncio
import logging
import random
import sys
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    RESEARCH_CYCLE_1, RESEARCH_CYCLE_2, ANALYTICS_PULL_TIME,
    LINKEDIN_WINDOW_START, LINKEDIN_WINDOW_END,
    DAILY_LINKEDIN_LIMIT,
    POST_INTERVAL_MIN, POST_INTERVAL_MAX,
    DATABASE_SYNC_URL, DRY_RUN
)

logger = logging.getLogger(__name__)

# ── Global Scheduler Instance ────────────────────────────────
scheduler: Optional[AsyncIOScheduler] = None
scheduling_lock = asyncio.Lock()


def create_scheduler() -> AsyncIOScheduler:
    """Create APScheduler with SQLite jobstore for persistence."""
    jobstores = {
        "default": SQLAlchemyJobStore(url=DATABASE_SYNC_URL, tablename="apscheduler_jobs")
    }
    job_defaults = {
        "coalesce": False,
        "max_instances": 1,
        "misfire_grace_time": 300  # 5 minute grace window
    }
    return AsyncIOScheduler(
        jobstores=jobstores,
        job_defaults=job_defaults,
        timezone=ZoneInfo("Asia/Kolkata")
    )


def parse_time(time_str: str) -> tuple[int, int]:
    """Parse 'HH:MM' string into (hour, minute) tuple."""
    h, m = time_str.split(":")
    return int(h), int(m)


STATIC_SLOT_TIMES = [
    "09:00", "09:14", "09:28", "09:43", "09:58",
    "10:12", "10:26", "10:41", "10:56",
    "11:09", "11:22", "11:36", "11:51",
    "12:07", "12:23", "12:40", "12:58",
    "13:14", "13:29", "13:45",
    "14:02", "14:18", "14:35", "14:49",
    "15:03", "15:16", "15:29", "15:41", "15:51", "15:58"
]


def generate_dynamic_schedule(
    window_start: str,
    window_end: str,
    count: int,
    min_interval: int = None,
    max_interval: int = None
) -> list[datetime]:
    """
    Generate custom slot times from the hardcoded static schedule.
    """
    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    schedule = []
    
    # Map count posts to custom static slot times
    for i in range(min(count, len(STATIC_SLOT_TIMES))):
        h, m = parse_time(STATIC_SLOT_TIMES[i])
        schedule.append(datetime(today.year, today.month, today.day, h, m))
        
    logger.info(f"Generated custom static schedule: {len(schedule)} posts using hardcoded slot times")
    return schedule


def get_next_static_slots(start_dt: datetime, count: int, reserved_times: set[datetime]) -> list[datetime]:
    """
    Get the next `count` static slot datetimes starting at or after `start_dt`.
    Skips any datetimes that are present in the `reserved_times` set.
    """
    slots = []
    current_dt = start_dt
    
    while len(slots) < count:
        day_slots = []
        for time_str in STATIC_SLOT_TIMES:
            h, m = parse_time(time_str)
            dt = datetime(current_dt.year, current_dt.month, current_dt.day, h, m)
            if dt >= start_dt and dt not in reserved_times:
                day_slots.append(dt)
        
        day_slots.sort()
        for dt in day_slots:
            if len(slots) < count:
                slots.append(dt)
            else:
                break
                
        current_dt = current_dt + timedelta(days=1)
        start_dt = datetime(current_dt.year, current_dt.month, current_dt.day, 0, 0)
        
    return slots


async def schedule_todays_posts():
    """
    Schedule pending LinkedIn posts at custom hardcoded intervals.
    Automatically assigns new times to unassigned or missed posts without clashing.
    """
    from database import AsyncSessionLocal, Post
    from sqlalchemy import select, and_, or_

    async with scheduling_lock:
        logger.info("📅 Scheduling pending posts...")

        # Clear existing publish jobs from scheduler to prevent duplicate executions
        if scheduler:
            for job in list(scheduler.get_jobs()):
                if job.id.startswith("linkedin_post_"):
                    try:
                        scheduler.remove_job(job.id)
                        logger.info(f"Removed stale job: {job.id}")
                    except Exception as e:
                        logger.warning(f"Could not remove stale job {job.id}: {e}")

        now = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
        today = now.date()
        today_start = datetime(today.year, today.month, today.day)
        today_end = today_start + timedelta(days=1)

        async with AsyncSessionLocal() as session:
            # Get all pending LinkedIn posts
            result = await session.execute(
                select(Post).where(
                    and_(
                        Post.platform == "linkedin",
                        Post.status == "pending"
                    )
                ).order_by(Post.id.asc())
            )
            all_pending = result.scalars().all()

        # Determine reserved times: include any posts already scheduled/posted today OR pending future posts
        async with AsyncSessionLocal() as session:
            result_reserved = await session.execute(
                select(Post).where(
                    and_(
                        Post.platform == "linkedin",
                        or_(
                            and_(Post.scheduled_at >= today_start, Post.scheduled_at < today_end),
                            and_(Post.status == "pending", Post.scheduled_at > now)
                        )
                    )
                )
            )
            reserved_posts = result_reserved.scalars().all()

        reserved_times = {p.scheduled_at for p in reserved_posts if p.scheduled_at}
        unassigned = [p for p in all_pending if p.scheduled_at is None or p.scheduled_at <= now]

        if unassigned:
            logger.info(f"Scheduling {len(unassigned)} unassigned or missed posts...")
            next_slots = get_next_static_slots(now, len(unassigned), reserved_times)
            
            async with AsyncSessionLocal() as session:
                for idx, post in enumerate(unassigned):
                    if idx < len(next_slots):
                        slot_time = next_slots[idx]
                        db_post = await session.get(Post, post.id)
                        db_post.scheduled_at = slot_time
                        session.add(db_post)
                        logger.info(f"Assigned Post ID {post.id} to slot: {slot_time}")
                await session.commit()

        # Refresh all pending posts from DB to register scheduler jobs
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Post).where(
                    and_(
                        Post.platform == "linkedin",
                        Post.status == "pending"
                    )
                ).order_by(Post.scheduled_at.asc())
            )
            all_pending = result.scalars().all()

        # Schedule active jobs in APScheduler
        if scheduler:
            for post in all_pending:
                post_time = post.scheduled_at
                if post_time and not DRY_RUN and post_time > now:
                    job_id = f"linkedin_post_{post.id}_{post_time.date()}"
                    try:
                        scheduler.add_job(
                            publish_linkedin_post,
                            trigger=DateTrigger(run_date=post_time, timezone=ZoneInfo("Asia/Kolkata")),
                            args=[post.id],
                            id=job_id,
                            replace_existing=True
                        )
                        logger.info(f"Registered job {job_id} for: {post_time}")
                    except Exception as e:
                        logger.warning(f"Could not schedule job {job_id}: {e}")
        else:
            logger.warning("Scheduler is not active. Posts have been saved to the DB but not scheduled in memory.")

        logger.info(f"✅ Scheduled {len(all_pending)} LinkedIn posts")
        try:
            from main import broadcast_event
            await broadcast_event("schedule_rebuilt", {})
        except Exception as e:
            logger.warning(f"Failed to broadcast schedule_rebuilt: {e}")


# ── Job Functions ────────────────────────────────────────────

async def run_morning_research():
    """5:30 AM — Morning research cycle."""
    logger.info("🌅 Morning research cycle starting...")
    from agents.research_agent import run_research_cycle
    from main import broadcast_event
    try:
        await broadcast_event("research_started", {"cycle": "morning"})
        await run_research_cycle(cycle="morning")
        await broadcast_event("research_completed", {"cycle": "morning"})
    except Exception as e:
        logger.error(f"Morning research failed: {e}")


async def run_afternoon_research():
    """12:15 PM — Afternoon refresh cycle."""
    logger.info("☀️ Afternoon research refresh starting...")
    from agents.research_agent import run_research_cycle
    from main import broadcast_event
    try:
        await broadcast_event("research_started", {"cycle": "afternoon"})
        await run_research_cycle(cycle="afternoon")
        await broadcast_event("research_completed", {"cycle": "afternoon"})
    except Exception as e:
        logger.error(f"Afternoon research failed: {e}")


async def run_content_generation():
    """7:45 AM — Generate all posts from research."""
    logger.info("🧠 Content generation starting...")
    from agents.content_agent import generate_all_posts, save_posts_to_db
    from database import AsyncSessionLocal, Article, Post
    from sqlalchemy import select, and_, or_, func
    from datetime import timezone as dt_timezone, timedelta as dt_timedelta
    from main import broadcast_event

    ist_tz = dt_timezone(dt_timedelta(hours=5, minutes=30))
    ist_now = datetime.now(ist_tz)
    today = ist_now.date()
    today_start = datetime(today.year, today.month, today.day)
    today_end = today_start + dt_timedelta(days=1)

    try:
        await broadcast_event("generation_started", {})

        # Count already posted/exported posts today
        async with AsyncSessionLocal() as session:
            result_posted = await session.execute(
                select(func.count(Post.id)).where(
                    Post.platform == "linkedin",
                    Post.status.in_(["posted", "exported", "live"]),
                    or_(
                        and_(Post.scheduled_at >= today_start, Post.scheduled_at < today_end),
                        and_(Post.scheduled_at.is_(None), Post.created_at >= today_start, Post.created_at < today_end)
                    )
                )
            )
            already_posted = result_posted.scalar() or 0

        actual_target = max(0, DAILY_LINKEDIN_LIMIT - already_posted)
        if actual_target == 0:
            logger.info("Daily LinkedIn posting limit of 30 posts has already been met. Skipping content generation.")
            await schedule_todays_posts()
            await broadcast_event("generation_completed", {"saved_count": 0})
            return

        logger.info(f"Target count to generate: {actual_target} posts (already posted {already_posted} today)")

        # Load today's morning research
        async with AsyncSessionLocal() as session:
            from sqlalchemy.orm import selectinload
            result = await session.execute(
                select(Article).options(selectinload(Article.company)).where(
                    and_(
                        Article.fetched_at >= today_start,
                        Article.fetched_at < today_end,
                        Article.cycle == "morning"
                    )
                ).order_by(Article.total_score.desc()).limit(20)
            )
            articles_db = result.scalars().all()

            articles = [
                {
                    "id": a.id,
                    "title": a.title,
                    "summary": a.summary or "",
                    "company": a.company.name if a.company else "AI",
                    "url": a.url,
                    "total_score": a.total_score
                }
                for a in articles_db
            ]

        posts_data = await generate_all_posts(articles, cycle="morning", target_count=actual_target)
        linkedin_saved = await save_posts_to_db(posts_data)
        logger.info(f"✅ Saved {linkedin_saved} LinkedIn posts to DB")

        # Schedule posts immediately after generation
        await schedule_todays_posts()
        await broadcast_event("generation_completed", {"saved_count": linkedin_saved})

    except Exception as e:
        logger.error(f"Content generation failed: {e}")
        import traceback
        traceback.print_exc()


async def run_analytics_pull():
    """4:00 PM — Pull engagement metrics and run learning loop."""
    logger.info("📊 Analytics pull starting...")
    try:
        from services.analytics import pull_all_analytics, run_learning_loop
        await pull_all_analytics()
        await run_learning_loop()
    except Exception as e:
        logger.error(f"Analytics pull failed: {e}")




async def publish_linkedin_post(post_id: int):
    """Publish a single LinkedIn post (called by scheduler)."""
    from agents.linkedin_agent import linkedin_agent
    from database import AsyncSessionLocal, Post
    from sqlalchemy import select
    from main import broadcast_event

    logger.info(f"💼 Publishing LinkedIn post #{post_id}")

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Post).where(Post.id == post_id))
        post = result.scalar_one_or_none()
        if not post or post.status in ["posted", "live", "exported", "publishing"]:
            logger.info(f"Skipping post #{post_id} because status is {post.status if post else 'None'}")
            return
            
        # Concurrency lock
        post.status = "publishing"
        await session.commit()

    res = await linkedin_agent.post_text(post.content, post_id)
    if res.get("success"):
        platform_post_id = res.get("post_id")
        
        # Verify post is live
        is_live = await linkedin_agent.verify_post_live(platform_post_id)
        if is_live:
            await linkedin_agent._update_post_status(post_id, "live", platform_post_id)
            logger.info(f"✅ LinkedIn post #{post_id} verified live on platform")
            await broadcast_event("post_published", {"post_id": post_id, "platform": "linkedin", "status": "live"})
        else:
            logger.warning(f"⚠️ LinkedIn post #{post_id} published but verification returned false")
            await broadcast_event("post_published", {"post_id": post_id, "platform": "linkedin", "status": "posted"})
    else:
        logger.error(f"❌ LinkedIn post #{post_id} failed: {res.get('error')}")
        await broadcast_event("post_failed", {"post_id": post_id, "platform": "linkedin", "error": res.get("error")})


# ── Scheduler Lifecycle ──────────────────────────────────────

def setup_cron_jobs(sched: AsyncIOScheduler):
    """Register all recurring cron jobs."""
    r1_h, r1_m = parse_time(RESEARCH_CYCLE_1)
    r2_h, r2_m = parse_time(RESEARCH_CYCLE_2)
    an_h, an_m = parse_time(ANALYTICS_PULL_TIME)

    # Morning research: 8:00 AM
    sched.add_job(
        run_morning_research,
        CronTrigger(hour=r1_h, minute=r1_m, timezone=ZoneInfo("Asia/Kolkata")),
        id="morning_research",
        replace_existing=True
    )
    # Score + generate: 8:15 AM
    sched.add_job(
        run_content_generation,
        CronTrigger(hour=8, minute=15, timezone=ZoneInfo("Asia/Kolkata")),
        id="content_generation",
        replace_existing=True
    )
    # Prepare schedule: 8:45 AM
    sched.add_job(
        schedule_todays_posts,
        CronTrigger(hour=8, minute=45, timezone=ZoneInfo("Asia/Kolkata")),
        id="prepare_schedule",
        replace_existing=True
    )
    # Afternoon refresh: 12:15 PM
    sched.add_job(
        run_afternoon_research,
        CronTrigger(hour=r2_h, minute=r2_m, timezone=ZoneInfo("Asia/Kolkata")),
        id="afternoon_research",
        replace_existing=True
    )
    # Analytics pull: 4:00 PM
    sched.add_job(
        run_analytics_pull,
        CronTrigger(hour=an_h, minute=an_m, timezone=ZoneInfo("Asia/Kolkata")),
        id="analytics_pull",
        replace_existing=True
    )

    logger.info("✅ Cron jobs registered: research(8:00), generate(8:15), prepare(8:45), refresh(12:15), analytics(16:00)")


async def start_scheduler() -> AsyncIOScheduler:
    """Initialize and start the global scheduler."""
    global scheduler
    scheduler = create_scheduler()
    setup_cron_jobs(scheduler)
    scheduler.start()
    logger.info("🕒 Scheduler started with SQLite-backed jobstore")
    return scheduler


async def stop_scheduler():
    """Gracefully stop the scheduler."""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("🛑 Scheduler stopped")


def get_scheduler() -> Optional[AsyncIOScheduler]:
    """Get the global scheduler instance."""
    return scheduler


def get_todays_schedule() -> list[dict]:
    """Return all scheduled jobs for today as a list of dicts."""
    if not scheduler:
        return []
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        if next_run and next_run.date() == datetime.now(ZoneInfo("Asia/Kolkata")).date():
            jobs.append({
                "id": job.id,
                "name": job.name or job.id,
                "next_run": next_run.isoformat(),
                "func": str(job.func)
            })
    return sorted(jobs, key=lambda x: x["next_run"])
