"""
scheduler.py — APScheduler setup with dynamic posting intervals
"""
import asyncio
import logging
import random
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    RESEARCH_CYCLE_1, RESEARCH_CYCLE_2, ANALYTICS_PULL_TIME,
    TWITTER_WINDOW_START, TWITTER_WINDOW_END,
    LINKEDIN_WINDOW_START, LINKEDIN_WINDOW_END,
    DAILY_TWITTER_LIMIT, DAILY_LINKEDIN_LIMIT,
    POST_INTERVAL_MIN, POST_INTERVAL_MAX,
    DATABASE_SYNC_URL, DRY_RUN
)

logger = logging.getLogger(__name__)

# ── Global Scheduler Instance ────────────────────────────────
scheduler: Optional[AsyncIOScheduler] = None


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
        job_defaults=job_defaults
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
    today = date.today()
    schedule = []
    
    # Map count posts to custom static slot times
    for i in range(min(count, len(STATIC_SLOT_TIMES))):
        h, m = parse_time(STATIC_SLOT_TIMES[i])
        schedule.append(datetime(today.year, today.month, today.day, h, m))
        
    logger.info(f"Generated custom static schedule: {len(schedule)} posts using hardcoded slot times")
    return schedule


async def schedule_todays_posts():
    """
    Schedule today's pending posts at custom hardcoded intervals.
    Called after content generation completes (~7:45 AM).
    """
    from database import AsyncSessionLocal, Post
    from sqlalchemy import select, and_

    logger.info("📅 Scheduling today's posts...")

    # Clear any existing today's publish jobs from scheduler to prevent stale executions
    if scheduler:
        for job in list(scheduler.get_jobs()):
            if job.id.startswith("twitter_post_") or job.id.startswith("linkedin_post_"):
                try:
                    scheduler.remove_job(job.id)
                    logger.info(f"Removed stale job: {job.id}")
                except Exception as e:
                    logger.warning(f"Could not remove stale job {job.id}: {e}")

    today = date.today()
    today_start = datetime(today.year, today.month, today.day)
    today_end = today_start + timedelta(days=1)

    async with AsyncSessionLocal() as session:
        # Get pending Twitter posts
        twitter_result = await session.execute(
            select(Post).where(
                and_(
                    Post.platform == "twitter",
                    Post.status == "pending",
                    Post.created_at >= today_start,
                    Post.created_at < today_end,
                    Post.scheduled_at.is_(None)
                )
            ).order_by(Post.thread_id.asc().nullslast(), Post.thread_position.asc().nullslast(), Post.id.asc())
        )
        twitter_posts = twitter_result.scalars().all()

        # Get pending LinkedIn posts
        linkedin_result = await session.execute(
            select(Post).where(
                and_(
                    Post.platform == "linkedin",
                    Post.status == "pending",
                    Post.created_at >= today_start,
                    Post.created_at < today_end,
                    Post.scheduled_at.is_(None)
                )
            ).order_by(Post.id.asc())
        )
        linkedin_posts = linkedin_result.scalars().all()

    # Filter twitter posts to only schedule jobs for slots (thread root or standalone)
    twitter_slots = [p for p in twitter_posts if p.thread_position is None or p.thread_position == 1]

    # Generate schedules using slot times
    twitter_schedule = generate_dynamic_schedule(
        TWITTER_WINDOW_START, TWITTER_WINDOW_END,
        len(twitter_slots)
    )
    linkedin_schedule = generate_dynamic_schedule(
        LINKEDIN_WINDOW_START, LINKEDIN_WINDOW_END,
        len(linkedin_posts)
    )

    # Assign times and schedule jobs
    async with AsyncSessionLocal() as session:
        for i, post in enumerate(twitter_slots):
            if i < len(twitter_schedule):
                post_time = twitter_schedule[i]
                if post_time <= datetime.now():
                    post_time = post_time + timedelta(days=1)
                post.scheduled_at = post_time
                session.add(post)

                # Set sub-tweets of this thread to the exact same schedule time
                if post.post_type == "thread" and post.thread_id is not None:
                    for sub_post in twitter_posts:
                        if sub_post.thread_id == post.thread_id and sub_post.thread_position > 1:
                            sub_post.scheduled_at = post_time
                            session.add(sub_post)

                if not DRY_RUN and post_time > datetime.now():
                    job_id = f"twitter_post_{post.id}_{today}"
                    try:
                        scheduler.add_job(
                            publish_twitter_post,
                            trigger=DateTrigger(run_date=post_time),
                            args=[post.id],
                            id=job_id,
                            replace_existing=True
                        )
                    except Exception as e:
                        logger.warning(f"Could not schedule job {job_id}: {e}")

        for i, post in enumerate(linkedin_posts):
            if i < len(linkedin_schedule):
                post_time = linkedin_schedule[i]
                if post_time <= datetime.now():
                    post_time = post_time + timedelta(days=1)
                post.scheduled_at = post_time
                session.add(post)

                if not DRY_RUN and post_time > datetime.now():
                    job_id = f"linkedin_post_{post.id}_{today}"
                    try:
                        scheduler.add_job(
                            publish_linkedin_post,
                            trigger=DateTrigger(run_date=post_time),
                            args=[post.id],
                            id=job_id,
                            replace_existing=True
                        )
                    except Exception as e:
                        logger.warning(f"Could not schedule job {job_id}: {e}")

        await session.commit()

    logger.info(f"✅ Scheduled {len(twitter_posts)} Twitter + {len(linkedin_posts)} LinkedIn posts")


# ── Job Functions ────────────────────────────────────────────

async def run_morning_research():
    """5:30 AM — Morning research cycle."""
    logger.info("🌅 Morning research cycle starting...")
    from agents.research_agent import run_research_cycle
    try:
        await run_research_cycle(cycle="morning")
    except Exception as e:
        logger.error(f"Morning research failed: {e}")


async def run_afternoon_research():
    """12:15 PM — Afternoon refresh cycle."""
    logger.info("☀️ Afternoon research refresh starting...")
    from agents.research_agent import run_research_cycle
    try:
        await run_research_cycle(cycle="afternoon")
    except Exception as e:
        logger.error(f"Afternoon research failed: {e}")


async def run_content_generation():
    """7:45 AM — Generate all posts from research."""
    logger.info("🧠 Content generation starting...")
    from agents.content_agent import generate_all_posts, save_posts_to_db
    from database import AsyncSessionLocal, Article
    from sqlalchemy import select, and_
    from datetime import date

    today = date.today()
    today_start = datetime(today.year, today.month, today.day)
    today_end = today_start + timedelta(days=1)

    try:
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

        posts_data = await generate_all_posts(articles, cycle="morning")
        twitter_saved, linkedin_saved = await save_posts_to_db(posts_data)
        logger.info(f"✅ Saved {twitter_saved} Twitter + {linkedin_saved} LinkedIn posts to DB")

        # Schedule posts immediately after generation
        await schedule_todays_posts()

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


async def publish_twitter_post(post_id: int):
    """Publish a single Twitter post (called by scheduler)."""
    from agents.twitter_agent import twitter_agent
    from database import AsyncSessionLocal, Post
    from sqlalchemy import select

    logger.info(f"🐦 Publishing Twitter post #{post_id}")

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Post).where(Post.id == post_id))
        post = result.scalar_one_or_none()
        if not post or post.status == "posted":
            logger.warning(f"Post #{post_id} not found or already posted")
            return

    if post.post_type == "thread" and post.thread_position == 1:
        # Find all tweets in this thread
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Post).where(
                    Post.thread_id == post.thread_id,
                    Post.platform == "twitter"
                ).order_by(Post.thread_position.asc())
            )
            thread_posts = result.scalars().all()

        tweet_texts = [p.content for p in thread_posts]
        post_ids = [p.id for p in thread_posts]
        result = await twitter_agent.post_thread(tweet_texts, post_ids)
    else:
        result = await twitter_agent.post_tweet(post.content, post_id)

    if result.get("success"):
        logger.info(f"✅ Twitter post #{post_id} published: {result.get('tweet_id', 'N/A')}")
    else:
        logger.error(f"❌ Twitter post #{post_id} failed: {result.get('error')}")


async def publish_linkedin_post(post_id: int):
    """Publish a single LinkedIn post (called by scheduler)."""
    from agents.linkedin_agent import linkedin_agent
    from database import AsyncSessionLocal, Post
    from sqlalchemy import select

    logger.info(f"💼 Publishing LinkedIn post #{post_id}")

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Post).where(Post.id == post_id))
        post = result.scalar_one_or_none()
        if not post or post.status == "posted":
            return

    result = await linkedin_agent.post_text(post.content, post_id)
    if result.get("success"):
        logger.info(f"✅ LinkedIn post #{post_id} published")
    else:
        logger.error(f"❌ LinkedIn post #{post_id} failed: {result.get('error')}")


# ── Scheduler Lifecycle ──────────────────────────────────────

def setup_cron_jobs(sched: AsyncIOScheduler):
    """Register all recurring cron jobs."""
    r1_h, r1_m = parse_time(RESEARCH_CYCLE_1)
    r2_h, r2_m = parse_time(RESEARCH_CYCLE_2)
    an_h, an_m = parse_time(ANALYTICS_PULL_TIME)

    # Morning research: 8:00 AM
    sched.add_job(
        run_morning_research,
        CronTrigger(hour=r1_h, minute=r1_m),
        id="morning_research",
        replace_existing=True
    )
    # Score + generate: 8:15 AM
    sched.add_job(
        run_content_generation,
        CronTrigger(hour=8, minute=15),
        id="content_generation",
        replace_existing=True
    )
    # Prepare schedule: 8:45 AM
    sched.add_job(
        schedule_todays_posts,
        CronTrigger(hour=8, minute=45),
        id="prepare_schedule",
        replace_existing=True
    )
    # Afternoon refresh: 12:15 PM
    sched.add_job(
        run_afternoon_research,
        CronTrigger(hour=r2_h, minute=r2_m),
        id="afternoon_research",
        replace_existing=True
    )
    # Analytics pull: 4:00 PM
    sched.add_job(
        run_analytics_pull,
        CronTrigger(hour=an_h, minute=an_m),
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
        if next_run and next_run.date() == date.today():
            jobs.append({
                "id": job.id,
                "name": job.name or job.id,
                "next_run": next_run.isoformat(),
                "func": str(job.func)
            })
    return sorted(jobs, key=lambda x: x["next_run"])
