"""
services/analytics.py — Metrics fetching + Learning Loop
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta, date
from pathlib import Path

from sqlalchemy import select, and_, func, desc

sys.path.insert(0, str(Path(__file__).parent.parent))
from database import AsyncSessionLocal, Post, PostAnalytics, LearningPreference, log_to_db
from config import DRY_RUN

logger = logging.getLogger(__name__)


async def pull_all_analytics():
    """
    Pull engagement metrics for all posted content from today.
    Called at 4:00 PM daily.
    """
    logger.info("📊 Pulling analytics for today's posts...")

    today = date.today()
    today_start = datetime(today.year, today.month, today.day)
    today_end = today_start + timedelta(days=1)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Post).where(
                and_(
                    Post.status == "posted",
                    Post.posted_at >= today_start,
                    Post.posted_at < today_end,
                    Post.platform_post_id.isnot(None)
                )
            )
        )
        posts = result.scalars().all()

    if not posts:
        logger.info("No posted content to pull analytics for")
        return

    logger.info(f"Fetching analytics for {len(posts)} posts...")

    # Separate by platform
    linkedin_posts = [p for p in posts if p.platform == "linkedin"]

    # LinkedIn analytics
    if linkedin_posts:
        await _pull_linkedin_analytics(linkedin_posts)

    logger.info("✅ Analytics pull complete")
    await log_to_db("analytics", "SUCCESS", f"Analytics pulled for {len(posts)} posts")




async def _pull_linkedin_analytics(posts: list):
    """Fetch LinkedIn metrics for a batch of posts."""
    try:
        from agents.linkedin_agent import linkedin_agent
        if not linkedin_agent.is_configured():
            return

        async with AsyncSessionLocal() as session:
            for post in posts:
                if not post.platform_post_id:
                    continue
                try:
                    metrics = await linkedin_agent.get_post_metrics(post.platform_post_id)
                    if not metrics or "error" in metrics:
                        continue

                    result = await session.execute(
                        select(PostAnalytics).where(PostAnalytics.post_id == post.id)
                    )
                    analytics = result.scalar_one_or_none()
                    if not analytics:
                        analytics = PostAnalytics(post_id=post.id)
                        session.add(analytics)

                    analytics.reach      = metrics.get("reach", 0)
                    analytics.impressions= metrics.get("impressions", 0)
                    analytics.link_clicks= metrics.get("clicks", 0)
                    analytics.likes      = metrics.get("likes", 0)
                    analytics.comments   = metrics.get("comments", 0)
                    analytics.shares     = metrics.get("shares", 0)
                    analytics.updated_at = datetime.utcnow()

                    await asyncio.sleep(0.5)  # Rate limit respect
                except Exception as e:
                    logger.warning(f"LinkedIn analytics failed for post {post.id}: {e}")

            await session.commit()
        logger.info(f"✅ LinkedIn analytics saved for {len(posts)} posts")

    except Exception as e:
        logger.error(f"LinkedIn analytics pull failed: {e}")


async def run_learning_loop():
    """
    Analyze today's performance and save preferences for tomorrow.
    Identifies: best hooks, best times, best topics, best lengths.
    """
    logger.info("🧠 Running learning loop...")

    yesterday = date.today() - timedelta(days=1)
    yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day)
    yesterday_end = yesterday_start + timedelta(days=1)

    async with AsyncSessionLocal() as session:
        # Get all posted posts with their analytics from yesterday
        result = await session.execute(
            select(Post, PostAnalytics).join(
                PostAnalytics, Post.id == PostAnalytics.post_id, isouter=True
            ).where(
                and_(
                    Post.status == "posted",
                    Post.posted_at >= yesterday_start,
                    Post.posted_at < yesterday_end
                )
            )
        )
        rows = result.all()

    if not rows:
        logger.info("No data for learning loop")
        return

    # Compute engagement scores
    def engagement_score(analytics) -> float:
        if not analytics:
            return 0.0
        return (analytics.reach or 0) * 0.001 + \
               (analytics.likes or 0) * 2 + \
               (analytics.comments or 0) * 5 + \
               (analytics.shares or 0) * 4

    scored_posts = [(post, analytics, engagement_score(analytics)) for post, analytics in rows]
    scored_posts.sort(key=lambda x: x[2], reverse=True)

    if not scored_posts:
        return

    # Best post type
    type_scores = {}
    for post, analytics, score in scored_posts:
        t = post.post_type or "unknown"
        if t not in type_scores:
            type_scores[t] = []
        type_scores[t].append(score)

    best_type = max(type_scores, key=lambda t: sum(type_scores[t]) / len(type_scores[t]))

    # Best posting time (hour)
    time_scores = {}
    for post, analytics, score in scored_posts:
        if post.posted_at:
            hour = post.posted_at.hour
            if hour not in time_scores:
                time_scores[hour] = []
            time_scores[hour].append(score)

    best_hour = max(time_scores, key=lambda h: sum(time_scores[h]) / len(time_scores[h])) if time_scores else 10

    # Best content length
    lengths_scores = [(len(post.content), score) for post, _, score in scored_posts[:10]]
    avg_best_length = int(sum(l for l, _ in lengths_scores) / len(lengths_scores)) if lengths_scores else 200

    # Avg engagement rate
    avg_engagement = sum(s for _, _, s in scored_posts) / len(scored_posts)

    # Best post (for CTA reference)
    best_post = scored_posts[0][0]

    # Save to learning_preferences
    async with AsyncSessionLocal() as session:
        for platform in ["linkedin"]:
            pref = LearningPreference(
                date=str(yesterday),
                platform=platform,
                best_hook_type=best_type,
                best_post_time=f"{best_hour:02d}:00",
                best_topic="AI News",  # Can be enhanced with NER
                best_length=avg_best_length,
                best_cta=best_post.content[-100:] if best_post else "",
                avg_engagement=avg_engagement,
                notes=f"Top post type: {best_type}, Best hour: {best_hour}:00"
            )
            session.add(pref)
        await session.commit()

    logger.info(f"✅ Learning loop complete — Best type: {best_type}, Best hour: {best_hour}:00, Avg engagement: {avg_engagement:.2f}")
    await log_to_db("analytics", "INFO", f"Learning loop: best_type={best_type}, best_hour={best_hour}")


async def get_dashboard_stats() -> dict:
    """Get summary stats for the dashboard overview cards."""
    today = date.today()
    today_start = datetime(today.year, today.month, today.day)
    today_end = today_start + timedelta(days=1)

    async with AsyncSessionLocal() as session:
        # Today's post counts by status
        result = await session.execute(
            select(Post.platform, Post.status, func.count(Post.id))
            .where(Post.created_at >= today_start, Post.created_at < today_end)
            .group_by(Post.platform, Post.status)
        )
        status_counts = result.all()

        # Total impressions today
        result = await session.execute(
            select(func.sum(PostAnalytics.impressions), func.sum(PostAnalytics.likes))
            .join(Post, PostAnalytics.post_id == Post.id)
            .where(Post.posted_at >= today_start, Post.posted_at < today_end)
        )
        totals = result.one()

        # Posts scheduled next
        result = await session.execute(
            select(Post)
            .where(
                Post.status == "pending",
                Post.scheduled_at > datetime.now()
            )
            .order_by(Post.scheduled_at.asc())
            .limit(5)
        )
        next_posts = result.scalars().all()

        # Recent posted posts
        from sqlalchemy.orm import selectinload
        result = await session.execute(
            select(Post)
            .options(selectinload(Post.analytics))
            .where(Post.status == "posted")
            .order_by(Post.posted_at.desc())
            .limit(10)
        )
        recent_posts = result.scalars().all()

        # All-time statistics across all published posts
        result_all = await session.execute(
            select(
                func.count(Post.id),
                func.sum(PostAnalytics.likes),
                func.sum(PostAnalytics.comments),
                func.sum(PostAnalytics.shares),
                func.sum(PostAnalytics.impressions)
            )
            .join(PostAnalytics, Post.id == PostAnalytics.post_id, isouter=True)
            .where(Post.status.in_(["posted", "exported"]))
        )
        all_time_totals = result_all.one()
        all_time_posts = all_time_totals[0] or 0
        all_time_likes = all_time_totals[1] or 0
        all_time_comments = all_time_totals[2] or 0
        all_time_shares = all_time_totals[3] or 0
        all_time_impressions = all_time_totals[4] or 0
        avg_likes = all_time_likes / all_time_posts if all_time_posts > 0 else 0.0

    stats = {
        "today": {
            "linkedin": {"posted": 0, "pending": 0, "failed": 0, "total": 0},
        },
        "impressions": int(totals[0] or 0),
        "likes": int(totals[1] or 0),
        "total_published": all_time_posts,
        "total_likes": all_time_likes,
        "total_comments": all_time_comments,
        "total_shares": all_time_shares,
        "total_impressions": all_time_impressions,
        "avg_likes_per_post": avg_likes,
        "next_posts": [
            {
                "id": p.id,
                "platform": p.platform,
                "post_type": p.post_type,
                "scheduled_at": p.scheduled_at.isoformat() if p.scheduled_at else None,
                "content_preview": p.content[:100] + "..." if len(p.content) > 100 else p.content
            }
            for p in next_posts
        ],
        "recent_posts": [
            {
                "id": p.id,
                "platform": p.platform,
                "post_type": p.post_type,
                "posted_at": p.posted_at.isoformat() if p.posted_at else None,
                "content_preview": p.content[:100] + "..." if len(p.content) > 100 else p.content,
                "status": p.status,
                "likes": p.analytics.likes if p.analytics else 0,
                "comments": p.analytics.comments if p.analytics else 0,
                "shares": p.analytics.shares if p.analytics else 0
            }
            for p in recent_posts
        ]
    }

    for platform, status, count in status_counts:
        if platform in stats["today"] and status in stats["today"][platform]:
            stats["today"][platform][status] = count
            stats["today"][platform]["total"] += count

    return stats


async def get_analytics_chart_data(days: int = 7) -> dict:
    """Get time-series data for analytics charts."""
    start_date = datetime.utcnow() - timedelta(days=days)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(
                func.date(Post.posted_at).label("day"),
                Post.platform,
                func.count(Post.id).label("posts"),
                func.sum(PostAnalytics.impressions).label("impressions"),
                func.sum(PostAnalytics.likes).label("likes")
            )
            .join(PostAnalytics, Post.id == PostAnalytics.post_id, isouter=True)
            .where(Post.posted_at >= start_date, Post.status == "posted")
            .group_by(func.date(Post.posted_at), Post.platform)
            .order_by(func.date(Post.posted_at).asc())
        )
        rows = result.all()

    chart_data = {"labels": [], "linkedin": [], "impressions": [], "likes": []}
    for row in rows:
        label = str(row.day)
        if label not in chart_data["labels"]:
            chart_data["labels"].append(label)
        elif row.platform == "linkedin":
            chart_data["linkedin"].append({"day": label, "posts": row.posts, "impressions": row.impressions or 0})

    return chart_data
