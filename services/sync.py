"""
services/sync.py — Complete LinkedIn Post & Metrics Synchronization Engine
"""
import asyncio
import logging
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import select, delete, and_, func

from database import AsyncSessionLocal, Post, PostAnalytics, log_to_db
from agents.linkedin_agent import linkedin_agent
from services.deduplication import clean_for_hash, get_content_hash

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

sync_lock = asyncio.Lock()


async def sync_posts_and_metrics():
    """Wrapper function to perform full sync of LinkedIn posts and metrics."""
    await sync_all_posts()


async def sync_all_posts():
    """Perform a full reconciliation and sync with LinkedIn.
    
    1. Fetch list of posts from LinkedIn.
    2. Reconcile database (backfill missing, update status, remove duplicates, check edits).
    3. Fetch and update metrics for all posts.
    """
    if sync_lock.locked():
        logger.info("Sync is already running. Skipping concurrent trigger.")
        return
        
    async with sync_lock:
        logger.info("🔄 Starting LinkedIn synchronization cycle...")
        try:
            from main import broadcast_event
            await broadcast_event("sync_started", {})
        except Exception:
            pass

        if not linkedin_agent.is_configured():
            logger.warning("LinkedInAgent is not configured. Skipping sync.")
            try:
                from main import broadcast_event
                await broadcast_event("sync_failed", {"error": "LinkedInAgent is not configured"})
            except Exception:
                pass
            return

        # Fetch posts from LinkedIn REST API
        encoded_person = urllib.parse.quote(linkedin_agent.person_urn)
        # We query the REST finder by author
        url = f"https://api.linkedin.com/rest/posts?q=author&author={encoded_person}&count=100"
        headers = {
            "Authorization": f"Bearer {linkedin_agent.access_token}",
            "LinkedIn-Version": "202605",
            "X-Restli-Protocol-Version": "2.0.0",
        }

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, lambda: linkedin_agent.session.get(url, headers=headers)
            )
            
            if response.status_code == 403:
                logger.warning("LinkedIn sync returned 403 Forbidden. Token lacks r_member_social scope. Reconciling using local DB only.")
                # We can't fetch posts, so we just sync metrics for posts we have URNs for (which will also return 403, but we try anyway)
                await _sync_local_posts_metrics_only()
                try:
                    from main import broadcast_event
                    await broadcast_event("sync_completed", {"count": 0, "note": "Write-only token restrictions"})
                except Exception:
                    pass
                return
                
            if response.status_code not in (200, 201):
                logger.error(f"LinkedIn sync API error {response.status_code}: {response.text}")
                try:
                    from main import broadcast_event
                    await broadcast_event("sync_failed", {"error": f"LinkedIn API HTTP {response.status_code}"})
                except Exception:
                    pass
                return

            data = response.json()
            elements = data.get("elements", [])
            logger.info(f"Retrieved {len(elements)} posts from LinkedIn API.")

            # Process elements and reconcile with database
            await _reconcile_posts(elements)

            # Sync metrics for all active posts
            await _sync_local_posts_metrics_only()

            await log_to_db("sync", "SUCCESS", f"LinkedIn synchronization complete. Processed {len(elements)} posts.")
            try:
                from main import broadcast_event
                await broadcast_event("sync_completed", {"count": len(elements)})
            except Exception:
                pass

        except Exception as e:
            logger.exception(f"Error during LinkedIn synchronization: {e}")
            await log_to_db("sync", "ERROR", f"LinkedIn sync failed: {str(e)[:500]}")
            try:
                from main import broadcast_event
                await broadcast_event("sync_failed", {"error": str(e)})
            except Exception:
                pass


async def _reconcile_posts(elements: list):
    """Reconcile LinkedIn elements list with database records."""
    async with AsyncSessionLocal() as session:
        # We will retrieve all local posts
        result = await session.execute(select(Post))
        local_posts = result.scalars().all()
        
        # Build map of local posts by platform_post_id
        local_by_urn = {p.platform_post_id: p for p in local_posts if p.platform_post_id}
        # Build map by content hash (deduplication cleaner hash)
        local_by_hash = {p.content_hash: p for p in local_posts if p.content_hash}

        api_urns = set()
        oldest_posted_date = datetime.now()
        
        for elem in elements:
            urn = elem.get("id")
            if not urn:
                continue
            api_urns.add(urn)
            
            # Extract commentary text
            commentary = elem.get("commentary") or elem.get("comment") or ""
            
            # Extract posted time (LinkedIn publishedAt is in milliseconds epoch)
            pub_ms = elem.get("publishedAt") or elem.get("createdAt")
            if pub_ms:
                posted_date = datetime.fromtimestamp(pub_ms / 1000, tz=ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
            else:
                posted_date = datetime.now()

            if posted_date < oldest_posted_date:
                oldest_posted_date = posted_date

            # Clean and hash the commentary text
            clean_text = clean_for_hash(commentary) if commentary else ""
            content_hash = get_content_hash(clean_text) if clean_text else None

            # 1. Match by URN
            db_post = local_by_urn.get(urn)
            
            # 2. Match by Content Hash if URN not matched
            if not db_post and content_hash:
                db_post = local_by_hash.get(content_hash)

            if db_post:
                # Post exists. Update URN and status if needed.
                db_post.platform_post_id = urn
                db_post.status = "live"
                db_post.posted_at = db_post.posted_at or posted_date
                
                # Check for edits
                if commentary and db_post.content != commentary:
                    logger.info(f"Post #{db_post.id} edited on LinkedIn. Updating local content.")
                    db_post.content = commentary
                    db_post.content_hash = content_hash
                
                session.add(db_post)
            else:
                # 3. Post is missing. Backfill / import it!
                logger.info(f"Found missing LinkedIn post. Backfilling URN {urn}")
                new_post = Post(
                    content=commentary,
                    platform="linkedin",
                    status="live",
                    scheduled_at=posted_date,
                    posted_at=posted_date,
                    platform_post_id=urn,
                    created_at=posted_date,
                    content_hash=content_hash
                )
                session.add(new_post)
                
        await session.commit()
        
        # Deduplicate local database
        await _deduplicate_db_records(session)

        # Detect deleted posts:
        # If a post URN is in our DB with status 'live' or 'posted' but not in api_urns, and it fell
        # within the date range of the fetched posts (posted_at >= oldest_posted_date), it was deleted.
        for post in local_posts:
            if post.platform_post_id and post.platform_post_id not in api_urns:
                if post.status in ["live", "posted"] and post.posted_at and post.posted_at >= oldest_posted_date:
                    logger.info(f"Post #{post.id} (URN: {post.platform_post_id}) not found on LinkedIn. Marking as deleted.")
                    post.status = "failed"
                    post.error_message = "Post was deleted from LinkedIn"
                    session.add(post)
        await session.commit()


async def _sync_local_posts_metrics_only():
    """Sync metrics for existing posts in the database."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Post).where(
                and_(
                    Post.platform == "linkedin",
                    Post.status.in_(["posted", "live"]),
                    Post.platform_post_id.isnot(None)
                )
            )
        )
        posts = result.scalars().all()
        
    for p in posts:
        await sync_single_post(p.id, p.platform_post_id)


async def sync_single_post(post_id: int, post_urn: str):
    """Fetch exact metrics from LinkedIn API for a single post and save to DB."""
    if not post_urn or not linkedin_agent.is_configured():
        return

    metrics = await linkedin_agent.get_post_metrics(post_urn)
    if metrics is None:
        logger.warning(f"Could not sync metrics for post #{post_id} (URN: {post_urn}): Empty response")
        return

    # If the error field is populated with a real error (not None), log and skip DB update
    if metrics.get("error"):
        logger.warning(f"Could not sync metrics for post #{post_id} (URN: {post_urn}): {metrics['error']}")
        return

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(PostAnalytics).where(PostAnalytics.post_id == post_id)
        )
        analytics = result.scalar_one_or_none()
        if not analytics:
            analytics = PostAnalytics(post_id=post_id)
            session.add(analytics)

        analytics.reach = metrics.get("reach", 0)
        analytics.impressions = metrics.get("impressions", 0)
        analytics.link_clicks = metrics.get("clicks", 0)
        analytics.likes = metrics.get("likes", 0)
        analytics.comments = metrics.get("comments", 0)
        analytics.shares = metrics.get("shares", 0)
        analytics.updated_at = datetime.utcnow()
        
        await session.commit()
    logger.info(f"Synced metrics for post #{post_id} successfully.")


async def _deduplicate_db_records(session):
    """Detect and remove duplicate database records by platform_post_id or content_hash."""
    # Find duplicate platform_post_ids
    result = await session.execute(
        select(Post.platform_post_id, func.count(Post.id))
        .where(Post.platform_post_id.isnot(None))
        .group_by(Post.platform_post_id)
        .having(func.count(Post.id) > 1)
    )
    dup_urns = [row[0] for row in result.all()]
    
    for urn in dup_urns:
        res_posts = await session.execute(
            select(Post).where(Post.platform_post_id == urn).order_by(Post.id.asc())
        )
        posts = res_posts.scalars().all()
        # Keep the first one, delete the rest
        for extra in posts[1:]:
            logger.info(f"Removing duplicate post record ID {extra.id} (URN: {urn})")
            await session.execute(delete(PostAnalytics).where(PostAnalytics.post_id == extra.id))
            await session.execute(delete(Post).where(Post.id == extra.id))
            
    # Find duplicate content_hashes for posted/live posts
    result_hash = await session.execute(
        select(Post.content_hash, func.count(Post.id))
        .where(and_(Post.content_hash.isnot(None), Post.status.in_(["posted", "live"])))
        .group_by(Post.content_hash)
        .having(func.count(Post.id) > 1)
    )
    dup_hashes = [row[0] for row in result_hash.all()]
    
    for h in dup_hashes:
        res_posts = await session.execute(
            select(Post).where(and_(Post.content_hash == h, Post.status.in_(["posted", "live"]))).order_by(Post.id.asc())
        )
        posts = res_posts.scalars().all()
        # Keep the first one, delete the rest
        for extra in posts[1:]:
            logger.info(f"Removing duplicate post record ID {extra.id} (Hash: {h})")
            await session.execute(delete(PostAnalytics).where(PostAnalytics.post_id == extra.id))
            await session.execute(delete(Post).where(Post.id == extra.id))
            
    await session.commit()
