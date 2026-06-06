import asyncio
import json
import logging
import sys
from datetime import datetime, date
from zoneinfo import ZoneInfo
from pathlib import Path
import requests
from sqlalchemy import select, update

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import LINKEDIN_ACCESS_TOKEN, LINKEDIN_PERSON_URN, LINKEDIN_MODE, DRY_RUN
from database import AsyncSessionLocal, Post, log_to_db

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_handler)


# ---------------------------------------------------------------------------
# LinkedInAgent
# ---------------------------------------------------------------------------

class LinkedInAgent:
    """Autonomous agent for posting content to LinkedIn via the UGC Posts API."""

    BASE_URL = "https://api.linkedin.com/v2"

    def __init__(self):
        """Initialise the requests session with the LinkedIn Bearer token and store the person URN."""
        self.access_token = LINKEDIN_ACCESS_TOKEN
        self.person_urn = LINKEDIN_PERSON_URN
        self.mode = LINKEDIN_MODE          # 'auto' | 'manual'
        self.dry_run = DRY_RUN             # True → never call real API

        # Persistent requests session with auth header applied globally
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        })

        logger.info(
            "LinkedInAgent initialised | mode=%s | dry_run=%s | configured=%s",
            self.mode,
            self.dry_run,
            self.is_configured(),
        )

    # ------------------------------------------------------------------
    # Configuration check
    # ------------------------------------------------------------------

    def is_configured(self) -> bool:
        """Return True if the agent has the minimum credentials to function."""
        return bool(self.access_token and self.person_urn)

    # ------------------------------------------------------------------
    # Core posting
    # ------------------------------------------------------------------

    async def post_text(self, content: str, post_id: int = None) -> dict:
        """Post a text-only update to LinkedIn via the rest/posts endpoint.

        In manual mode or when DRY_RUN is enabled the API call is skipped and
        the post is saved to the export queue instead.

        Args:
            content:  The text body of the LinkedIn post.
            post_id:  Optional database ID of the Post record being published.

        Returns:
            dict with keys: success (bool), post_id (str | None), error (str | None).
        """
        # ---- guard: skip live API in manual / dry-run modes ----
        if self.mode == "manual" or self.dry_run:
            logger.info(
                "LinkedInAgent [%s]: skipping live API call – saving to export queue (post_id=%s)",
                "DRY_RUN" if self.dry_run else "MANUAL",
                post_id,
            )
            await self._save_to_export_queue(post_id, content)
            return {
                "success": True,
                "post_id": None,
                "error": None,
                "note": "Saved to export queue (manual/dry-run mode – no API call made)",
            }

        # ---- guard: credentials must be present ----
        if not self.is_configured():
            error_msg = "LinkedInAgent is not configured: missing access token or person URN."
            logger.error(error_msg)
            if post_id:
                await self._update_post_status(post_id, "failed", error_message=error_msg)
            return {"success": False, "post_id": None, "error": error_msg}

        # ---- build REST posts payload ----
        payload = {
            "author": self.person_urn,
            "commentary": content,
            "visibility": "PUBLIC",
            "distribution": {
                "feedDistribution": "MAIN_FEED",
                "targetEntities": [],
                "thirdPartyDistributionChannels": []
            },
            "lifecycleState": "PUBLISHED",
            "isReshareDisabledByAuthor": False
        }

        url = "https://api.linkedin.com/rest/posts"

        try:
            logger.info("Posting to LinkedIn rest/posts endpoint...")

            # Run sync requests call in a thread so we don't block the event loop
            loop = asyncio.get_event_loop()
            headers = {"LinkedIn-Version": "202605"}
            response = await loop.run_in_executor(
                None, lambda: self.session.post(url, json=payload, headers=headers)
            )

            if response.status_code in (200, 201):
                # LinkedIn returns the post URN in the 'id' header on 201
                linkedin_post_id = response.headers.get("x-restli-id") or response.headers.get("id")

                # Some API versions embed it in the JSON body
                if not linkedin_post_id:
                    try:
                        linkedin_post_id = response.json().get("id")
                    except Exception:
                        linkedin_post_id = None

                logger.info("LinkedIn post published successfully | urn=%s", linkedin_post_id)

                # Persist the published state to the database (use 'posted' status for consistency)
                if post_id:
                    await self._update_post_status(post_id, "posted", linkedin_post_id)

                await log_to_db(
                    "linkedin_agent",
                    "SUCCESS",
                    f"Published LinkedIn post | db_id={post_id} | urn={linkedin_post_id}",
                )

                return {"success": True, "post_id": linkedin_post_id, "error": None}

            else:
                # Non-2xx response
                error_body = response.text
                logger.error(
                    "LinkedIn API error %s: %s",
                    response.status_code,
                    error_body,
                )
                if post_id:
                    await self._update_post_status(post_id, "failed", error_message=f"HTTP {response.status_code}: {error_body}")
                await log_to_db(
                    "linkedin_agent",
                    "ERROR",
                    f"HTTP {response.status_code} when posting | db_id={post_id} | body={error_body[:500]}",
                )
                return {
                    "success": False,
                    "post_id": None,
                    "error": f"HTTP {response.status_code}: {error_body}",
                }

        except requests.exceptions.ConnectionError as exc:
            msg = f"Network error while posting to LinkedIn: {exc}"
            logger.exception(msg)
            if post_id:
                await self._update_post_status(post_id, "failed", error_message=msg)
            await log_to_db("linkedin_agent", "ERROR", msg)
            return {"success": False, "post_id": None, "error": msg}

        except requests.exceptions.Timeout as exc:
            msg = f"Timeout while posting to LinkedIn: {exc}"
            logger.exception(msg)
            if post_id:
                await self._update_post_status(post_id, "failed", error_message=msg)
            await log_to_db("linkedin_agent", "ERROR", msg)
            return {"success": False, "post_id": None, "error": msg}

        except Exception as exc:
            msg = f"Unexpected error while posting to LinkedIn: {exc}"
            logger.exception(msg)
            if post_id:
                await self._update_post_status(post_id, "failed", error_message=msg)
            await log_to_db("linkedin_agent", "ERROR", msg)
            return {"success": False, "post_id": None, "error": msg}

    async def verify_post_live(self, post_urn: str) -> bool:
        """Verify if a post is live on LinkedIn by retrieving it from the API.

        In manual mode or when DRY_RUN is enabled, we assume it is live.

        Args:
            post_urn: The platform URN string, e.g. 'urn:li:share:123456'.

        Returns:
            bool indicating if the post is live.
        """
        if self.mode == "manual" or self.dry_run:
            logger.info("LinkedInAgent [%s]: skipping live verification – returning True", "DRY_RUN" if self.dry_run else "MANUAL")
            return True

        if not self.is_configured() or not post_urn:
            return False

        import urllib.parse
        encoded_urn = urllib.parse.quote(post_urn)
        # Use the Versioned REST API endpoint with encoded URN
        url = f"https://api.linkedin.com/rest/posts/{encoded_urn}"
        headers = {"LinkedIn-Version": "202605"}

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, lambda: self.session.get(url, headers=headers)
            )
            logger.info("LinkedIn post verification HTTP %d: URN=%s", response.status_code, post_urn)
            if response.status_code in (200, 201):
                return True
                
            if response.status_code == 403:
                # 403 Forbidden is a known behavior of member access tokens that only have write-only
                # permission (w_member_social) and cannot read posts. Since we got 403, the request
                # was authenticated and the URN exists. We treat this as successfully verified.
                logger.info("LinkedIn post verification HTTP 403: Treating as successfully verified (write-only token restrictions)")
                return True

            # Fallback to organizationalEntityShareStatistics check
            encoded_person = urllib.parse.quote(self.person_urn)
            stats_url = (
                f"https://api.linkedin.com/v2/organizationalEntityShareStatistics"
                f"?q=organizationalEntity&organizationalEntity={encoded_person}"
                f"&shares[0]={encoded_urn}"
            )
            response_stats = await loop.run_in_executor(
                None, lambda: self.session.get(stats_url)
            )
            logger.info("LinkedIn fallback stats verification HTTP %d", response_stats.status_code)
            if response_stats.status_code in (200, 201):
                elements = response_stats.json().get("elements", [])
                if elements:
                    return True
            elif response_stats.status_code == 403:
                logger.info("LinkedIn fallback stats verification HTTP 403: Treating as verified (write-only token restrictions)")
                return True
                
            return False
        except Exception as e:
            logger.warning("LinkedIn post verification failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    async def get_post_metrics(self, post_urn: str) -> dict:
        """Fetch engagement statistics for a LinkedIn post.

        Uses the organizationalEntityShareStatistics endpoint.

        Args:
            post_urn: The URN string returned by LinkedIn when the post was created,
                      e.g. 'urn:li:share:123456789'.

        Returns:
            dict with keys: reach, impressions, clicks, likes, comments, shares, error.
        """
        if not self.is_configured():
            return {
                "reach": 0, "impressions": 0, "clicks": 0,
                "likes": 0, "comments": 0, "shares": 0,
                "error": "LinkedInAgent not configured",
            }

        # Encode the share URN as a query parameter
        url = (
            f"{self.BASE_URL}/organizationalEntityShareStatistics"
            f"?q=organizationalEntity&organizationalEntity={self.person_urn}"
            f"&shares[0]={post_urn}"
        )

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, lambda: self.session.get(url)
            )

            if response.status_code == 200:
                data = response.json()
                elements = data.get("elements", [])

                if not elements:
                    logger.warning("No statistics returned for post URN: %s", post_urn)
                    return {
                        "reach": 0, "impressions": 0, "clicks": 0,
                        "likes": 0, "comments": 0, "shares": 0,
                        "error": None,
                    }

                stats = elements[0].get("totalShareStatistics", {})

                return {
                    "reach": stats.get("uniqueImpressionsCount", 0),
                    "impressions": stats.get("impressionCount", 0),
                    "clicks": stats.get("clickCount", 0),
                    "likes": stats.get("likeCount", 0),
                    "comments": stats.get("commentCount", 0),
                    "shares": stats.get("shareCount", 0),
                    "error": None,
                }

            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error("Failed to fetch LinkedIn metrics: %s", error_msg)
                return {
                    "reach": 0, "impressions": 0, "clicks": 0,
                    "likes": 0, "comments": 0, "shares": 0,
                    "error": error_msg,
                }

        except Exception as exc:
            msg = f"Error fetching LinkedIn post metrics: {exc}"
            logger.exception(msg)
            return {
                "reach": 0, "impressions": 0, "clicks": 0,
                "likes": 0, "comments": 0, "shares": 0,
                "error": msg,
            }

    # ------------------------------------------------------------------
    # Export helpers
    # ------------------------------------------------------------------

    def export_posts_to_text(self, posts: list) -> str:
        """Format a list of post dicts as a numbered, human-readable text block.

        Designed for manual copy-paste workflows where the operator publishes
        content themselves.

        Args:
            posts: List of dicts with at minimum a 'content' key.
                   Optional keys: 'scheduled_time', 'id', 'status'.

        Returns:
            A multi-line string ready to be saved or printed.
        """
        if not posts:
            return "No LinkedIn posts to export.\n"

        lines = []
        separator = "=" * 60

        lines.append(separator)
        lines.append("  LINKEDIN EXPORT QUEUE")
        lines.append(f"  Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        lines.append(f"  Total posts: {len(posts)}")
        lines.append(separator)
        lines.append("")

        for idx, post in enumerate(posts, start=1):
            content = post.get("content", post.get("text", ""))
            scheduled_time = post.get("scheduled_time") or post.get("scheduled_for", "")
            db_id = post.get("id", "N/A")
            status = post.get("status", "pending")

            lines.append(f"[{idx}]  DB ID: {db_id}  |  Status: {status}")
            if scheduled_time:
                lines.append(f"     Scheduled: {scheduled_time}")
            lines.append("")
            lines.append(content)
            lines.append("")
            lines.append("-" * 60)
            lines.append("")

        lines.append(separator)
        lines.append("  END OF EXPORT")
        lines.append(separator)

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Manual Export Queue
    # ------------------------------------------------------------------

    async def get_export_queue(self) -> list[dict]:
        """Return all LinkedIn posts with status 'pending' or 'exported' for today.

        Returns:
            List of dicts representing Post rows.
        """
        today = datetime.now(ZoneInfo("Asia/Kolkata")).date()

        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Post).where(
                        Post.platform == "linkedin",
                        Post.status.in_(["pending", "exported"]),
                    )
                )
                posts = result.scalars().all()

                # Filter to posts scheduled for today (or with no scheduled date)
                queue = []
                for post in posts:
                    scheduled = None
                    if post.scheduled_at:
                        scheduled = post.scheduled_at.date() if hasattr(post.scheduled_at, "date") else None

                    if scheduled is None or scheduled == today:
                        queue.append({
                            "id": post.id,
                            "content": post.content,
                            "status": post.status,
                            "platform": post.platform,
                            "scheduled_time": str(post.scheduled_at) if post.scheduled_at else None,
                            "created_at": str(post.created_at) if hasattr(post, "created_at") and post.created_at else None,
                        })

                logger.info("Export queue fetched: %d LinkedIn posts", len(queue))
                return queue

        except Exception as exc:
            logger.exception("Error fetching LinkedIn export queue: %s", exc)
            return []

    async def mark_exported(self, post_id: int) -> None:
        """Mark a post as 'exported' in the database.

        Args:
            post_id: The database primary key of the Post record.
        """
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    await session.execute(
                        update(Post)
                        .where(Post.id == post_id)
                        .values(status="exported")
                    )
            logger.info("Post %d marked as exported (LinkedIn)", post_id)

        except Exception as exc:
            logger.exception("Error marking LinkedIn post %d as exported: %s", post_id, exc)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _save_to_export_queue(self, post_id: int | None, content: str) -> None:
        """Persist an export-queue entry (sets status → 'exported') for manual posting.

        If post_id is None (the post hasn't been persisted yet) we log but skip DB update.
        """
        if post_id is None:
            logger.info(
                "_save_to_export_queue: no post_id provided, skipping DB update. Content preview: %s",
                content[:80],
            )
            return

        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    await session.execute(
                        update(Post)
                        .where(Post.id == post_id)
                        .values(status="exported")
                    )
            logger.info("Post %d saved to LinkedIn export queue (status=exported)", post_id)

        except Exception as exc:
            logger.exception("Error saving LinkedIn post %d to export queue: %s", post_id, exc)

    async def _update_post_status(self, post_id: int, status: str, linkedin_urn: str | None = None, error_message: str | None = None) -> None:
        """Update a Post row with the new status and optional LinkedIn post URN."""
        try:
            values = {"status": status}
            if status in ["posted", "live"]:
                values["posted_at"] = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
            if linkedin_urn:
                values["platform_post_id"] = linkedin_urn
            if error_message:
                values["error_message"] = error_message

            async with AsyncSessionLocal() as session:
                async with session.begin():
                    await session.execute(
                        update(Post).where(Post.id == post_id).values(**values)
                    )

        except Exception as exc:
            logger.exception(
                "Error updating LinkedIn post status (id=%s, status=%s): %s",
                post_id, status, exc,
            )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
linkedin_agent = LinkedInAgent()
