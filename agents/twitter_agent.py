"""
Twitter/X Posting Agent
Handles all interactions with the Twitter/X API via Tweepy.
Supports single tweets, threads, polls, and metrics retrieval.
"""

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

import tweepy

# Ensure the project root is on sys.path so sibling packages are importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    TWITTER_API_KEY,
    TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_SECRET,
    TWITTER_BEARER_TOKEN,
    TWITTER_MODE,
    DRY_RUN,
)
from database import AsyncSessionLocal, Post, log_to_db

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: manual export formatter (standalone function, no class needed)
# ---------------------------------------------------------------------------

def format_for_export(posts: list[dict]) -> str:
    """
    Format a list of post dicts as a numbered plain-text block suitable for
    copy-paste or saving to a file.

    Args:
        posts: List of dicts, each expected to contain at least a 'content'
               key.  Optional keys: 'tweet_id', 'created_at', 'status'.

    Returns:
        A multi-line string with each post numbered sequentially.
    """
    if not posts:
        return "(no posts to export)"

    lines: list[str] = []
    for idx, post in enumerate(posts, start=1):
        content = post.get("content", "").strip()
        tweet_id = post.get("tweet_id") or post.get("platform_post_id", "N/A")
        created_at = post.get("created_at", "N/A")
        status = post.get("status", "N/A")

        lines.append(f"--- Post #{idx} ---")
        lines.append(f"Tweet ID  : {tweet_id}")
        lines.append(f"Created   : {created_at}")
        lines.append(f"Status    : {status}")
        lines.append(f"Content   :\n{content}")
        lines.append("")  # blank separator

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main agent class
# ---------------------------------------------------------------------------

class TwitterAgent:
    """
    Async-friendly wrapper around the Tweepy v2 Client (and v1.1 API for
    media uploads).  All public methods are coroutines so they integrate
    cleanly with the rest of the async pipeline.
    """

    # Twitter API v2 allows up to 100 tweet IDs per batch lookup
    _BATCH_LIMIT = 100

    def __init__(self) -> None:
        """
        Initialise the Tweepy clients using credentials from config.
        Logs whether the agent is properly configured.
        """
        self._configured: bool = False

        # ------------------------------------------------------------------
        # Tweepy v2 Client  (used for tweets, polls, metrics)
        # ------------------------------------------------------------------
        try:
            self.client = tweepy.Client(
                bearer_token=TWITTER_BEARER_TOKEN,
                consumer_key=TWITTER_API_KEY,
                consumer_secret=TWITTER_API_SECRET,
                access_token=TWITTER_ACCESS_TOKEN,
                access_token_secret=TWITTER_ACCESS_SECRET,
                wait_on_rate_limit=False,  # We handle rate limits ourselves
            )

            # ------------------------------------------------------------------
            # Tweepy v1.1 API  (used for media uploads – still required in 2024)
            # ------------------------------------------------------------------
            auth = tweepy.OAuth1UserHandler(
                consumer_key=TWITTER_API_KEY,
                consumer_secret=TWITTER_API_SECRET,
                access_token=TWITTER_ACCESS_TOKEN,
                access_token_secret=TWITTER_ACCESS_SECRET,
            )
            self.api = tweepy.API(auth, wait_on_rate_limit=False)

            self._configured = self.is_configured()

            if self._configured:
                logger.info(
                    "TwitterAgent initialised successfully. "
                    "Mode=%s | DRY_RUN=%s",
                    TWITTER_MODE,
                    DRY_RUN,
                )
            else:
                logger.warning(
                    "TwitterAgent initialised but one or more API credentials "
                    "are missing or empty. The agent will not be able to post."
                )

        except Exception as exc:
            logger.error("Failed to initialise TwitterAgent: %s", exc, exc_info=True)
            # Keep client references as None so callers can detect the failure
            self.client = None  # type: ignore[assignment]
            self.api = None     # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Public helper
    # ------------------------------------------------------------------

    def is_configured(self) -> bool:
        """
        Return True if all required API credentials are present and non-empty.
        Does NOT make a network call – purely a local credential check.
        """
        required = [
            TWITTER_API_KEY,
            TWITTER_API_SECRET,
            TWITTER_ACCESS_TOKEN,
            TWITTER_ACCESS_SECRET,
            TWITTER_BEARER_TOKEN,
        ]
        return all(bool(v) for v in required)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _handle_rate_limit(self, exc: Exception) -> int:
        """
        Inspect a Tweepy exception for HTTP 429 (rate-limit) information.
        Logs the situation and returns the number of seconds the caller
        should wait before retrying.

        Args:
            exc: The caught exception (TweepyException or subclass).

        Returns:
            retry_after seconds (int).  Defaults to 60 if not determinable.
        """
        retry_after: int = 60  # sensible default

        # Tweepy wraps HTTP errors in TweepyException; the response object
        # is available via exc.response for HTTPException subclasses.
        if isinstance(exc, tweepy.errors.TweepyException):
            response = getattr(exc, "response", None)
            if response is not None:
                status_code = getattr(response, "status_code", None)
                if status_code == 429:
                    # The Retry-After header is the authoritative source
                    retry_after_header = response.headers.get("Retry-After")
                    if retry_after_header:
                        try:
                            retry_after = int(retry_after_header)
                        except ValueError:
                            pass
                    logger.warning(
                        "Rate limit hit (HTTP 429). Retry after %d seconds.",
                        retry_after,
                    )
                    return retry_after

        # Fallback: check for the string "429" in the exception message
        if "429" in str(exc):
            logger.warning(
                "Possible rate limit (429 detected in error message). "
                "Defaulting retry_after=%d seconds.",
                retry_after,
            )
            return retry_after

        # Not a rate-limit error – return 0 to signal "don't wait"
        return 0

    async def _update_post_status(
        self,
        post_id: int | None,
        status: str,
        platform_post_id: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Persist a Post record's status (and optionally its platform_post_id and error_message)
        to the database.  Silently skips if post_id is None.

        Args:
            post_id:          Primary key of the Post row to update.
            status:           New status string (e.g. 'posted', 'failed').
            platform_post_id: The tweet ID returned by the Twitter API.
            error_message:    The error message string, if failed.
        """
        if post_id is None:
            return

        try:
            async with AsyncSessionLocal() as session:
                post = await session.get(Post, post_id)
                if post is None:
                    logger.warning(
                        "_update_post_status: Post(id=%d) not found in DB.", post_id
                    )
                    return

                post.status = status
                if status == "posted":
                    post.posted_at = datetime.utcnow()
                if platform_post_id is not None:
                    post.platform_post_id = str(platform_post_id)
                if error_message is not None:
                    post.error_message = str(error_message)

                await session.commit()
                logger.debug(
                    "Post(id=%d) updated → status=%s, platform_post_id=%s, error_message=%s",
                    post_id,
                    status,
                    platform_post_id,
                    error_message,
                )

        except Exception as exc:
            logger.error(
                "Failed to update Post(id=%s) in DB: %s", post_id, exc, exc_info=True
            )

    # ------------------------------------------------------------------
    # Core posting methods
    # ------------------------------------------------------------------

    async def post_tweet(
        self,
        content: str,
        post_id: int | None = None,
    ) -> dict:
        """
        Post a single tweet to Twitter/X.

        Args:
            content: The text body of the tweet (max 280 characters).
            post_id: Optional DB primary key of the related Post row.
                     When supplied the Post's status is updated in the DB.

        Returns:
            {
                'success':  bool,
                'tweet_id': str | None,
                'error':    str | None,
            }
        """
        logger.info("post_tweet called | post_id=%s | content_len=%d", post_id, len(content))

        # ── DRY RUN ──────────────────────────────────────────────────────────
        if DRY_RUN:
            fake_id = f"DRY_RUN_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
            logger.info("[DRY RUN] Would post tweet: %s", content)
            await self._update_post_status(post_id, "dry_run", fake_id)
            return {"success": True, "tweet_id": fake_id, "error": None}

        # ── Credential guard ─────────────────────────────────────────────────
        if not self._configured or self.client is None:
            err = "TwitterAgent is not configured – missing API credentials."
            logger.error(err)
            await self._update_post_status(post_id, "failed")
            return {"success": False, "tweet_id": None, "error": err}

        # ── Real API call ────────────────────────────────────────────────────
        try:
            # tweepy.Client.create_tweet is synchronous; run in executor so
            # we don't block the event loop.
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.create_tweet(text=content),
            )

            tweet_id = str(response.data["id"])
            logger.info("Tweet posted successfully. tweet_id=%s", tweet_id)

            await self._update_post_status(post_id, "posted", tweet_id)
            await log_to_db("twitter_agent", "SUCCESS", f"Tweet posted: id={tweet_id}")

            return {"success": True, "tweet_id": tweet_id, "error": None}

        except tweepy.errors.TweepyException as exc:
            wait = self._handle_rate_limit(exc)
            if wait:
                logger.warning("Rate-limited. Caller should retry after %d s.", wait)

            logger.error("Failed to post tweet: %s", exc, exc_info=True)
            await self._update_post_status(post_id, "failed", error_message=str(exc))
            await log_to_db("twitter_agent", "ERROR", f"Failed to post tweet: {exc}")
            return {"success": False, "tweet_id": None, "error": str(exc)}

        except Exception as exc:
            logger.error("Unexpected error posting tweet: %s", exc, exc_info=True)
            await self._update_post_status(post_id, "failed", error_message=str(exc))
            await log_to_db("twitter_agent", "ERROR", f"Unexpected error posting tweet: {exc}")
            return {"success": False, "tweet_id": None, "error": str(exc)}

    # ------------------------------------------------------------------

    async def post_thread(
        self,
        tweets: list[str],
        post_ids: list[int] | None = None,
    ) -> dict:
        """
        Post a Twitter thread by chaining tweets as replies.

        Args:
            tweets:   Ordered list of tweet text strings.
            post_ids: Optional list of DB Post PKs aligned with ``tweets``.
                      May be shorter than ``tweets`` or None.

        Returns:
            {
                'success':   bool,
                'tweet_ids': list[str],
                'error':     str | None,
            }
        """
        if not tweets:
            return {"success": False, "tweet_ids": [], "error": "No tweets provided."}

        logger.info(
            "post_thread called | %d tweets | post_ids=%s", len(tweets), post_ids
        )

        # ── DRY RUN ──────────────────────────────────────────────────────────
        if DRY_RUN:
            fake_ids: list[str] = []
            for idx, text in enumerate(tweets):
                fake_id = f"DRY_RUN_THREAD_{idx}_{datetime.utcnow().strftime('%f')}"
                fake_ids.append(fake_id)
                logger.info("[DRY RUN] Thread tweet %d: %s", idx + 1, text)
                pid = (post_ids[idx] if post_ids and idx < len(post_ids) else None)
                await self._update_post_status(pid, "dry_run", fake_id)
            return {"success": True, "tweet_ids": fake_ids, "error": None}

        # ── Credential guard ─────────────────────────────────────────────────
        if not self._configured or self.client is None:
            err = "TwitterAgent is not configured – missing API credentials."
            logger.error(err)
            if post_ids:
                for pid in post_ids:
                    await self._update_post_status(pid, "failed", error_message=err)
            return {"success": False, "tweet_ids": [], "error": err}

        # ── Real posting loop ────────────────────────────────────────────────
        tweet_ids: list[str] = []
        reply_to_id: str | None = None
        loop = asyncio.get_event_loop()

        try:
            for idx, text in enumerate(tweets):
                pid = (post_ids[idx] if post_ids and idx < len(post_ids) else None)

                kwargs: dict = {"text": text}
                if reply_to_id:
                    kwargs["in_reply_to_tweet_id"] = reply_to_id

                try:
                    response = await loop.run_in_executor(
                        None,
                        lambda kw=kwargs: self.client.create_tweet(**kw),
                    )
                    tweet_id = str(response.data["id"])
                    tweet_ids.append(tweet_id)
                    reply_to_id = tweet_id

                    logger.info(
                        "Thread tweet %d/%d posted. tweet_id=%s",
                        idx + 1,
                        len(tweets),
                        tweet_id,
                    )
                    await self._update_post_status(pid, "posted", tweet_id)

                    # Small courtesy delay between thread tweets to reduce
                    # the chance of being throttled mid-thread.
                    if idx < len(tweets) - 1:
                        await asyncio.sleep(1)

                except tweepy.errors.TweepyException as exc:
                    wait = self._handle_rate_limit(exc)
                    logger.error(
                        "Error posting thread tweet %d: %s", idx + 1, exc, exc_info=True
                    )
                    await self._update_post_status(pid, "failed", error_message=str(exc))
                    # Mark all subsequent tweets in this thread as failed in the DB too
                    if post_ids:
                        for rem_idx in range(idx + 1, len(tweets)):
                            if rem_idx < len(post_ids):
                                await self._update_post_status(post_ids[rem_idx], "failed", error_message=f"Aborted due to failure in preceding tweet: {exc}")
                    # Partial thread – surface what succeeded and what failed
                    await log_to_db("twitter_agent", "ERROR", f"Failed posting thread at tweet {idx + 1}/{len(tweets)}: {exc}")
                    return {
                        "success": False,
                        "tweet_ids": tweet_ids,
                        "error": f"Failed at tweet {idx + 1}/{len(tweets)}: {exc}",
                    }

            await log_to_db("twitter_agent", "SUCCESS", f"Thread posted: {len(tweet_ids)} tweets, root={tweet_ids[0]}")
            return {"success": True, "tweet_ids": tweet_ids, "error": None}

        except Exception as exc:
            logger.error("Unexpected error posting thread: %s", exc, exc_info=True)
            # Mark all remaining unposted tweets in the thread as failed
            if post_ids:
                for idx, pid in enumerate(post_ids):
                    if idx >= len(tweet_ids):
                        await self._update_post_status(pid, "failed", error_message=str(exc))
            await log_to_db("twitter_agent", "ERROR", f"Unexpected error posting thread: {exc}")
            return {
                "success": False,
                "tweet_ids": tweet_ids,
                "error": str(exc),
            }

    # ------------------------------------------------------------------

    async def post_poll(
        self,
        question: str,
        options: list[str],
        duration_minutes: int = 1440,
        post_id: int | None = None,
    ) -> dict:
        """
        Post a poll tweet.

        Twitter constraints:
          - 2–4 options.
          - duration_minutes: 5 – 10080 (7 days).

        Args:
            question:         The poll question (tweet body text).
            options:          List of 2–4 answer strings.
            duration_minutes: How long the poll runs (default = 24 h).
            post_id:          Optional DB Post PK for status tracking.

        Returns:
            {
                'success':  bool,
                'tweet_id': str | None,
                'error':    str | None,
            }
        """
        logger.info(
            "post_poll called | question=%r | options=%s | duration=%d min | post_id=%s",
            question,
            options,
            duration_minutes,
            post_id,
        )

        # ── Validate options ─────────────────────────────────────────────────
        if not (2 <= len(options) <= 4):
            err = f"Poll must have 2–4 options; got {len(options)}."
            logger.error(err)
            return {"success": False, "tweet_id": None, "error": err}

        # Clamp duration to Twitter-allowed bounds (5 min – 7 days)
        duration_minutes = max(5, min(duration_minutes, 10_080))

        # ── DRY RUN ──────────────────────────────────────────────────────────
        if DRY_RUN:
            fake_id = f"DRY_RUN_POLL_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
            logger.info(
                "[DRY RUN] Would post poll: %r | options=%s | duration=%d min",
                question,
                options,
                duration_minutes,
            )
            await self._update_post_status(post_id, "dry_run", fake_id)
            return {"success": True, "tweet_id": fake_id, "error": None}

        # ── Credential guard ─────────────────────────────────────────────────
        if not self._configured or self.client is None:
            err = "TwitterAgent is not configured – missing API credentials."
            logger.error(err)
            await self._update_post_status(post_id, "failed")
            return {"success": False, "tweet_id": None, "error": err}

        # ── Real API call ────────────────────────────────────────────────────
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.create_tweet(
                    text=question,
                    poll_options=options,
                    poll_duration_minutes=duration_minutes,
                ),
            )

            tweet_id = str(response.data["id"])
            logger.info("Poll posted successfully. tweet_id=%s", tweet_id)

            await self._update_post_status(post_id, "posted", tweet_id)
            await log_to_db("twitter_agent", "SUCCESS", f"Poll posted: id={tweet_id}, question={question!r}")

            return {"success": True, "tweet_id": tweet_id, "error": None}

        except tweepy.errors.TweepyException as exc:
            wait = self._handle_rate_limit(exc)
            if wait:
                logger.warning("Rate-limited posting poll. Retry after %d s.", wait)

            logger.error("Failed to post poll: %s", exc, exc_info=True)
            await self._update_post_status(post_id, "failed", error_message=str(exc))
            await log_to_db("twitter_agent", "ERROR", f"Failed to post poll: {exc}")
            return {"success": False, "tweet_id": None, "error": str(exc)}

        except Exception as exc:
            logger.error("Unexpected error posting poll: %s", exc, exc_info=True)
            await self._update_post_status(post_id, "failed", error_message=str(exc))
            await log_to_db("twitter_agent", "ERROR", f"Unexpected error posting poll: {exc}")
            return {"success": False, "tweet_id": None, "error": str(exc)}

    # ------------------------------------------------------------------
    # Metrics methods
    # ------------------------------------------------------------------

    async def get_tweet_metrics(self, tweet_id: str) -> dict:
        """
        Fetch public engagement metrics for a single tweet.

        Args:
            tweet_id: The Twitter numeric tweet ID (as a string).

        Returns:
            On success:
            {
                'success':        True,
                'tweet_id':       str,
                'impressions':    int,
                'like_count':     int,
                'retweet_count':  int,
                'reply_count':    int,
                'bookmark_count': int,
                'error':          None,
            }
            On failure:
            {
                'success': False,
                'error':   str,
                ...metric keys set to 0,
            }
        """
        logger.debug("get_tweet_metrics | tweet_id=%s", tweet_id)

        _empty_metrics = {
            "impressions": 0,
            "like_count": 0,
            "retweet_count": 0,
            "reply_count": 0,
            "bookmark_count": 0,
        }

        if not self._configured or self.client is None:
            err = "TwitterAgent is not configured – missing API credentials."
            logger.error(err)
            return {"success": False, "tweet_id": tweet_id, "error": err, **_empty_metrics}

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.get_tweet(
                    id=tweet_id,
                    tweet_fields=["public_metrics"],
                ),
            )

            if response.data is None:
                err = f"Tweet {tweet_id} not found or not accessible."
                logger.warning(err)
                return {"success": False, "tweet_id": tweet_id, "error": err, **_empty_metrics}

            metrics: dict = response.data.get("public_metrics", {})

            return {
                "success": True,
                "tweet_id": tweet_id,
                "impressions": metrics.get("impression_count", 0),
                "like_count": metrics.get("like_count", 0),
                "retweet_count": metrics.get("retweet_count", 0),
                "reply_count": metrics.get("reply_count", 0),
                "bookmark_count": metrics.get("bookmark_count", 0),
                "error": None,
            }

        except tweepy.errors.TweepyException as exc:
            wait = self._handle_rate_limit(exc)
            if wait:
                logger.warning("Rate-limited fetching metrics. Retry after %d s.", wait)

            logger.error(
                "Failed to fetch metrics for tweet %s: %s", tweet_id, exc, exc_info=True
            )
            return {
                "success": False,
                "tweet_id": tweet_id,
                "error": str(exc),
                **_empty_metrics,
            }

        except Exception as exc:
            logger.error(
                "Unexpected error fetching metrics for tweet %s: %s",
                tweet_id,
                exc,
                exc_info=True,
            )
            return {
                "success": False,
                "tweet_id": tweet_id,
                "error": str(exc),
                **_empty_metrics,
            }

    # ------------------------------------------------------------------

    async def get_multiple_metrics(self, tweet_ids: list[str]) -> list[dict]:
        """
        Batch-fetch public metrics for up to 100 tweet IDs per call.
        Automatically chunks larger lists.

        Args:
            tweet_ids: List of tweet ID strings (up to 100 per Twitter batch).

        Returns:
            List of metric dicts in the same order as ``tweet_ids``.
            Each dict mirrors the structure returned by get_tweet_metrics.
        """
        if not tweet_ids:
            return []

        logger.info("get_multiple_metrics | %d tweet IDs requested", len(tweet_ids))

        if not self._configured or self.client is None:
            err = "TwitterAgent is not configured – missing API credentials."
            logger.error(err)
            _empty = {
                "impressions": 0,
                "like_count": 0,
                "retweet_count": 0,
                "reply_count": 0,
                "bookmark_count": 0,
            }
            return [
                {"success": False, "tweet_id": tid, "error": err, **_empty}
                for tid in tweet_ids
            ]

        results: list[dict] = []
        loop = asyncio.get_event_loop()

        # Process in chunks of _BATCH_LIMIT (100)
        for chunk_start in range(0, len(tweet_ids), self._BATCH_LIMIT):
            chunk = tweet_ids[chunk_start : chunk_start + self._BATCH_LIMIT]
            logger.debug(
                "Fetching metrics batch %d-%d", chunk_start, chunk_start + len(chunk) - 1
            )

            try:
                response = await loop.run_in_executor(
                    None,
                    lambda c=chunk: self.client.get_tweets(
                        ids=c,
                        tweet_fields=["public_metrics"],
                    ),
                )

                # Build a lookup by tweet id for fast alignment
                metrics_by_id: dict[str, dict] = {}
                if response.data:
                    for tweet in response.data:
                        pm = tweet.get("public_metrics", {})
                        metrics_by_id[str(tweet["id"])] = {
                            "success": True,
                            "tweet_id": str(tweet["id"]),
                            "impressions": pm.get("impression_count", 0),
                            "like_count": pm.get("like_count", 0),
                            "retweet_count": pm.get("retweet_count", 0),
                            "reply_count": pm.get("reply_count", 0),
                            "bookmark_count": pm.get("bookmark_count", 0),
                            "error": None,
                        }

                # Align results with original chunk order; fill missing entries
                _empty_m = {
                    "impressions": 0,
                    "like_count": 0,
                    "retweet_count": 0,
                    "reply_count": 0,
                    "bookmark_count": 0,
                }
                for tid in chunk:
                    if tid in metrics_by_id:
                        results.append(metrics_by_id[tid])
                    else:
                        results.append(
                            {
                                "success": False,
                                "tweet_id": tid,
                                "error": "Tweet not found in batch response.",
                                **_empty_m,
                            }
                        )

            except tweepy.errors.TweepyException as exc:
                wait = self._handle_rate_limit(exc)
                if wait:
                    logger.warning(
                        "Rate-limited during batch metrics fetch. Retry after %d s.", wait
                    )

                logger.error("Batch metrics fetch failed: %s", exc, exc_info=True)
                _empty_e = {
                    "impressions": 0,
                    "like_count": 0,
                    "retweet_count": 0,
                    "reply_count": 0,
                    "bookmark_count": 0,
                }
                for tid in chunk:
                    results.append(
                        {"success": False, "tweet_id": tid, "error": str(exc), **_empty_e}
                    )

            except Exception as exc:
                logger.error(
                    "Unexpected error during batch metrics fetch: %s", exc, exc_info=True
                )
                _empty_ue = {
                    "impressions": 0,
                    "like_count": 0,
                    "retweet_count": 0,
                    "reply_count": 0,
                    "bookmark_count": 0,
                }
                for tid in chunk:
                    results.append(
                        {"success": False, "tweet_id": tid, "error": str(exc), **_empty_ue}
                    )

        return results


# ---------------------------------------------------------------------------
# Module-level singleton – import this instead of instantiating directly
# ---------------------------------------------------------------------------
twitter_agent = TwitterAgent()
