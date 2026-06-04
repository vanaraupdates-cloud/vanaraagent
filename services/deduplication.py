import hashlib
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).parent.parent))
from database import AsyncSessionLocal, ContentHash, Post


# ---------------------------------------------------------------------------
# Text normalisation helpers
# ---------------------------------------------------------------------------

def clean_for_hash(text: str) -> str:
    """
    Normalise *text* for hashing and similarity comparison.

    Steps applied (in order):
    1. Lowercase everything.
    2. Strip leading/trailing whitespace.
    3. Remove URLs  (http/https/bare www links).
    4. Remove Twitter/X @mentions.
    5. Remove hashtags.
    6. Collapse multiple whitespace characters into a single space.
    7. Final strip to tidy edges produced by the replacements above.
    """
    if not text:
        return ""

    # 1. Lowercase + strip
    cleaned = text.lower().strip()

    # 2. Remove URLs
    cleaned = re.sub(r"https?://\S+|www\.\S+", " ", cleaned)

    # 3. Remove @mentions
    cleaned = re.sub(r"@\w+", " ", cleaned)

    # 4. Remove hashtags (the # symbol and the tag word)
    cleaned = re.sub(r"#\w+", " ", cleaned)

    # 5. Collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    return cleaned


def get_content_hash(text: str) -> str:
    """
    Return a SHA-256 hex-digest of the normalised *text*.

    Normalisation strips punctuation (beyond what clean_for_hash already
    removed) so that minor punctuation differences don't produce different
    hashes for essentially identical content.
    """
    # First apply the standard cleaning pipeline
    normalised = clean_for_hash(text)

    # Additionally remove all remaining punctuation for hash purposes
    normalised = re.sub(r"[^\w\s]", "", normalised)

    # Collapse any whitespace introduced by the punctuation removal
    normalised = re.sub(r"\s+", " ", normalised).strip()

    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def is_duplicate_hash(content_hash: str) -> bool:
    """
    Check the *ContentHash* table for an existing record matching
    *content_hash*.

    Returns ``True`` if a matching hash is found, ``False`` otherwise.
    """
    try:
        async with AsyncSessionLocal() as session:
            stmt = select(ContentHash).where(ContentHash.hash == content_hash)
            result = await session.execute(stmt)
            existing = result.scalars().first()
            return existing is not None
    except Exception as exc:
        # Log and fail-open (treat as not duplicate) to avoid blocking posts
        print(f"[deduplication] is_duplicate_hash error: {exc}")
        return False


async def save_content_hash(content_hash: str, platform: str) -> None:
    """
    Persist *content_hash* for *platform* to the *ContentHash* table.

    If a record with the same hash already exists the operation is silently
    ignored to preserve idempotency.
    """
    try:
        async with AsyncSessionLocal() as session:
            # Guard against race-condition double-inserts
            stmt = select(ContentHash).where(ContentHash.hash == content_hash)
            result = await session.execute(stmt)
            if result.scalars().first() is not None:
                return  # Already stored – nothing to do

            new_record = ContentHash(
                hash=content_hash,
                platform=platform,
                created_at=datetime.utcnow(),
            )
            session.add(new_record)
            await session.commit()
    except Exception as exc:
        print(f"[deduplication] save_content_hash error: {exc}")


async def get_recent_posts_content(platform: str, days: int = 7) -> list[str]:
    """
    Fetch the text content of all *Post* rows for *platform* created within
    the last *days* days.

    Used as the reference corpus for TF-IDF similarity checking.
    Returns an empty list if no posts are found or on DB error.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        async with AsyncSessionLocal() as session:
            stmt = (
                select(Post.content)
                .where(Post.platform == platform)
                .where(Post.created_at >= cutoff)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            # Filter out any None / empty-string rows just in case
            return [row for row in rows if row and row.strip()]
    except Exception as exc:
        print(f"[deduplication] get_recent_posts_content error: {exc}")
        return []


# ---------------------------------------------------------------------------
# Similarity check
# ---------------------------------------------------------------------------

def is_near_duplicate(
    new_post: str,
    existing_posts: list[str],
    threshold: float = 0.82,
) -> bool:
    """
    Determine whether *new_post* is a near-duplicate of any post in
    *existing_posts* using TF-IDF cosine similarity.

    Returns ``True`` if the cosine similarity between *new_post* and **any**
    post in *existing_posts* exceeds *threshold*; ``False`` otherwise.

    Edge cases handled:
    - Empty *existing_posts* list → returns ``False`` (nothing to compare).
    - *new_post* is empty/whitespace → returns ``False``.
    - sklearn requires a minimum corpus of 2 documents; when only one existing
      post is available, *new_post* is temporarily appended to the corpus so
      that the vectoriser can be fitted, and the resulting similarity is still
      computed between *new_post* and the single existing post.
    """
    if not existing_posts:
        return False

    cleaned_new = clean_for_hash(new_post)
    if not cleaned_new:
        return False

    # Clean existing posts the same way for a fair comparison
    cleaned_existing = [clean_for_hash(p) for p in existing_posts]
    # Drop any that became empty after cleaning
    cleaned_existing = [p for p in cleaned_existing if p]

    if not cleaned_existing:
        return False

    # sklearn's TfidfVectorizer requires at least 2 documents in the corpus.
    # If we only have one existing post, add the new post to the fit corpus.
    corpus = cleaned_existing + [cleaned_new]

    try:
        vectoriser = TfidfVectorizer(
            analyzer="word",
            ngram_range=(1, 2),   # unigrams + bigrams for richer signal
            min_df=1,
            stop_words="english",
        )
        tfidf_matrix = vectoriser.fit_transform(corpus)

        # The new post is always the last document in the matrix
        new_vector = tfidf_matrix[-1]
        existing_vectors = tfidf_matrix[: len(cleaned_existing)]

        similarities = cosine_similarity(new_vector, existing_vectors).flatten()

        max_similarity = float(similarities.max()) if len(similarities) > 0 else 0.0

        return max_similarity >= threshold

    except Exception as exc:
        # On any sklearn error, fail-open (not a duplicate)
        print(f"[deduplication] is_near_duplicate error: {exc}")
        return False


# ---------------------------------------------------------------------------
# Unified duplicate check
# ---------------------------------------------------------------------------

async def check_duplicate(content: str, platform: str) -> tuple[bool, str]:
    """
    Run a full two-pass duplicate detection check for *content* on *platform*.

    Pass 1 – Exact hash check (fast, O(1) DB lookup):
        Computes the SHA-256 hash of the normalised content and queries the
        *ContentHash* table.  If a match is found, we return immediately.

    Pass 2 – Near-duplicate / semantic similarity check (slower):
        Fetches recent posts from the DB (last 7 days) and runs TF-IDF cosine
        similarity against the candidate content.  A similarity score above
        the default threshold (0.82) is treated as a near-duplicate.

    Returns:
        A ``(is_duplicate, reason)`` tuple where:
        - ``is_duplicate`` is ``True`` when a duplicate is detected.
        - ``reason`` is a human-readable string explaining the outcome.
    """
    # ------------------------------------------------------------------
    # Pass 1: exact hash check
    # ------------------------------------------------------------------
    content_hash = get_content_hash(content)

    try:
        hash_exists = await is_duplicate_hash(content_hash)
    except Exception as exc:
        print(f"[deduplication] check_duplicate – hash check error: {exc}")
        hash_exists = False

    if hash_exists:
        return (
            True,
            f"Exact duplicate detected (hash: {content_hash[:12]}…) "
            f"on platform '{platform}'.",
        )

    # ------------------------------------------------------------------
    # Pass 2: TF-IDF near-duplicate check against recent posts
    # ------------------------------------------------------------------
    try:
        recent_posts = await get_recent_posts_content(platform, days=7)
    except Exception as exc:
        print(f"[deduplication] check_duplicate – fetch recent posts error: {exc}")
        recent_posts = []

    if recent_posts:
        near_dup = is_near_duplicate(content, recent_posts, threshold=0.82)
        if near_dup:
            return (
                True,
                f"Near-duplicate detected via TF-IDF similarity "
                f"(threshold 0.82) against recent posts on platform '{platform}'.",
            )

    # ------------------------------------------------------------------
    # Not a duplicate – caller should now save the hash
    # ------------------------------------------------------------------
    return (
        False,
        "Content passed both exact-hash and near-duplicate checks; "
        "no duplicate found.",
    )
