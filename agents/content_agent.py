"""
agents/content_agent.py — AI Content Generation Engine
Generates LinkedIn posts using LLM
"""
import asyncio
import json
import logging
import random
import sys
import hashlib
from datetime import datetime, date
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (AI_PROVIDER, GEMINI_API_KEY, OPENAI_API_KEY,
                    OLLAMA_MODEL, OLLAMA_BASE_URL, DATA_DIR, DRY_RUN,
                    DAILY_LINKEDIN_LIMIT)
from database import AsyncSessionLocal, Article, Post, log_to_db
from services.deduplication import check_duplicate, save_content_hash, get_content_hash

logger = logging.getLogger(__name__)

# Load content library
CONTENT_LIBRARY_PATH = DATA_DIR / "content_library.json"
with open(CONTENT_LIBRARY_PATH, "r") as f:
    CONTENT_LIBRARY = json.load(f)

# ── LLM Client Setup ─────────────────────────────────────────

def get_llm_client():
    """Initialize the appropriate LLM client based on config."""
    if AI_PROVIDER == "gemini" and GEMINI_API_KEY:
        from google import genai
        return genai.Client(api_key=GEMINI_API_KEY)
    elif AI_PROVIDER == "openai" and OPENAI_API_KEY:
        from openai import AsyncOpenAI
        return AsyncOpenAI(api_key=OPENAI_API_KEY)
    else:
        logger.warning("No AI provider configured — using template fallback")
        return None


async def generate_with_llm(prompt: str, max_tokens: int = 500) -> str:
    """Call the configured LLM and return text response with retries and model fallbacks."""
    retries = 3
    backoff = 3.0  # base backoff in seconds

    for attempt in range(retries + 1):
        try:
            if AI_PROVIDER == "gemini" and GEMINI_API_KEY:
                from google import genai
                client = genai.Client(api_key=GEMINI_API_KEY)
                loop = asyncio.get_event_loop()
                
                # Dynamic model fallback chain to handle API quota restrictions
                candidate_models = ["gemini-flash-lite-latest", "gemini-3.1-flash-lite", "gemini-flash-latest"]
                last_err = None
                
                for model_name in candidate_models:
                    model_retries = 4
                    model_backoff = 4.0
                    for model_attempt in range(model_retries + 1):
                        try:
                            response = await loop.run_in_executor(
                                None,
                                lambda m=model_name: client.models.generate_content(
                                    model=m,
                                    contents=prompt
                                )
                            )
                            return response.text.strip()
                        except Exception as me:
                            last_err = me
                            err_str = str(me).lower()
                            is_transient = any(code in err_str for code in ["429", "resource_exhausted", "503", "unavailable"])
                            is_daily_limit = "generaterequestsperday" in err_str or "requests per day" in err_str
                            
                            if is_transient and not is_daily_limit and model_attempt < model_retries:
                                sleep_time = model_backoff * (2 ** model_attempt) + random.uniform(0.5, 1.5)
                                if "retry in" in err_str:
                                    try:
                                        import re
                                        match = re.search(r"retry in ([\d\.]+)s", err_str)
                                        if match:
                                            sleep_time = float(match.group(1)) + 1.0
                                    except Exception:
                                        pass
                                logger.warning(f"Model {model_name} hit rate-limit. Retrying in {sleep_time:.2f}s (Attempt {model_attempt+1}/{model_retries})...")
                                await asyncio.sleep(sleep_time)
                            else:
                                logger.warning(f"Model {model_name} failed: {me}. Moving to next candidate...")
                                break
                raise last_err

            elif AI_PROVIDER == "openai" and OPENAI_API_KEY:
                from openai import AsyncOpenAI
                client = AsyncOpenAI(api_key=OPENAI_API_KEY)
                response = await client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    temperature=0.85
                )
                return response.choices[0].message.content.strip()

            elif AI_PROVIDER == "ollama":
                import httpx
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"{OLLAMA_BASE_URL}/api/generate",
                        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
                        timeout=60.0
                    )
                    return response.json()["response"].strip()

            else:
                return generate_template_fallback(prompt)

        except Exception as e:
            err_str = str(e).lower()
            is_transient = any(code in err_str for code in ["429", "resource_exhausted", "503", "unavailable"])
            
            if is_transient and attempt < retries:
                # Calculate sleep with a minimum of the retry delay specified in error if possible
                sleep_time = backoff * (2 ** attempt) + random.uniform(0.5, 1.5)
                # Parse retryDelay from Google RPC error if present
                if "retry in" in err_str:
                    try:
                        import re
                        match = re.search(r"retry in ([\d\.]+)s", err_str)
                        if match:
                            sleep_time = float(match.group(1)) + 1.0
                    except Exception:
                        pass
                logger.warning(f"Gemini transient error/rate-limit hit ({e}). Retrying in {sleep_time:.2f} seconds (Attempt {attempt+1}/{retries})...")
                await asyncio.sleep(sleep_time)
            else:
                logger.error(f"LLM generation failed: {e}")
                return generate_template_fallback(prompt)

    return generate_template_fallback(prompt)


def generate_template_fallback(prompt: str) -> str:
    """Fallback generator that strictly follows the user's unified format."""
    import random
    import re

    # Helper to extract value between quotes or after a keyword
    def extract_field(pattern, text):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip().replace('"', '')
        return ""

    title = extract_field(r"(?:Title|TITLE):\s*(.*?)(?:\n|$)", prompt)
    summary = extract_field(r"(?:Summary|SUMMARY):\s*(.*?)(?:\n|$)", prompt)
    company = extract_field(r"(?:Company|COMPANY):\s*(.*?)(?:\n|$)", prompt)

    if not title:
        title = "AI Model Architecture Breakthrough"
    if not company:
        company = "AI Research Group"
    if not summary:
        summary = "enables significantly better performance and execution accuracy across benchmarks."

    # Hooks library
    hooks = [
        "Big Shift in AI Architecture.",
        "Major Breakthrough in AI Agents.",
        "The New AI Standard Is Here.",
        "Next-Gen AI Systems Evolve.",
        "A Massive Leap in Model Capability.",
        "This Changes AI Workflow Forever."
    ]
    hook = random.choice(hooks)

    # Bullet points
    bullets_pool = [
        [
            "Better execution accuracy",
            "Fewer errors and hallucinations",
            "More reliable workflow integration",
            "Stronger production-ready systems"
        ],
        [
            "Faster inference latency",
            "Lower operational compute cost",
            "Seamless multi-step reasoning",
            "Enhanced developer productivity"
        ],
        [
            "Highly accurate tool-calling",
            "Robust decision-making pathways",
            "Optimized context-window usage",
            "Scalable enterprise deployments"
        ]
    ]
    bullets = random.choice(bullets_pool)

    # Takeaway
    takeaways = [
        "The next AI advantage isn't bigger models — it's better execution.",
        "Scale is no longer the bottleneck; system optimization is.",
        "The future of software is agentic, and reliability is the currency.",
        "True progress is measured by utility, not parameter count."
    ]
    takeaway = random.choice(takeaways)

    # Clean up company name for hashtags
    hashtag_company = "".join(e for e in company if e.isalnum())
    
    # 10 Hashtags
    hashtags = f"#AI #AIAgents #{hashtag_company} #MachineLearning #LLM #GenerativeAI #AIEngineering #TechNews #Innovation #TechUpdates"

    # Build the template
    lines = [
        hook,
        "",
        f"{company} now {title.lower()} to {summary.lower().rstrip('.')}.",
        "",
        "What this means:",
        f"• {bullets[0]}",
        f"• {bullets[1]}",
        f"• {bullets[2]}",
        f"• {bullets[3]}",
        "",
        takeaway,
        "",
        hashtags,
        "",
        "For more updates → @vanaraupdates"
    ]
    return "\n".join(lines)


# ── Prompt Building ──────────────────────────────────────────

def get_used_hooks_today() -> list[str]:
    """Track used hooks within this session to avoid repetition."""
    return getattr(get_used_hooks_today, '_used', [])

def mark_hook_used(hook: str):
    if not hasattr(get_used_hooks_today, '_used'):
        get_used_hooks_today._used = []
    get_used_hooks_today._used.append(hook)

def get_random_hook(hook_type: str = None) -> str:
    """Get a random hook, avoiding ones used today."""
    all_hooks = []
    lib = CONTENT_LIBRARY["hooks"]
    if hook_type and hook_type in lib:
        all_hooks = lib[hook_type]
    else:
        for hooks in lib.values():
            all_hooks.extend(hooks)

    used = get_used_hooks_today()
    available = [h for h in all_hooks if h not in used]
    if not available:
        available = all_hooks  # Reset if all used

    hook = random.choice(available)
    mark_hook_used(hook)
    return hook

def get_random_cta(platform: str = "twitter") -> str:
    """Get a random CTA for the platform."""
    ctas = CONTENT_LIBRARY["ctas"].get(platform, CONTENT_LIBRARY["ctas"]["twitter"])
    return random.choice(ctas)

def build_unified_post_prompt(article: dict, hook: str) -> str:
    hashtag_company = "".join(e for e in article.get('company', 'AI') if e.isalnum())
    return f"""You are an expert AI news journalist writing for Twitter/X and LinkedIn. Write a high-quality post about this AI news story.

NEWS STORY:
Title: {article.get('title', '')}
Summary: {article.get('summary', '')}
Company: {article.get('company', 'AI')}

FORMAT REQUIREMENTS:
Your response MUST strictly follow this exact template structure, including the empty lines:

[Hook Line]

[A 1-sentence description of the core news/change]

What this means:
• [Implication/Benefit 1]
• [Implication/Benefit 2]
• [Implication/Benefit 3]
• [Implication/Benefit 4]

[A strong, punchy takeaway or conclusion sentence]

#AI #AIAgents #{hashtag_company} #MachineLearning #LLM #GenerativeAI #AIEngineering #TechNews #Innovation #TechUpdates

For more updates → @vanaraupdates

RULES:
- Hook Line: 4-6 words, bold style (e.g. "{hook}" or similar) ending with a period.
- Bullet points: Use the bullet character '•' (Unicode U+2022). Keep each bullet to 4-7 words.
- Takeaway line: A single powerful sentence summarizing the larger trend or context.
- Hashtags: Exactly the 10 hashtags listed above. Do not include spaces within hashtags.
- Outro: Exactly the text "For more updates → @vanaraupdates".
- Total character count should be under 700 characters.
- Output ONLY the formatted post. Do not include markdown code block syntax (like ```) or any preamble."""


# ── Main Content Generation ──────────────────────────────────

async def generate_all_posts(articles: list[dict], cycle: str = "morning", target_count: int = None) -> dict:
    """
    Master function — generates LinkedIn posts in the unified template format.
    Returns dict with linkedin_posts list.
    """
    if target_count is None:
        target_count = DAILY_LINKEDIN_LIMIT

    logger.info(f"🧠 Starting content generation for {cycle} cycle ({len(articles)} articles)")
    await log_to_db("content_agent", "INFO", f"Content generation started — {len(articles)} source articles")

    if not articles:
        logger.warning("No articles to generate from — using dummy data")
        articles = [{
            "id": 1,
            "title": "Amazon SageMaker AI now enables better AI agent tool-calling accuracy",
            "summary": "Amazon SageMaker AI enables better AI agent tool-calling accuracy using SFT (Supervised Fine-Tuning) and DPO (Direct Preference Optimization).",
            "company": "Amazon SageMaker",
            "total_score": 90
        }]

    # Sort by score, best articles first
    articles = sorted(articles, key=lambda x: x.get("total_score", 0), reverse=True)

    linkedin_posts = []

    # Reset hook tracker for new day
    get_used_hooks_today._used = []

    async def make_post(content: str, platform: str, post_type: str, article_id: Optional[int] = None) -> Optional[dict]:
        """Dedup check and save a single post."""
        is_dup, reason = await check_duplicate(content, platform)
        if is_dup:
            logger.warning(f"Duplicate detected ({reason}): {content[:50]}...")
            return None

        # Scope the hash uniquely to platform to avoid DB unique constraint conflicts
        content_hash = hashlib.sha256(f"{platform}:{content}".encode('utf-8')).hexdigest()
        await save_content_hash(content_hash, platform)

        return {
            "content": content,
            "platform": platform,
            "post_type": post_type,
            "content_hash": content_hash,
            "source_article_id": article_id,
            "thread_id": None,
            "thread_position": None,
            "thread_total": None,
            "generation_cycle": cycle,
            "status": "pending"
        }

    logger.info(f"Generating unified posts (target={target_count})...")
    post_count = 0
    article_idx = 0
    
    # Try generating until we have target_count posts or run out of attempts
    max_attempts = 150
    attempts = 0
    
    while post_count < target_count and attempts < max_attempts:
        attempts += 1
        art = articles[article_idx % len(articles)]
        article_idx += 1
        
        hook = get_random_hook()
        prompt = build_unified_post_prompt(art, hook)
        content = await generate_with_llm(prompt, max_tokens=400)
        
        # Validation rules: check if format markers are present, otherwise use fallback
        valid = (
            "What this means:" in content and
            "•" in content and
            "For more updates → @vanaraupdates" in content
        )
        
        if not valid:
            logger.warning(f"LLM output failed format validation, using fallback for: {art['title']}")
            content = generate_template_fallback(prompt)
            
        li_post = await make_post(content, "linkedin", "unified", art.get("id"))
        
        if li_post:
            linkedin_posts.append(li_post)
            post_count += 1
            logger.info(f"Generated unified post {post_count}/{target_count} successfully.")
        else:
            logger.warning("Post was flagged as duplicate, skipping.")
            
        await asyncio.sleep(0.5)  # Gentle API spacing

    logger.info(f"✅ Content generation complete. Generated {len(linkedin_posts)} LinkedIn posts")
    await log_to_db("content_agent", "SUCCESS", f"Generated {len(linkedin_posts)} LinkedIn posts")

    return {
        "linkedin": linkedin_posts,
        "generated_at": datetime.utcnow().isoformat(),
        "cycle": cycle
    }


async def save_posts_to_db(posts_data: dict) -> int:
    """Save generated posts to the database. Returns linkedin_count."""
    linkedin_saved = 0

    async with AsyncSessionLocal() as session:
        # Clean up existing pending/failed/skipped posts for today first to avoid duplication
        from sqlalchemy import delete, or_, and_
        from datetime import timezone as dt_timezone, timedelta as dt_timedelta
        ist_tz = dt_timezone(dt_timedelta(hours=5, minutes=30))
        ist_now = datetime.now(ist_tz)
        today = ist_now.date()
        today_start = datetime(today.year, today.month, today.day)
        today_end = today_start + dt_timedelta(days=1)
        
        await session.execute(
            delete(Post).where(
                Post.platform == "linkedin",
                Post.status.in_(["pending", "failed", "skipped"]),
                or_(
                    and_(Post.scheduled_at >= today_start, Post.scheduled_at < today_end),
                    and_(Post.scheduled_at.is_(None), Post.created_at >= today_start, Post.created_at < today_end)
                )
            )
        )

        for post_data in posts_data.get("linkedin", []):
            post = Post(**{k: v for k, v in post_data.items()
                          if k in Post.__table__.columns.keys()})
            session.add(post)
            linkedin_saved += 1

        try:
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Failed to save posts: {e}")

    return linkedin_saved


if __name__ == "__main__":
    # Test with dummy articles
    test_articles = [
        {
            "id": 1,
            "title": "Anthropic releases Claude 4 with 200K context window",
            "summary": "Anthropic's newest model features unprecedented context length and improved reasoning capabilities, targeting enterprise customers.",
            "company": "Anthropic",
            "total_score": 92
        },
        {
            "id": 2,
            "title": "OpenAI launches GPT-5 with multimodal capabilities",
            "summary": "OpenAI's latest flagship model processes text, images, audio, and video natively, representing a significant leap forward.",
            "company": "OpenAI",
            "total_score": 95
        },
        {
            "id": 3,
            "title": "Mistral releases new code-focused model",
            "summary": "Mistral's new coding model outperforms competitors on HumanEval benchmarks while running on consumer hardware.",
            "company": "Mistral AI",
            "total_score": 78
        }
    ]

    async def test():
        posts = await generate_all_posts(test_articles, "morning")
        print(f"[OK] LinkedIn posts: {len(posts['linkedin'])}")
        if posts['linkedin']:
            print(f"\nSample LinkedIn post:\n{posts['linkedin'][0]['content']}")

    asyncio.run(test())
