"""
agents/content_agent.py — AI Content Generation Engine
Generates all 30 Twitter + 10 LinkedIn posts using LLM
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
                    OLLAMA_MODEL, OLLAMA_BASE_URL, DATA_DIR, DRY_RUN)
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
    """Call the configured LLM and return text response."""
    try:
        if AI_PROVIDER == "gemini" and GEMINI_API_KEY:
            from google import genai
            client = genai.Client(api_key=GEMINI_API_KEY)
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt
                )
            )
            return response.text.strip()

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
        logger.error(f"LLM generation failed: {e}")
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

async def generate_all_posts(articles: list[dict], cycle: str = "morning") -> dict:
    """
    Master function — generates exactly 30 Twitter + 30 LinkedIn posts in the unified template format.
    Returns dict with twitter_posts list and linkedin_posts list.
    """
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

    twitter_posts = []
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

    # Generate exactly 30 posts
    logger.info("Generating 30 unified posts for Twitter and LinkedIn...")
    post_count = 0
    article_idx = 0
    
    # Try generating until we have 30 posts or run out of attempts
    max_attempts = 150
    attempts = 0
    
    while post_count < 30 and attempts < max_attempts:
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
            
        # Try to make both Twitter and LinkedIn posts (deduped independently)
        tw_post = await make_post(content, "twitter", "unified", art.get("id"))
        li_post = await make_post(content, "linkedin", "unified", art.get("id"))
        
        if tw_post and li_post:
            twitter_posts.append(tw_post)
            linkedin_posts.append(li_post)
            post_count += 1
            logger.info(f"Generated unified post {post_count}/30 successfully.")
        else:
            logger.warning("Post was flagged as duplicate for one or more platforms, skipping.")
            
        await asyncio.sleep(0.5)  # Gentle API spacing

    logger.info(f"✅ Content generation complete. Generated {len(twitter_posts)} Twitter + {len(linkedin_posts)} LinkedIn posts")
    await log_to_db("content_agent", "SUCCESS", f"Generated {len(twitter_posts)} Twitter + {len(linkedin_posts)} LinkedIn posts")

    return {
        "twitter": twitter_posts,
        "linkedin": linkedin_posts,
        "generated_at": datetime.utcnow().isoformat(),
        "cycle": cycle
    }


async def save_posts_to_db(posts_data: dict) -> tuple[int, int]:
    """Save generated posts to the database. Returns (twitter_count, linkedin_count)."""
    twitter_saved = 0
    linkedin_saved = 0

    async with AsyncSessionLocal() as session:
        # Clean up existing pending/failed/skipped posts for today first to avoid duplication
        from sqlalchemy import delete
        from datetime import timedelta
        today = date.today()
        today_start = datetime(today.year, today.month, today.day)
        today_end = today_start + timedelta(days=1)
        
        await session.execute(
            delete(Post).where(
                Post.created_at >= today_start,
                Post.created_at < today_end,
                Post.status.in_(["pending", "failed", "skipped"])
            )
        )

        for post_data in posts_data.get("twitter", []):
            post = Post(**{k: v for k, v in post_data.items()
                          if k in Post.__table__.columns.keys()})
            session.add(post)
            twitter_saved += 1

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

    return twitter_saved, linkedin_saved


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
        print(f"\n[OK] Twitter posts: {len(posts['twitter'])}")
        print(f"[OK] LinkedIn posts: {len(posts['linkedin'])}")
        if posts['twitter']:
            print(f"\nSample Twitter post:\n{posts['twitter'][0]['content']}")
        if posts['linkedin']:
            print(f"\nSample LinkedIn post:\n{posts['linkedin'][0]['content']}")

    asyncio.run(test())
