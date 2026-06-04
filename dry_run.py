"""
dry_run.py — Test the full pipeline without any API calls
Run: python dry_run.py
"""
import asyncio
import sys
import os
from pathlib import Path

# Force dry run mode
os.environ["DRY_RUN"] = "true"
os.environ["AI_PROVIDER"] = "gemini"

sys.path.insert(0, str(Path(__file__).parent))

async def main():
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import print as rprint

    console = Console()

    console.print("\n[bold violet]╔══════════════════════════════════════╗[/]")
    console.print("[bold violet]║   AI NEWS MEDIA AGENT — DRY RUN    ║[/]")
    console.print("[bold violet]╚══════════════════════════════════════╝[/]\n")

    # Step 1: Init DB
    console.print("[bold]Step 1:[/] Initializing database...")
    from database import init_db
    await init_db()
    console.print("[green]✅ Database initialized[/]")

    # Step 2: Seed companies
    console.print("\n[bold]Step 2:[/] Seeding company database...")
    from main import seed_companies_if_needed
    await seed_companies_if_needed()
    console.print("[green]✅ Companies loaded[/]")

    # Step 3: Research cycle
    console.print("\n[bold]Step 3:[/] Running research cycle (real RSS feeds, no auth needed)...")
    try:
        from agents.research_agent import run_research_cycle
        await run_research_cycle(cycle="morning")
        console.print("[green]✅ Research complete[/]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Research partial (expected without Reddit API): {e}[/]")

    # Step 4: Load articles and generate content
    console.print("\n[bold]Step 4:[/] Generating content (requires AI API key)...")
    from database import AsyncSessionLocal, Article
    from sqlalchemy import select

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Article).order_by(Article.total_score.desc()).limit(20)
        )
        articles_db = result.scalars().all()

    if not articles_db:
        console.print("[yellow]⚠️ No articles from research — using test data[/]")
        articles = [
            {"id": 1, "title": "OpenAI releases GPT-5 with unprecedented capabilities", "summary": "The new model outperforms all benchmarks", "company": "OpenAI", "total_score": 95},
            {"id": 2, "title": "Anthropic launches Claude 4 with 1M context window", "summary": "Massive context window enables new use cases", "company": "Anthropic", "total_score": 91},
            {"id": 3, "title": "Mistral releases new code model beating Copilot", "summary": "Open source model tops coding benchmarks", "company": "Mistral AI", "total_score": 82},
            {"id": 4, "title": "Google DeepMind achieves AGI milestone in chess variant", "summary": "New reasoning capabilities emerge from RLHF training", "company": "Google DeepMind", "total_score": 78},
            {"id": 5, "title": "Meta releases Llama 4 with multimodal capabilities", "summary": "Open source model now handles images, audio, and video", "company": "Meta AI", "total_score": 88},
        ]
    else:
        articles = [{"id": a.id, "title": a.title, "summary": a.summary or "", "company": "AI", "total_score": a.total_score} for a in articles_db]
        console.print(f"[green]✅ Loaded {len(articles)} articles from research[/]")

    try:
        from agents.content_agent import generate_all_posts, save_posts_to_db
        posts_data = await generate_all_posts(articles, cycle="morning")
        twitter_saved, linkedin_saved = await save_posts_to_db(posts_data)

        # Print summary table
        table = Table(title="Generated Posts Summary", style="bold")
        table.add_column("Platform", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Count", style="green", justify="right")

        type_counts = {}
        for p in posts_data["twitter"]:
            t = p.get("post_type", "unknown")
            type_counts[f"Twitter/{t}"] = type_counts.get(f"Twitter/{t}", 0) + 1
        for p in posts_data["linkedin"]:
            t = p.get("post_type", "unknown")
            type_counts[f"LinkedIn/{t}"] = type_counts.get(f"LinkedIn/{t}", 0) + 1

        for key, count in sorted(type_counts.items()):
            platform, ptype = key.split("/", 1)
            table.add_row(platform, ptype, str(count))

        table.add_row("[bold]TOTAL Twitter[/]", "", f"[bold]{twitter_saved}[/]")
        table.add_row("[bold]TOTAL LinkedIn[/]", "", f"[bold]{linkedin_saved}[/]")
        console.print(table)

        # Print sample posts
        if posts_data["twitter"]:
            console.print(Panel(
                posts_data["twitter"][0]["content"],
                title="[bold cyan]Sample Twitter Post[/]",
                border_style="cyan"
            ))
        if posts_data["linkedin"]:
            console.print(Panel(
                posts_data["linkedin"][0]["content"][:500] + "..." if len(posts_data["linkedin"][0]["content"]) > 500 else posts_data["linkedin"][0]["content"],
                title="[bold blue]Sample LinkedIn Post[/]",
                border_style="blue"
            ))

    except Exception as e:
        console.print(f"[red]❌ Content generation failed: {e}[/]")
        console.print("[yellow]  → Make sure your GEMINI_API_KEY or OPENAI_API_KEY is set in .env[/]")
        import traceback
        traceback.print_exc()

    # Step 5: Test scheduler (just generate schedule, don't actually publish)
    console.print("\n[bold]Step 5:[/] Testing schedule generation...")
    from scheduler import generate_dynamic_schedule
    from config import TWITTER_WINDOW_START, TWITTER_WINDOW_END

    schedule = generate_dynamic_schedule(TWITTER_WINDOW_START, TWITTER_WINDOW_END, 30)
    gaps = [(schedule[i+1] - schedule[i]).seconds // 60 for i in range(len(schedule)-1)]

    console.print(f"[green]✅ Generated {len(schedule)} post times[/]")
    console.print(f"   Intervals: min={min(gaps)}min, max={max(gaps)}min, avg={sum(gaps)//len(gaps)}min")
    console.print(f"   First: {schedule[0].strftime('%H:%M')} → Last: {schedule[-1].strftime('%H:%M')}")

    console.print("\n[bold green]╔══════════════════════════════════════╗[/]")
    console.print("[bold green]║   DRY RUN COMPLETE — System Ready!  ║[/]")
    console.print("[bold green]╚══════════════════════════════════════╝[/]")
    console.print("\n[bold]Next steps:[/]")
    console.print("  1. Copy .env.example to .env and add your API keys")
    console.print("  2. Run [bold cyan]python main.py[/] or double-click [bold]START.bat[/]")
    console.print("  3. Open [bold cyan]http://localhost:3000[/] in your browser\n")


if __name__ == "__main__":
    asyncio.run(main())
