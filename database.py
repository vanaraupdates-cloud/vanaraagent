"""
database.py — SQLAlchemy async models + engine setup
"""
import asyncio
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, Float, DateTime,
    Boolean, ForeignKey, create_engine
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.pool import StaticPool
from config import DATABASE_URL, DATABASE_SYNC_URL

Base = declarative_base()

# ── Models ───────────────────────────────────────────────────

class Company(Base):
    __tablename__ = "companies"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    name           = Column(String(200), nullable=False)
    domain         = Column(String(200))
    twitter_handle = Column(String(100))
    linkedin_url   = Column(String(300))
    category       = Column(String(100))  # LLM | Agent | Infrastructure | Tools | Research
    tier           = Column(Integer, default=1)  # 1=top, 2=mid, 3=emerging
    active         = Column(Boolean, default=True)
    created_at     = Column(DateTime, default=datetime.utcnow)

    articles = relationship("Article", back_populates="company")


class Article(Base):
    __tablename__ = "articles"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    title            = Column(Text, nullable=False)
    url              = Column(String(1000), unique=True, nullable=False)
    source           = Column(String(200))
    company_id       = Column(Integer, ForeignKey("companies.id"), nullable=True)
    summary          = Column(Text)
    full_text        = Column(Text)
    virality_score   = Column(Float, default=0.0)
    business_score   = Column(Float, default=0.0)
    relevance_score  = Column(Float, default=0.0)
    trend_score      = Column(Float, default=0.0)
    total_score      = Column(Float, default=0.0)
    fetched_at       = Column(DateTime, default=datetime.utcnow)
    cycle            = Column(String(20), default="morning")  # morning | afternoon
    used             = Column(Boolean, default=False)

    company  = relationship("Company", back_populates="articles")
    posts    = relationship("Post", back_populates="source_article")


class Post(Base):
    __tablename__ = "posts"
    id                 = Column(Integer, primary_key=True, autoincrement=True)
    content            = Column(Text, nullable=False)
    platform           = Column(String(20), nullable=False)   # twitter | linkedin
    post_type          = Column(String(50))                   # short | medium | thread | engagement | insight | ...
    status             = Column(String(20), default="pending")# pending | posted | failed | skipped | editing
    scheduled_at       = Column(DateTime)
    posted_at          = Column(DateTime)
    content_hash       = Column(String(64), unique=True)
    source_article_id  = Column(Integer, ForeignKey("articles.id"), nullable=True)
    platform_post_id   = Column(String(200))
    thread_id          = Column(Integer, nullable=True)       # groups thread tweets
    thread_position    = Column(Integer, nullable=True)       # position in thread
    thread_total       = Column(Integer, nullable=True)       # total tweets in thread
    generation_cycle   = Column(String(20))                   # morning | afternoon
    created_at         = Column(DateTime, default=datetime.utcnow)
    error_message      = Column(Text)

    source_article = relationship("Article", back_populates="posts")
    analytics      = relationship("PostAnalytics", back_populates="post", uselist=False)


class PostAnalytics(Base):
    __tablename__ = "analytics"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    post_id     = Column(Integer, ForeignKey("posts.id"), unique=True)
    impressions = Column(Integer, default=0)
    likes       = Column(Integer, default=0)
    reposts     = Column(Integer, default=0)
    replies     = Column(Integer, default=0)
    bookmarks   = Column(Integer, default=0)
    link_clicks = Column(Integer, default=0)
    reach       = Column(Integer, default=0)
    comments    = Column(Integer, default=0)
    shares      = Column(Integer, default=0)
    updated_at  = Column(DateTime, default=datetime.utcnow)

    post = relationship("Post", back_populates="analytics")


class ContentHash(Base):
    __tablename__ = "content_hashes"
    hash       = Column(String(64), primary_key=True)
    platform   = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)


class LearningPreference(Base):
    __tablename__ = "learning_preferences"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    date            = Column(String(10))   # YYYY-MM-DD
    platform        = Column(String(20))
    best_hook_type  = Column(String(100))
    best_post_time  = Column(String(10))
    best_topic      = Column(String(200))
    best_length     = Column(Integer)
    best_cta        = Column(Text)
    avg_engagement  = Column(Float, default=0.0)
    top_company     = Column(String(200))
    notes           = Column(Text)
    created_at      = Column(DateTime, default=datetime.utcnow)


class SystemLog(Base):
    __tablename__ = "system_logs"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    level      = Column(String(10))   # INFO | WARNING | ERROR | SUCCESS
    module     = Column(String(100))
    message    = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


# ── Engine ───────────────────────────────────────────────────

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False},
)

AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Sync engine for APScheduler jobstore
sync_engine = create_engine(
    DATABASE_SYNC_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

# SQLite pragmas to enable WAL mode for concurrency
from sqlalchemy import event

@event.listens_for(engine.sync_engine, "connect")
def set_sqlite_pragma_async(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    finally:
        cursor.close()

@event.listens_for(sync_engine, "connect")
def set_sqlite_pragma_sync(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    finally:
        cursor.close()


async def init_db():
    """Create all tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db():
    """Dependency for FastAPI routes."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def log_to_db(module: str, level: str, message: str):
    """Quick helper to log events to DB (for dashboard display)."""
    async with AsyncSessionLocal() as session:
        entry = SystemLog(level=level, module=module, message=message)
        session.add(entry)
        await session.commit()
