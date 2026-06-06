import sys
from pathlib import Path
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent))
from database import DATABASE_SYNC_URL

def check_posts():
    engine = create_engine(DATABASE_SYNC_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, status, platform, platform_post_id FROM posts WHERE id IN (24, 25, 26, 27)"))
        for row in result.fetchall():
            print(dict(zip(result.keys(), row)))

if __name__ == "__main__":
    check_posts()
