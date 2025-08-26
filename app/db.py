# app/db.py
import os
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, declarative_base

# Railway stellt oft DATABASE_URL bereit; manchmal beginnt sie mit 'postgres://'
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL Umgebungsvariable ist nicht gesetzt.")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

Base = declarative_base()

def create_db_and_tables() -> None:
    # sicherstellen, dass Models geladen sind
    from . import models  # noqa: F401
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



def ensure_min_schema():
    # Legt policies an, falls sie fehlt, und rüstet fehlende Spalten nach.
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS policies (
            id SERIAL PRIMARY KEY,
            use_case VARCHAR NOT NULL,
            body JSON NOT NULL DEFAULT '{}'::json,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("ALTER TABLE policies ADD COLUMN IF NOT EXISTS body JSON NOT NULL DEFAULT '{}'::json;"))
        conn.execute(text("ALTER TABLE policies ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();"))

