# app/db.py
from __future__ import annotations

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------------------------------------------------------------------
# Engine / Session / Base
# ---------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL Umgebungsvariable ist nicht gesetzt.")
# Railway liefert teils 'postgres://', SQLAlchemy erwartet 'postgresql://'
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
Base = declarative_base()


def create_db_and_tables() -> None:
    """legt neue Tabellen an (migriert nicht bestehende)."""
    from . import models  # noqa: F401
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------
# Mini-"Auto-Migration" (idempotent, ohne Bind-Parameter in DDL)
# ---------------------------------------------------------------------
def _column_exists(conn, table: str, column: str) -> bool:
    row = conn.execute(
        text("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = :t AND column_name = :c
            LIMIT 1
        """),
        {"t": table, "c": column},
    ).first()
    return row is not None


def _add_json_column_if_missing(conn, table: str, column: str):
    if not _column_exists(conn, table, column):
        conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {column} JSON'))
    conn.execute(text(f"UPDATE {table} SET {column} = '{{}}'::json WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT '{{}}'::json"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))


def _add_timestamptz_column_if_missing(conn, table: str, column: str):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} TIMESTAMPTZ"))
    conn.execute(text(f"UPDATE {table} SET {column} = NOW() WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT NOW()"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))


def _add_float_column_if_missing(conn, table: str, column: str, default: float = 0.0):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} DOUBLE PRECISION"))
    conn.execute(text(f"UPDATE {table} SET {column} = {default} WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT {default}"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))


def _add_varchar_column_if_missing(conn, table: str, column: str, default: str = ""):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} VARCHAR"))
    lit = default.replace("'", "''")
    conn.execute(text(f"UPDATE {table} SET {column} = '{lit}' WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT '{lit}'"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))


def _drop_column_if_exists(conn, table: str, column: str):
    if _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {column}"))


def ensure_min_schema():
    """
    Heilt Schema-Drift für PoC-Tabellen:
      - policies: body(JSON), created_at(TIMESTAMPTZ), DROP definition
      - usage_events: quantity(FLOAT), unit(VARCHAR 'kWh'), meta(JSON), timestamp(TIMESTAMPTZ)
    """
    with engine.begin() as conn:
        # policies minimal sichern
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS policies (
                id SERIAL PRIMARY KEY,
                use_case VARCHAR NOT NULL
            );
        """))
        _add_json_column_if_missing(conn,  "policies", "body")
        _add_timestamptz_column_if_missing(conn, "policies", "created_at")
        _drop_column_if_exists(conn, "policies", "definition")

        # usage_events fehlende Spalten nachrüsten
        _add_float_column_if_missing(conn,   "usage_events", "quantity", 0.0)
        _add_varchar_column_if_missing(conn, "usage_events", "unit", "kWh")
        if not _column_exists(conn, "usage_events", "meta"):
            conn.execute(text("ALTER TABLE usage_events ADD COLUMN meta JSON"))
        _add_timestamptz_column_if_missing(conn, "usage_events", "timestamp")
