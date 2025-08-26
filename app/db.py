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
    """
    Legt Tabellen gemäß SQLAlchemy-Models an (nur neue Tabellen).
    Migriert KEINE bestehenden Tabellen – dafür gibt es ensure_min_schema().
    """
    from . import models  # noqa: F401
    Base.metadata.create_all(bind=engine)


def get_db():
    """FastAPI-Dependency für eine DB-Session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------
# Helpers für minimale, idempotente "Auto-Migration"
#  -> KEINE Bind-Parameter in DDL/Casts; alles als Literale/casts
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
    # Default-
