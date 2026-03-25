from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import psycopg


@dataclass(frozen=True)
class ProgressEvent:
    telegram_user_id: int
    telegram_username: Optional[str]
    full_name: Optional[str]
    week_number: int
    status: str  # done | partial | not_yet
    created_at_iso: str


def is_postgres_url(database_url: Optional[str]) -> bool:
    return bool(database_url) and database_url.startswith(("postgres://", "postgresql://"))


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def connect_postgres(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url)


def init_db_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS progress_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_user_id INTEGER NOT NULL,
            telegram_username TEXT,
            full_name TEXT,
            week_number INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at_iso TEXT NOT NULL
        )
        """
    )
    conn.commit()


def init_db_postgres(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS progress_events (
                id BIGSERIAL PRIMARY KEY,
                telegram_user_id BIGINT NOT NULL,
                telegram_username TEXT,
                full_name TEXT,
                week_number INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at_iso TEXT NOT NULL
            )
            """
        )
    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    # Backwards compatibility for existing sqlite usage.
    init_db_sqlite(conn)


def insert_progress_event(
    conn: sqlite3.Connection,
    *,
    telegram_user_id: int,
    telegram_username: Optional[str],
    full_name: Optional[str],
    week_number: int,
    status: str,
    now: Optional[datetime] = None,
) -> ProgressEvent:
    if now is None:
        now = datetime.utcnow()
    created_at_iso = now.replace(microsecond=0).isoformat() + "Z"

    conn.execute(
        """
        INSERT INTO progress_events (
            telegram_user_id, telegram_username, full_name,
            week_number, status, created_at_iso
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (telegram_user_id, telegram_username, full_name, week_number, status, created_at_iso),
    )
    conn.commit()

    return ProgressEvent(
        telegram_user_id=telegram_user_id,
        telegram_username=telegram_username,
        full_name=full_name,
        week_number=week_number,
        status=status,
        created_at_iso=created_at_iso,
    )


def insert_progress_event_postgres(
    conn: psycopg.Connection,
    *,
    telegram_user_id: int,
    telegram_username: Optional[str],
    full_name: Optional[str],
    week_number: int,
    status: str,
    now: Optional[datetime] = None,
) -> ProgressEvent:
    if now is None:
        now = datetime.utcnow()
    created_at_iso = now.replace(microsecond=0).isoformat() + "Z"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO progress_events (
                telegram_user_id, telegram_username, full_name,
                week_number, status, created_at_iso
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (telegram_user_id, telegram_username, full_name, week_number, status, created_at_iso),
        )
    conn.commit()

    return ProgressEvent(
        telegram_user_id=telegram_user_id,
        telegram_username=telegram_username,
        full_name=full_name,
        week_number=week_number,
        status=status,
        created_at_iso=created_at_iso,
    )

