from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import psycopg


@dataclass(frozen=True)
class ProgressEvent:
    telegram_user_id: int
    telegram_username: Optional[str]
    full_name: Optional[str]
    week_number: int
    status: str  # done | partial | not_yet
    created_at_iso: str


@dataclass(frozen=True)
class MonthlyWeeklyPlan:
    month: str
    week_number: int
    start_page: int
    end_page: int
    summary: str
    encouragement: str
    scheduled_date: str
    sent_at_iso: Optional[str]


@dataclass(frozen=True)
class WeeklyProgressStat:
    status: str
    count: int


@dataclass(frozen=True)
class WeeklyProgressMember:
    telegram_user_id: int
    telegram_username: Optional[str]
    full_name: Optional[str]
    status: str
    updated_at_iso: str


@dataclass(frozen=True)
class Bookmark:
    id: int
    telegram_user_id: int
    telegram_username: Optional[str]
    full_name: Optional[str]
    page: Optional[int]
    text: str
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_user_id INTEGER NOT NULL,
            telegram_username TEXT,
            full_name TEXT,
            page INTEGER,
            text TEXT NOT NULL,
            created_at_iso TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS club_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at_iso TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS club_month_settings (
            month TEXT NOT NULL,           -- YYYY-MM
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            updated_at_iso TEXT NOT NULL,
            PRIMARY KEY (month, key)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_weekly_plans (
            month TEXT NOT NULL,
            week_number INTEGER NOT NULL,
            start_page INTEGER NOT NULL,
            end_page INTEGER NOT NULL,
            summary TEXT NOT NULL,
            encouragement TEXT NOT NULL,
            scheduled_date TEXT NOT NULL,
            sent_at_iso TEXT,
            PRIMARY KEY (month, week_number)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS weekly_progress_status (
            month TEXT NOT NULL,
            week_number INTEGER NOT NULL,
            telegram_user_id INTEGER NOT NULL,
            telegram_username TEXT,
            full_name TEXT,
            status TEXT NOT NULL,
            updated_at_iso TEXT NOT NULL,
            PRIMARY KEY (month, week_number, telegram_user_id)
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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bookmarks (
                id BIGSERIAL PRIMARY KEY,
                telegram_user_id BIGINT NOT NULL,
                telegram_username TEXT,
                full_name TEXT,
                page INTEGER,
                text TEXT NOT NULL,
                created_at_iso TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS club_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at_iso TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS club_month_settings (
                month TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at_iso TEXT NOT NULL,
                PRIMARY KEY (month, key)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_weekly_plans (
                month TEXT NOT NULL,
                week_number INTEGER NOT NULL,
                start_page INTEGER NOT NULL,
                end_page INTEGER NOT NULL,
                summary TEXT NOT NULL,
                encouragement TEXT NOT NULL,
                scheduled_date TEXT NOT NULL,
                sent_at_iso TEXT,
                PRIMARY KEY (month, week_number)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_progress_status (
                month TEXT NOT NULL,
                week_number INTEGER NOT NULL,
                telegram_user_id BIGINT NOT NULL,
                telegram_username TEXT,
                full_name TEXT,
                status TEXT NOT NULL,
                updated_at_iso TEXT NOT NULL,
                PRIMARY KEY (month, week_number, telegram_user_id)
            )
            """
        )
    conn.commit()


def set_setting_sqlite(conn: sqlite3.Connection, *, key: str, value: str, now: Optional[datetime] = None) -> None:
    if now is None:
        now = datetime.utcnow()
    updated_at_iso = now.replace(microsecond=0).isoformat() + "Z"
    conn.execute(
        """
        INSERT INTO club_settings(key, value, updated_at_iso)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at_iso=excluded.updated_at_iso
        """,
        (key, value, updated_at_iso),
    )
    conn.commit()


def get_setting_sqlite(conn: sqlite3.Connection, *, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM club_settings WHERE key = ? LIMIT 1", (key,)).fetchone()
    if row is None:
        return None
    return str(row["value"])


def set_setting_postgres(conn: psycopg.Connection, *, key: str, value: str, now: Optional[datetime] = None) -> None:
    if now is None:
        now = datetime.utcnow()
    updated_at_iso = now.replace(microsecond=0).isoformat() + "Z"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO club_settings(key, value, updated_at_iso)
            VALUES (%s, %s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at_iso = EXCLUDED.updated_at_iso
            """,
            (key, value, updated_at_iso),
        )
    conn.commit()


def get_setting_postgres(conn: psycopg.Connection, *, key: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM club_settings WHERE key = %s LIMIT 1", (key,))
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0])


def set_month_setting_sqlite(
    conn: sqlite3.Connection,
    *,
    month: str,
    key: str,
    value: str,
    now: Optional[datetime] = None,
) -> None:
    if now is None:
        now = datetime.utcnow()
    updated_at_iso = now.replace(microsecond=0).isoformat() + "Z"
    conn.execute(
        """
        INSERT INTO club_month_settings(month, key, value, updated_at_iso)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(month, key) DO UPDATE SET value=excluded.value, updated_at_iso=excluded.updated_at_iso
        """,
        (month, key, value, updated_at_iso),
    )
    conn.commit()


def get_month_setting_sqlite(conn: sqlite3.Connection, *, month: str, key: str) -> Optional[str]:
    row = conn.execute(
        "SELECT value FROM club_month_settings WHERE month = ? AND key = ? LIMIT 1", (month, key)
    ).fetchone()
    if row is None:
        return None
    return str(row["value"])


def set_month_setting_postgres(
    conn: psycopg.Connection,
    *,
    month: str,
    key: str,
    value: str,
    now: Optional[datetime] = None,
) -> None:
    if now is None:
        now = datetime.utcnow()
    updated_at_iso = now.replace(microsecond=0).isoformat() + "Z"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO club_month_settings(month, key, value, updated_at_iso)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (month, key) DO UPDATE SET value = EXCLUDED.value, updated_at_iso = EXCLUDED.updated_at_iso
            """,
            (month, key, value, updated_at_iso),
        )
    conn.commit()


def get_month_setting_postgres(conn: psycopg.Connection, *, month: str, key: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT value FROM club_month_settings WHERE month = %s AND key = %s LIMIT 1", (month, key)
        )
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0])


def upsert_monthly_weekly_plan_sqlite(
    conn: sqlite3.Connection,
    *,
    month: str,
    week_number: int,
    start_page: int,
    end_page: int,
    summary: str,
    encouragement: str,
    scheduled_date: str,
) -> None:
    conn.execute(
        """
        INSERT INTO monthly_weekly_plans(
            month, week_number, start_page, end_page, summary, encouragement, scheduled_date, sent_at_iso
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(month, week_number) DO UPDATE SET
            start_page=excluded.start_page,
            end_page=excluded.end_page,
            summary=excluded.summary,
            encouragement=excluded.encouragement,
            scheduled_date=excluded.scheduled_date
        """,
        (month, week_number, start_page, end_page, summary, encouragement, scheduled_date),
    )
    conn.commit()


def upsert_monthly_weekly_plan_postgres(
    conn: psycopg.Connection,
    *,
    month: str,
    week_number: int,
    start_page: int,
    end_page: int,
    summary: str,
    encouragement: str,
    scheduled_date: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO monthly_weekly_plans(
                month, week_number, start_page, end_page, summary, encouragement, scheduled_date, sent_at_iso
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
            ON CONFLICT (month, week_number) DO UPDATE SET
                start_page=EXCLUDED.start_page,
                end_page=EXCLUDED.end_page,
                summary=EXCLUDED.summary,
                encouragement=EXCLUDED.encouragement,
                scheduled_date=EXCLUDED.scheduled_date
            """,
            (month, week_number, start_page, end_page, summary, encouragement, scheduled_date),
        )
    conn.commit()


def list_monthly_weekly_plans_sqlite(conn: sqlite3.Connection, *, month: str) -> List[MonthlyWeeklyPlan]:
    rows = conn.execute(
        """
        SELECT month, week_number, start_page, end_page, summary, encouragement, scheduled_date, sent_at_iso
        FROM monthly_weekly_plans
        WHERE month = ?
        ORDER BY week_number
        """,
        (month,),
    ).fetchall()
    return [
        MonthlyWeeklyPlan(
            month=str(r["month"]),
            week_number=int(r["week_number"]),
            start_page=int(r["start_page"]),
            end_page=int(r["end_page"]),
            summary=str(r["summary"]),
            encouragement=str(r["encouragement"]),
            scheduled_date=str(r["scheduled_date"]),
            sent_at_iso=r["sent_at_iso"],
        )
        for r in rows
    ]


def list_monthly_weekly_plans_postgres(conn: psycopg.Connection, *, month: str) -> List[MonthlyWeeklyPlan]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT month, week_number, start_page, end_page, summary, encouragement, scheduled_date, sent_at_iso
            FROM monthly_weekly_plans
            WHERE month = %s
            ORDER BY week_number
            """,
            (month,),
        )
        rows = cur.fetchall()
    return [
        MonthlyWeeklyPlan(
            month=str(r[0]),
            week_number=int(r[1]),
            start_page=int(r[2]),
            end_page=int(r[3]),
            summary=str(r[4]),
            encouragement=str(r[5]),
            scheduled_date=str(r[6]),
            sent_at_iso=r[7],
        )
        for r in rows
    ]


def list_due_unsent_weekly_plans_sqlite(
    conn: sqlite3.Connection, *, today_iso: str
) -> List[MonthlyWeeklyPlan]:
    rows = conn.execute(
        """
        SELECT month, week_number, start_page, end_page, summary, encouragement, scheduled_date, sent_at_iso
        FROM monthly_weekly_plans
        WHERE sent_at_iso IS NULL
          AND scheduled_date <= ?
        ORDER BY scheduled_date, month, week_number
        """,
        (today_iso,),
    ).fetchall()
    return [
        MonthlyWeeklyPlan(
            month=str(r["month"]),
            week_number=int(r["week_number"]),
            start_page=int(r["start_page"]),
            end_page=int(r["end_page"]),
            summary=str(r["summary"]),
            encouragement=str(r["encouragement"]),
            scheduled_date=str(r["scheduled_date"]),
            sent_at_iso=r["sent_at_iso"],
        )
        for r in rows
    ]


def list_due_unsent_weekly_plans_postgres(
    conn: psycopg.Connection, *, today_iso: str
) -> List[MonthlyWeeklyPlan]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT month, week_number, start_page, end_page, summary, encouragement, scheduled_date, sent_at_iso
            FROM monthly_weekly_plans
            WHERE sent_at_iso IS NULL
              AND scheduled_date <= %s
            ORDER BY scheduled_date, month, week_number
            """,
            (today_iso,),
        )
        rows = cur.fetchall()
    return [
        MonthlyWeeklyPlan(
            month=str(r[0]),
            week_number=int(r[1]),
            start_page=int(r[2]),
            end_page=int(r[3]),
            summary=str(r[4]),
            encouragement=str(r[5]),
            scheduled_date=str(r[6]),
            sent_at_iso=r[7],
        )
        for r in rows
    ]


def mark_weekly_plan_sent_sqlite(
    conn: sqlite3.Connection, *, month: str, week_number: int, now: Optional[datetime] = None
) -> None:
    if now is None:
        now = datetime.utcnow()
    sent_at_iso = now.replace(microsecond=0).isoformat() + "Z"
    conn.execute(
        """
        UPDATE monthly_weekly_plans
        SET sent_at_iso = ?
        WHERE month = ? AND week_number = ?
        """,
        (sent_at_iso, month, week_number),
    )
    conn.commit()


def mark_weekly_plan_sent_postgres(
    conn: psycopg.Connection, *, month: str, week_number: int, now: Optional[datetime] = None
) -> None:
    if now is None:
        now = datetime.utcnow()
    sent_at_iso = now.replace(microsecond=0).isoformat() + "Z"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE monthly_weekly_plans
            SET sent_at_iso = %s
            WHERE month = %s AND week_number = %s
            """,
            (sent_at_iso, month, week_number),
        )
    conn.commit()


def upsert_weekly_progress_status_sqlite(
    conn: sqlite3.Connection,
    *,
    month: str,
    week_number: int,
    telegram_user_id: int,
    telegram_username: Optional[str],
    full_name: Optional[str],
    status: str,
    now: Optional[datetime] = None,
) -> None:
    if now is None:
        now = datetime.utcnow()
    updated_at_iso = now.replace(microsecond=0).isoformat() + "Z"
    conn.execute(
        """
        INSERT INTO weekly_progress_status(
            month, week_number, telegram_user_id, telegram_username, full_name, status, updated_at_iso
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(month, week_number, telegram_user_id) DO UPDATE SET
            telegram_username=excluded.telegram_username,
            full_name=excluded.full_name,
            status=excluded.status,
            updated_at_iso=excluded.updated_at_iso
        """,
        (month, week_number, telegram_user_id, telegram_username, full_name, status, updated_at_iso),
    )
    conn.commit()


def upsert_weekly_progress_status_postgres(
    conn: psycopg.Connection,
    *,
    month: str,
    week_number: int,
    telegram_user_id: int,
    telegram_username: Optional[str],
    full_name: Optional[str],
    status: str,
    now: Optional[datetime] = None,
) -> None:
    if now is None:
        now = datetime.utcnow()
    updated_at_iso = now.replace(microsecond=0).isoformat() + "Z"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO weekly_progress_status(
                month, week_number, telegram_user_id, telegram_username, full_name, status, updated_at_iso
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (month, week_number, telegram_user_id) DO UPDATE SET
                telegram_username=EXCLUDED.telegram_username,
                full_name=EXCLUDED.full_name,
                status=EXCLUDED.status,
                updated_at_iso=EXCLUDED.updated_at_iso
            """,
            (month, week_number, telegram_user_id, telegram_username, full_name, status, updated_at_iso),
        )
    conn.commit()


def list_weekly_progress_stats_sqlite(
    conn: sqlite3.Connection, *, month: str, week_number: int
) -> List[WeeklyProgressStat]:
    rows = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM weekly_progress_status
        WHERE month = ? AND week_number = ?
        GROUP BY status
        ORDER BY status
        """,
        (month, week_number),
    ).fetchall()
    return [WeeklyProgressStat(status=str(r["status"]), count=int(r["count"])) for r in rows]


def list_weekly_progress_stats_postgres(
    conn: psycopg.Connection, *, month: str, week_number: int
) -> List[WeeklyProgressStat]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM weekly_progress_status
            WHERE month = %s AND week_number = %s
            GROUP BY status
            ORDER BY status
            """,
            (month, week_number),
        )
        rows = cur.fetchall()
    return [WeeklyProgressStat(status=str(r[0]), count=int(r[1])) for r in rows]


def list_weekly_progress_members_sqlite(
    conn: sqlite3.Connection, *, month: str, week_number: int
) -> List[WeeklyProgressMember]:
    rows = conn.execute(
        """
        SELECT telegram_user_id, telegram_username, full_name, status, updated_at_iso
        FROM weekly_progress_status
        WHERE month = ? AND week_number = ?
        ORDER BY status, COALESCE(telegram_username, full_name, CAST(telegram_user_id AS TEXT))
        """,
        (month, week_number),
    ).fetchall()
    return [
        WeeklyProgressMember(
            telegram_user_id=int(r["telegram_user_id"]),
            telegram_username=r["telegram_username"],
            full_name=r["full_name"],
            status=str(r["status"]),
            updated_at_iso=str(r["updated_at_iso"]),
        )
        for r in rows
    ]


def list_weekly_progress_members_postgres(
    conn: psycopg.Connection, *, month: str, week_number: int
) -> List[WeeklyProgressMember]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT telegram_user_id, telegram_username, full_name, status, updated_at_iso
            FROM weekly_progress_status
            WHERE month = %s AND week_number = %s
            ORDER BY status, COALESCE(telegram_username, full_name, CAST(telegram_user_id AS TEXT))
            """,
            (month, week_number),
        )
        rows = cur.fetchall()
    return [
        WeeklyProgressMember(
            telegram_user_id=int(r[0]),
            telegram_username=r[1],
            full_name=r[2],
            status=str(r[3]),
            updated_at_iso=str(r[4]),
        )
        for r in rows
    ]


def list_user_weekly_progress_sqlite(
    conn: sqlite3.Connection, *, month: str, telegram_user_id: int
) -> List[WeeklyProgressMember]:
    rows = conn.execute(
        """
        SELECT telegram_user_id, telegram_username, full_name, status, updated_at_iso
        FROM weekly_progress_status
        WHERE month = ? AND telegram_user_id = ?
        ORDER BY updated_at_iso
        """,
        (month, telegram_user_id),
    ).fetchall()
    return [
        WeeklyProgressMember(
            telegram_user_id=int(r["telegram_user_id"]),
            telegram_username=r["telegram_username"],
            full_name=r["full_name"],
            status=str(r["status"]),
            updated_at_iso=str(r["updated_at_iso"]),
        )
        for r in rows
    ]


def get_user_weekly_status_map_sqlite(
    conn: sqlite3.Connection, *, month: str, telegram_user_id: int
) -> dict:
    rows = conn.execute(
        """
        SELECT week_number, status
        FROM weekly_progress_status
        WHERE month = ? AND telegram_user_id = ?
        """,
        (month, telegram_user_id),
    ).fetchall()
    return {int(r["week_number"]): str(r["status"]) for r in rows}


def get_user_weekly_status_map_postgres(
    conn: psycopg.Connection, *, month: str, telegram_user_id: int
) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT week_number, status
            FROM weekly_progress_status
            WHERE month = %s AND telegram_user_id = %s
            """,
            (month, telegram_user_id),
        )
        rows = cur.fetchall()
    return {int(r[0]): str(r[1]) for r in rows}


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


def insert_bookmark_sqlite(
    conn: sqlite3.Connection,
    *,
    telegram_user_id: int,
    telegram_username: Optional[str],
    full_name: Optional[str],
    page: Optional[int],
    text: str,
    now: Optional[datetime] = None,
) -> Bookmark:
    if now is None:
        now = datetime.utcnow()
    created_at_iso = now.replace(microsecond=0).isoformat() + "Z"

    cur = conn.execute(
        """
        INSERT INTO bookmarks (
            telegram_user_id, telegram_username, full_name,
            page, text, created_at_iso
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (telegram_user_id, telegram_username, full_name, page, text, created_at_iso),
    )
    conn.commit()

    return Bookmark(
        id=int(cur.lastrowid),
        telegram_user_id=telegram_user_id,
        telegram_username=telegram_username,
        full_name=full_name,
        page=page,
        text=text,
        created_at_iso=created_at_iso,
    )


def insert_bookmark_postgres(
    conn: psycopg.Connection,
    *,
    telegram_user_id: int,
    telegram_username: Optional[str],
    full_name: Optional[str],
    page: Optional[int],
    text: str,
    now: Optional[datetime] = None,
) -> Bookmark:
    if now is None:
        now = datetime.utcnow()
    created_at_iso = now.replace(microsecond=0).isoformat() + "Z"

    new_id = None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bookmarks (
                telegram_user_id, telegram_username, full_name,
                page, text, created_at_iso
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (telegram_user_id, telegram_username, full_name, page, text, created_at_iso),
        )
        row = cur.fetchone()
        if row:
            new_id = int(row[0])
    conn.commit()

    return Bookmark(
        id=int(new_id or 0),
        telegram_user_id=telegram_user_id,
        telegram_username=telegram_username,
        full_name=full_name,
        page=page,
        text=text,
        created_at_iso=created_at_iso,
    )


def list_bookmarks_sqlite(
    conn: sqlite3.Connection,
    *,
    telegram_user_id: int,
    query: Optional[str] = None,
    limit: int = 10,
) -> List[Bookmark]:
    if query:
        rows = conn.execute(
            """
            SELECT id, telegram_user_id, telegram_username, full_name, page, text, created_at_iso
            FROM bookmarks
            WHERE telegram_user_id = ?
              AND text LIKE '%' || ? || '%'
            ORDER BY id DESC
            LIMIT ?
            """,
            (telegram_user_id, query, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, telegram_user_id, telegram_username, full_name, page, text, created_at_iso
            FROM bookmarks
            WHERE telegram_user_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (telegram_user_id, limit),
        ).fetchall()
    return [
        Bookmark(
            id=int(r["id"]),
            telegram_user_id=int(r["telegram_user_id"]),
            telegram_username=r["telegram_username"],
            full_name=r["full_name"],
            page=r["page"],
            text=r["text"],
            created_at_iso=r["created_at_iso"],
        )
        for r in rows
    ]


def list_bookmarks_postgres(
    conn: psycopg.Connection,
    *,
    telegram_user_id: int,
    query: Optional[str] = None,
    limit: int = 10,
) -> List[Bookmark]:
    with conn.cursor() as cur:
        if query:
            cur.execute(
                """
                SELECT id, telegram_user_id, telegram_username, full_name, page, text, created_at_iso
                FROM bookmarks
                WHERE telegram_user_id = %s
                  AND text ILIKE %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (telegram_user_id, f"%{query}%", limit),
            )
        else:
            cur.execute(
                """
                SELECT id, telegram_user_id, telegram_username, full_name, page, text, created_at_iso
                FROM bookmarks
                WHERE telegram_user_id = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (telegram_user_id, limit),
            )
        rows = cur.fetchall()
    return [
        Bookmark(
            id=int(r[0]),
            telegram_user_id=int(r[1]),
            telegram_username=r[2],
            full_name=r[3],
            page=r[4],
            text=r[5],
            created_at_iso=r[6],
        )
        for r in rows
    ]


def update_bookmark_sqlite(
    conn: sqlite3.Connection,
    *,
    bookmark_id: int,
    telegram_user_id: int,
    page: Optional[int],
    text: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE bookmarks
        SET page = ?, text = ?
        WHERE id = ?
          AND telegram_user_id = ?
        """,
        (page, text, bookmark_id, telegram_user_id),
    )
    conn.commit()
    return cur.rowcount > 0


def update_bookmark_postgres(
    conn: psycopg.Connection,
    *,
    bookmark_id: int,
    telegram_user_id: int,
    page: Optional[int],
    text: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bookmarks
            SET page = %s, text = %s
            WHERE id = %s
              AND telegram_user_id = %s
            """,
            (page, text, bookmark_id, telegram_user_id),
        )
        updated = cur.rowcount > 0
    conn.commit()
    return updated


def delete_bookmark_sqlite(
    conn: sqlite3.Connection,
    *,
    bookmark_id: int,
    telegram_user_id: int,
) -> bool:
    cur = conn.execute(
        """
        DELETE FROM bookmarks
        WHERE id = ?
          AND telegram_user_id = ?
        """,
        (bookmark_id, telegram_user_id),
    )
    conn.commit()
    return cur.rowcount > 0


def delete_bookmark_postgres(
    conn: psycopg.Connection,
    *,
    bookmark_id: int,
    telegram_user_id: int,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM bookmarks
            WHERE id = %s
              AND telegram_user_id = %s
            """,
            (bookmark_id, telegram_user_id),
        )
        deleted = cur.rowcount > 0
    conn.commit()
    return deleted


def enforce_bookmarks_limit_sqlite(
    conn: sqlite3.Connection,
    *,
    telegram_user_id: int,
    max_per_user: int,
) -> int:
    if max_per_user <= 0:
        return 0
    # Delete oldest rows over the limit
    cur = conn.execute(
        """
        DELETE FROM bookmarks
        WHERE id IN (
            SELECT id FROM bookmarks
            WHERE telegram_user_id = ?
            ORDER BY id DESC
            LIMIT -1 OFFSET ?
        )
        """,
        (telegram_user_id, max_per_user),
    )
    conn.commit()
    return cur.rowcount


def enforce_bookmarks_limit_postgres(
    conn: psycopg.Connection,
    *,
    telegram_user_id: int,
    max_per_user: int,
) -> int:
    if max_per_user <= 0:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM bookmarks
            WHERE id IN (
                SELECT id FROM bookmarks
                WHERE telegram_user_id = %s
                ORDER BY id DESC
                OFFSET %s
            )
            """,
            (telegram_user_id, max_per_user),
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def find_user_id_by_username_sqlite(
    conn: sqlite3.Connection,
    *,
    username: str,
) -> Optional[int]:
    uname = username.lstrip("@").strip()
    if not uname:
        return None
    row = conn.execute(
        """
        SELECT telegram_user_id
        FROM bookmarks
        WHERE telegram_username = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (uname,),
    ).fetchone()
    if row is None:
        # fallback to progress events
        row = conn.execute(
            """
            SELECT telegram_user_id
            FROM progress_events
            WHERE telegram_username = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (uname,),
        ).fetchone()
    if row is None:
        return None
    return int(row["telegram_user_id"])


def find_user_id_by_username_postgres(
    conn: psycopg.Connection,
    *,
    username: str,
) -> Optional[int]:
    uname = username.lstrip("@").strip()
    if not uname:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT telegram_user_id
            FROM bookmarks
            WHERE telegram_username = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (uname,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                """
                SELECT telegram_user_id
                FROM progress_events
                WHERE telegram_username = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (uname,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return int(row[0])


def list_recent_bookmarks_all_sqlite(
    conn: sqlite3.Connection,
    *,
    limit: int = 200,
) -> List[Bookmark]:
    rows = conn.execute(
        """
        SELECT id, telegram_user_id, telegram_username, full_name, page, text, created_at_iso
        FROM bookmarks
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        Bookmark(
            id=int(r["id"]),
            telegram_user_id=int(r["telegram_user_id"]),
            telegram_username=r["telegram_username"],
            full_name=r["full_name"],
            page=r["page"],
            text=r["text"],
            created_at_iso=r["created_at_iso"],
        )
        for r in rows
    ]


def list_recent_bookmarks_all_postgres(
    conn: psycopg.Connection,
    *,
    limit: int = 200,
) -> List[Bookmark]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, telegram_user_id, telegram_username, full_name, page, text, created_at_iso
            FROM bookmarks
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [
        Bookmark(
            id=int(r[0]),
            telegram_user_id=int(r[1]),
            telegram_username=r[2],
            full_name=r[3],
            page=r[4],
            text=r[5],
            created_at_iso=r[6],
        )
        for r in rows
    ]

