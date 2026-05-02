from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import Counter
from datetime import datetime
from io import BytesIO
from typing import List, Optional, Tuple

import httpx

from openai import OpenAI
from openai import APIConnectionError, APIError, AuthenticationError, RateLimitError

from telegram import Bot, ChatMember, Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.book_catalog import get_book_for_month, load_book_catalog
from app.config import Settings
from app.db import (
    Bookmark,
    MonthlyWeeklyPlan,
    connect_postgres,
    connect_sqlite,
    delete_bookmark_postgres,
    delete_bookmark_sqlite,
    enforce_bookmarks_limit_postgres,
    enforce_bookmarks_limit_sqlite,
    find_user_id_by_username_postgres,
    find_user_id_by_username_sqlite,
    init_db_postgres,
    init_db_sqlite,
    insert_bookmark_postgres,
    insert_bookmark_sqlite,
    insert_progress_event,
    insert_progress_event_postgres,
    is_postgres_url,
    get_setting_postgres,
    get_setting_sqlite,
    get_month_setting_postgres,
    get_month_setting_sqlite,
    get_user_weekly_status_map_postgres,
    get_user_weekly_status_map_sqlite,
    list_due_unsent_weekly_plans_postgres,
    list_due_unsent_weekly_plans_sqlite,
    list_bookmarks_postgres,
    list_bookmarks_sqlite,
    list_monthly_weekly_plans_postgres,
    list_monthly_weekly_plans_sqlite,
    list_recent_bookmarks_all_postgres,
    list_recent_bookmarks_all_sqlite,
    list_weekly_progress_members_postgres,
    list_weekly_progress_members_sqlite,
    list_weekly_progress_stats_postgres,
    list_weekly_progress_stats_sqlite,
    update_bookmark_postgres,
    update_bookmark_sqlite,
    set_setting_postgres,
    set_setting_sqlite,
    set_month_setting_postgres,
    set_month_setting_sqlite,
    mark_weekly_plan_sent_postgres,
    mark_weekly_plan_sent_sqlite,
    upsert_monthly_weekly_plan_postgres,
    upsert_monthly_weekly_plan_sqlite,
    upsert_weekly_progress_status_postgres,
    upsert_weekly_progress_status_sqlite,
)
from app.reading_check import WeeklyCheckConfig, build_weekly_check_message
from app.progress_puzzle import (
    calculate_progress_percent,
    calculate_revealed_tiles,
    render_image_puzzle,
    render_text_grid,
)

logger = logging.getLogger(__name__)

def _norm_flag(s: Optional[str]) -> str:
    return (s or "").strip().lower()

_BUILD_COOLDOWN_MINUTES = 60

def _now_iso() -> str:
    return datetime.utcnow().isoformat()

def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    raw = (s or "").strip()
    if not raw:
        return None
    try:
        # stored as ISO without timezone; treat as UTC
        return datetime.fromisoformat(raw.replace("Z", ""))
    except Exception:
        return None

def _get_month_setting(settings: Settings, *, month: str, key: str) -> Optional[str]:
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            return get_month_setting_postgres(conn, month=month, key=key)
        finally:
            conn.close()
    conn = connect_sqlite(settings.db_path)
    try:
        return get_month_setting_sqlite(conn, month=month, key=key)
    finally:
        conn.close()

def _set_month_setting(settings: Settings, *, month: str, key: str, value: str) -> None:
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_month_setting_postgres(conn, month=month, key=key, value=value)
        finally:
            conn.close()
        return
    conn = connect_sqlite(settings.db_path)
    try:
        set_month_setting_sqlite(conn, month=month, key=key, value=value)
    finally:
        conn.close()


def _get_global_setting(settings: Settings, *, key: str) -> Optional[str]:
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            return get_setting_postgres(conn, key=key)
        finally:
            conn.close()
    conn = connect_sqlite(settings.db_path)
    try:
        return get_setting_sqlite(conn, key=key)
    finally:
        conn.close()


def _set_global_setting(settings: Settings, *, key: str, value: str) -> None:
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_setting_postgres(conn, key=key, value=value)
        finally:
            conn.close()
        return
    conn = connect_sqlite(settings.db_path)
    try:
        set_setting_sqlite(conn, key=key, value=value)
    finally:
        conn.close()


_LAST_MEMBER_MESSAGE_ID_KEY = "last_member_message_id"


def _remember_last_member_message(settings: Settings, *, message_id: int) -> None:
    try:
        _set_global_setting(settings, key=_LAST_MEMBER_MESSAGE_ID_KEY, value=str(int(message_id)))
    except Exception:
        logger.info("Failed to store last member message id", exc_info=True)


def _load_last_member_message_id(settings: Settings) -> Optional[int]:
    raw = _get_global_setting(settings, key=_LAST_MEMBER_MESSAGE_ID_KEY)
    if not raw:
        return None
    raw = str(raw).strip()
    return int(raw) if raw.isdigit() else None

def _rate_limit_hint(e: Exception) -> str:
    """
    Best-effort: OpenAI RateLimitError can mean RPM/TPM throttling OR insufficient quota.
    We avoid leaking internal details but provide actionable next steps.
    """
    msg = (str(e) or "").lower()
    if "insufficient_quota" in msg or "insufficient quota" in msg or "quota" in msg:
        return "OpenAI 크레딧/쿼터가 부족한 것 같아요. 결제/한도(Usage/Billing)를 확인해주세요."
    return "OpenAI 요청이 잠시 몰려 제한됐어요. 20~60초 후 다시 시도해줘요."


async def _with_openai_retries(func, *args, **kwargs):
    """
    Run a blocking OpenAI call in a thread with a small retry budget for transient 429s.
    """
    delays = [2.0, 6.0, 15.0]
    last_err: Optional[Exception] = None
    for attempt, d in enumerate([0.0] + delays):
        if attempt > 0:
            await asyncio.sleep(d)
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except RateLimitError as e:
            last_err = e
            continue
    if last_err is not None:
        raise last_err
    # should not happen
    return await asyncio.to_thread(func, *args, **kwargs)


def _default_weekly_check_cfg() -> WeeklyCheckConfig:
    return WeeklyCheckConfig(month=_current_month_yyyy_mm(), week_number=1, range_label="p.1-50")

def _is_active_member_status(status: str) -> bool:
    return status in ("creator", "administrator", "member", ChatMember.OWNER, ChatMember.ADMINISTRATOR, ChatMember.MEMBER)


async def _is_member_of(chat_id: str, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user

    if user is None:
        return False

    try:
        member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user.id)
        if _is_active_member_status(getattr(member, "status", "")):
            return True
    except TelegramError:
        logger.info("Failed to check chat member", exc_info=True)

    return False


async def _require_member_or_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    settings: Settings = context.application.bot_data["settings"]
    msg = update.effective_message

    ok = await _is_member_of(settings.member_chat_id, update, context) or await _is_member_of(
        settings.admin_chat_id, update, context
    )
    if ok:
        return True
    if msg is not None:
        await msg.reply_text("이 봇은 북클럽 멤버/운영진만 사용할 수 있어요.")
    return False


async def _require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    settings: Settings = context.application.bot_data["settings"]
    msg = update.effective_message
    ok = await _is_member_of(settings.admin_chat_id, update, context)
    if ok:
        return True
    if msg is not None:
        await msg.reply_text("이 명령은 운영진 방 멤버만 사용할 수 있어요.")
    return False


async def _require_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    settings: Settings = context.application.bot_data["settings"]
    msg = update.effective_message
    ok = await _is_member_of(settings.member_chat_id, update, context)
    if ok:
        return True
    if msg is not None:
        await msg.reply_text("이 명령은 북클럽 멤버만 사용할 수 있어요.")
    return False


async def _require_private_chat(update: Update) -> bool:
    chat = update.effective_chat
    msg = update.effective_message
    if chat is None:
        return False
    if getattr(chat, "type", None) != "private":
        if msg is not None:
            await msg.reply_text("이 기능은 봇과의 1:1 대화에서만 사용할 수 있어요.")
        return False
    return True


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    await update.message.reply_text(
        "AI Reading Club Agent입니다.\n\n"
        "운영자: /help 를 확인하고 /send_weekly_check 로 주간 진도체크를 보낼 수 있어요.",
    )

async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Prints chat_id for the current chat (group/supergroup/private).
    Useful for setting TELEGRAM_CHAT_ID env var.
    """
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if msg is None or chat is None:
        return

    chat_type = getattr(chat, "type", None) or "unknown"
    chat_title = getattr(chat, "title", None)
    username = getattr(user, "username", None) if user else None

    lines = [
        "Chat ID 정보를 가져왔어요.",
        f"- chat_id: `{chat.id}`",
        f"- chat_type: `{chat_type}`",
    ]
    if chat_title:
        lines.append(f"- chat_title: `{chat_title}`")
    if user:
        lines.append(f"- your_user_id: `{user.id}`")
    if username:
        lines.append(f"- your_username: `@{username}`")

    await msg.reply_text("\n".join(lines))


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    if msg is None:
        return

    # taste_* / diag_taste 등 비공개 운영용 명령은 여기에 나열하지 않음.

    text = "\n".join(
        [
            "운영진용 명령어",
            "",
            "- /chatid: 현재 채팅의 chat_id 확인 (Railway 변수 MEMBER_CHAT_ID/ADMIN_CHAT_ID 설정용)",
            "- /test_weekly_check [주차]: 운영진 방에서 진도 체크 미리보기 (멤버방 발송/발송 처리 없음)",
            "- /send_weekly_check [주차]: (수동) 북클럽 단체방에 주간 진도 체크 메시지 전송 (기본 1주차)",
            "- /send_weekly_quiz [주차]: (운영진) 해당 주차 미니 퀴즈(투표) 전송",
            "- /send_weekly_topic [주차]: (운영진) 해당 주차 토론 주제 전송",
            "- /preview_weekly [주차]: (운영진) 저장된 해당 주차 요약·퀴즈·토론 미리보기",
            "- /rebuild_weekly [주차]: (운영진) 해당 주만 OpenAI로 다시 생성·저장 후 미리보기",
            "- 책/모임 정보는 파일로 관리: data/book_catalog.json (또는 환경변수 BOOK_CATALOG_PATH)",
            "- /build_book_summary: (선택) 책 소개를 1~3줄로 요약 (OPENAI_API_KEY 필요)",
            "- /build_month_plan: 모임 날짜 기준 4주 계획 생성(주차별 미니 퀴즈·토론 포함)",
            "- /show_month_plan: 4주 계획(운영진은 퀴즈·토론 미리보기 포함)",
            "- /send_book_info: 확정된 책 요약을 멤버 단체방에 전송",
            "- /test_book_videos: 운영진 방에서 현재 책 관련 영상 자료 미리보기",
            "- /send_book_videos: 현재 책 관련 영상 자료를 멤버방에 전송",
            "- /show_book: (다음 모임 기준) 책/모임 일정 확인",
            "- /set_puzzle_cover: 사진 메시지에 답장해 퍼즐 대표 이미지 저장",
            "- /show_puzzle <읽은페이지>: 랜덤 퍼즐 미리보기 (운영진 테스트)",
            "- /weekly_stats [주차]: 주차별 응답 통계",
            "- /weekly_stats_detail [주차]: 주차별 멤버 상태 상세",
            "- /share_weekly_stats [주차]: 주차별 통계를 단체방에 공유",
            "- /sync_catalog_plans [force]: 카탈로그 기반 4주 계획을 DB에 반영 (기본: 기존 계획 유지)",
            "- /delete_last: (운영진) 멤버방에 마지막으로 보낸 봇 메시지 삭제",
            "- /delete_reply: (운영진) 삭제할 메시지에 답장 후 실행하면 해당 메시지 삭제",
            "",
            "빠른 시작",
            "1) 봇을 독서모임 그룹에 초대",
            "2) 그룹에서 /chatid 로 chat_id 복사",
            "3) Railway Variables에 MEMBER_CHAT_ID/ADMIN_CHAT_ID로 저장",
            "4) 운영진 방에서 /sync_catalog_plans 로 4주 계획 반영",
            "5) 운영진 방에서 /test_weekly_check 1 로 미리보기",
            "6) 문제 없으면 /send_weekly_check 1 로 멤버방 전송",
        ]
    )

    await msg.reply_text(text)


async def cmd_guide(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Member-facing usage guide. Keep this usable in the member group only.
    # taste_* 등 운영 전용 기능은 노출하지 않음.
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    bot = context.bot
    if msg is None:
        return

    bot_username = getattr(bot, "username", None) or ""
    dm_link = f"https://t.me/{bot_username}" if bot_username else ""

    text = "\n".join(
        [
            "북클럽 봇 사용 방법",
            "",
            "봇 소개",
            "- /about",
            "",
            "진도 체크",
            "- 북클럽 단체방에 올라오는 메시지에서 버튼(✅/🟡/🔴)을 눌러주세요.",
            "",
            "이번 책 정보 (다음 모임 기준)",
            "- /book",
            "- /book_month 2026-04",
            "- /plan",
            "- /my_progress (1:1 대화에서만)",
            "- 단체방 주간 진도체크 버튼으로 상태를 남겨주세요. (메시지에 책 제목이 같이 표시돼요)",
            "- 이전 주차 메시지가 남아 있으면, 같은 버튼을 다시 눌러 상태를 업데이트할 수 있어요.",
            "- 1:1 대화 바로가기: " + (dm_link or "봇 프로필에서 개인 대화를 열어주세요."),
            "",
            "책갈피(문장 메모) — 1:1 대화에서만",
            "- 저장: /bookmark 인상 깊은 문장",
            "- 보기: /bookmarks",
            "- 수정: /bookmark_edit #id 수정할 문장",
            "- 삭제: /bookmark_delete #id",
        ]
    )

    await msg.reply_text(text)


async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    if msg is None:
        return

    text = "\n".join(
        [
            "📚 AI Book Club Bot 소개",
            "",
            "이 봇은 독서모임을 ‘꾸준히, 즐겁게’ 이어가도록 돕는 북클럽 도우미예요.",
            "",
            "- ✅ 진도 체크: 매주 올라오는 진도 체크에 버튼으로 응답해요.",
            "- 🔖 북마크: 읽다가 마음에 든 문장을 책갈피처럼 저장해요.",
            "",
            "사용법은 /guide 를 참고해주세요.",
        ]
    )
    await msg.reply_text(text)


def _clean_one_line(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\n", " ").split()).strip()

def _current_month_yyyy_mm() -> str:
    return datetime.now().strftime("%Y-%m")


def _parse_month_yyyy_mm(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    try:
        dt = datetime.strptime(s, "%Y-%m")
    except ValueError:
        return None
    return dt.strftime("%Y-%m")


def _add_months(month: str, delta: int) -> str:
    dt = datetime.strptime(month, "%Y-%m")
    year = dt.year
    mon = dt.month + delta
    while mon > 12:
        year += 1
        mon -= 12
    while mon < 1:
        year -= 1
        mon += 12
    return f"{year:04d}-{mon:02d}"


def _extract_target_month_from_question(question: str, base_month: str) -> str:
    q = (question or "").strip().lower()
    if "다다음달" in q or "다다음 달" in q:
        return _add_months(base_month, 2)
    if "다음달" in q or "다음 달" in q:
        return _add_months(base_month, 1)
    if "지난달" in q or "지난 달" in q:
        return _add_months(base_month, -1)

    for m in range(1, 13):
        if f"{m}월" in q:
            year = int(base_month.split("-")[0])
            return f"{year:04d}-{m:02d}"
    return base_month


def _get_active_month(settings: Settings) -> str:
    """
    Active month selection rule (new):

    - Do NOT use "current month".
    - Pick the book whose meeting_at is the closest upcoming meeting (>= now),
      using the catalog file (BOOK_CATALOG_PATH).
    - If no future meeting exists, fall back to current month.
    """
    try:
        from zoneinfo import ZoneInfo

        now = datetime.now(tz=ZoneInfo(settings.timezone or "Asia/Seoul"))
    except Exception:
        now = datetime.now()

    catalog = load_book_catalog(settings.book_catalog_path)
    if not isinstance(catalog, dict) or not catalog:
        return _current_month_yyyy_mm()

    candidates: list[tuple[datetime, str]] = []

    for month, entry in catalog.items():
        if not isinstance(entry, dict):
            continue
        m = _parse_month_yyyy_mm(str(month))
        if not m:
            continue
        meeting_at = str(entry.get("meeting_at") or "").strip()
        if not meeting_at:
            continue
        meeting_dt = _parse_meeting_date_for_plan(meeting_at)
        if meeting_dt is None:
            continue
        # Interpret date-only meeting_at as end-of-day so it's still "upcoming" on that day.
        if len(meeting_at) == 10:
            meeting_dt = meeting_dt.replace(hour=23, minute=59)

        # Compare with "now" in the same naive/aware space.
        try:
            meeting_dt_cmp = meeting_dt.replace(tzinfo=now.tzinfo)  # type: ignore[arg-type]
            if meeting_dt_cmp >= now:
                candidates.append((meeting_dt_cmp, m))
        except Exception:
            if meeting_dt >= now.replace(tzinfo=None):
                candidates.append((meeting_dt, m))

    if not candidates:
        return _current_month_yyyy_mm()

    # closest future meeting
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _set_active_month(settings: Settings, month: str) -> None:
    # Deprecated: active month is no longer stored.
    _ = settings
    _ = month


async def cmd_set_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    _ = context
    await msg.reply_text(
        "\n".join(
            [
                "이제 /set_month 는 사용하지 않아요.",
                "- 기준 월은 '가장 가까운 다음 모임'을 기준으로 자동 결정돼요.",
                "- 특정 월의 책을 보려면: /book_month 2026-04",
                f"- 책/모임 정보는 파일에서 관리해요: `{settings.book_catalog_path}`",
            ]
        )
    )


def _load_month_puzzle_meta(settings: Settings, *, month: str) -> Tuple[Optional[str], Optional[int]]:
    file_id: Optional[str] = None
    seed_raw: Optional[str] = None
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            file_id = get_month_setting_postgres(conn, month=month, key="puzzle_cover_file_id")
            seed_raw = get_month_setting_postgres(conn, month=month, key="puzzle_seed")
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            file_id = get_month_setting_sqlite(conn, month=month, key="puzzle_cover_file_id")
            seed_raw = get_month_setting_sqlite(conn, month=month, key="puzzle_seed")
        finally:
            conn.close()
    seed = int(seed_raw) if seed_raw and seed_raw.isdigit() else None
    return file_id, seed


def _save_month_puzzle_meta(settings: Settings, *, month: str, file_id: str, seed: int) -> None:
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_month_setting_postgres(conn, month=month, key="puzzle_cover_file_id", value=file_id)
            set_month_setting_postgres(conn, month=month, key="puzzle_seed", value=str(seed))
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            set_month_setting_sqlite(conn, month=month, key="puzzle_cover_file_id", value=file_id)
            set_month_setting_sqlite(conn, month=month, key="puzzle_seed", value=str(seed))
        finally:
            conn.close()


def _truncate(s: str, *, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 1)].rstrip() + "…"


def _extract_isbn(identifiers: Optional[list]) -> Optional[str]:
    if not identifiers:
        return None
    # Prefer ISBN_13, then ISBN_10
    isbn13 = None
    isbn10 = None
    for it in identifiers:
        t = (it or {}).get("type")
        v = (it or {}).get("identifier")
        if not v:
            continue
        if t == "ISBN_13":
            isbn13 = v
        elif t == "ISBN_10":
            isbn10 = v
    return isbn13 or isbn10


async def _google_books_search(*, query: str, api_key: Optional[str], max_results: int = 5) -> list[dict]:
    q = (query or "").strip()
    if not q:
        return []
    params = {
        "q": q,
        "maxResults": str(max(1, min(10, int(max_results)))),
        "printType": "books",
        "orderBy": "relevance",
    }
    if api_key:
        params["key"] = api_key
    url = "https://www.googleapis.com/books/v1/volumes"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    items = data.get("items") or []
    results: list[dict] = []
    for it in items:
        vi = (it or {}).get("volumeInfo") or {}
        title = _clean_one_line(vi.get("title"))
        authors = vi.get("authors") or []
        publisher = _clean_one_line(vi.get("publisher"))
        published = _clean_one_line(vi.get("publishedDate"))
        page_count = vi.get("pageCount")
        description = _clean_one_line(vi.get("description"))
        info_link = _clean_one_line(vi.get("infoLink"))
        isbn = _extract_isbn(vi.get("industryIdentifiers"))
        if not title:
            continue
        results.append(
            {
                "title": title,
                "authors": authors,
                "publisher": publisher,
                "published": published,
                "page_count": int(page_count) if isinstance(page_count, int) else None,
                "description": description,
                "info_link": info_link,
                "isbn": isbn,
            }
        )
    return results


async def _search_book_description_for_summary(
    *,
    title: str,
    authors: str,
    api_key: Optional[str],
) -> Optional[str]:
    title = (title or "").strip()
    authors = (authors or "").strip()
    if not title:
        return None

    queries = []
    if authors and authors != "미상":
        queries.append(f'intitle:"{title}" inauthor:"{authors.split(",")[0].strip()}"')
        queries.append(f'{title} {authors}')
    queries.append(f'intitle:"{title}"')
    queries.append(title)

    for query in queries:
        try:
            results = await _google_books_search(query=query, api_key=api_key, max_results=3)
        except Exception:
            logger.info("book description lookup failed", exc_info=True)
            continue
        for item in results:
            desc = (item.get("description") or "").strip()
            if len(desc) >= 80:
                return desc
        for item in results:
            desc = (item.get("description") or "").strip()
            if desc:
                return desc
    return None


def _format_book_candidate_line(idx: int, b: dict) -> str:
    authors = ", ".join(b.get("authors") or []) or "저자 미상"
    pages = b.get("page_count")
    pages_part = f"{pages}p" if pages else "?p"
    pub = b.get("publisher") or ""
    pub_date = b.get("published") or ""
    meta = " / ".join([p for p in [pages_part, pub, pub_date] if p]).strip()
    if meta:
        meta = f" ({meta})"
    return f"{idx}) {b.get('title')} — {authors}{meta}"


async def cmd_book_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    query = " ".join(context.args).strip() if context.args else ""
    if not query:
        await msg.reply_text("사용법: /book_search <책 제목 | 저자 | ISBN>")
        return

    try:
        results = await _google_books_search(query=query, api_key=settings.google_books_api_key, max_results=5)
    except Exception:
        logger.info("book_search failed", exc_info=True)
        await msg.reply_text("지금은 책 검색을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    if not results:
        await msg.reply_text("검색 결과가 없어요. 다른 키워드로 다시 시도해줘요.")
        return

    context.user_data["book_search_results"] = results
    lines = [
        "책 검색 결과 (Google Books)",
        "(참고용) 이제 봇이 책 정보를 DB에 저장하진 않아요. 필요한 값을 data/book_catalog.json에 옮겨 적어주세요.",
        "",
    ]
    for i, b in enumerate(results, start=1):
        lines.append(_format_book_candidate_line(i, b))
    lines.extend(["", "확정: /book_select <번호>  (예: /book_select 1)"])
    await msg.reply_text("\n".join(lines))


async def cmd_book_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    if not context.args or not context.args[0].isdigit():
        await msg.reply_text("사용법: /book_select <번호>\n먼저 /book_search 로 검색해줘요.")
        return

    results = context.user_data.get("book_search_results") or []
    if not isinstance(results, list) or not results:
        await msg.reply_text("선택할 검색 결과가 없어요. 먼저 /book_search 를 실행해줘요.")
        return

    idx = int(context.args[0])
    if idx < 1 or idx > len(results):
        await msg.reply_text(f"번호가 범위를 벗어났어요. 1~{len(results)} 중에서 골라줘요.")
        return

    b = results[idx - 1]
    title = (b.get("title") or "").strip()
    authors = ", ".join(b.get("authors") or []) or ""
    isbn = (b.get("isbn") or "").strip()
    pages = b.get("page_count")
    published = (b.get("published") or "").strip()
    publisher = (b.get("publisher") or "").strip()
    description = (b.get("description") or "").strip()
    info_link = (b.get("info_link") or "").strip()

    month = _get_active_month(settings)
    context.user_data.pop("book_search_results", None)

    # We no longer persist book info into DB. Provide a copy-ready snippet instead.
    snippet = {
        month: {
            "title": title,
            "authors": authors,
            "isbn": isbn,
            "page_count": (str(pages) if isinstance(pages, int) else ""),
            "published": published,
            "publisher": publisher,
            "info_link": info_link,
            "description": description,
            "summary": "",
            "meeting_at": "",
        }
    }

    await msg.reply_text(
        "\n".join(
            [
                "이제 /book_select 는 DB에 저장하지 않고, 파일에 옮겨 적을 수 있게 정보만 정리해드려요.",
                f"- 대상 월: {month}",
                f"- 파일: `{settings.book_catalog_path}`",
                "",
                "아래 JSON을 참고해서 해당 월 항목을 채워주세요.",
                json.dumps(snippet, ensure_ascii=False, indent=2),
            ]
        )
    )


def _load_club_book_info(settings: Settings, *, month: Optional[str] = None) -> dict:
    m = _parse_month_yyyy_mm(month or "") or _get_active_month(settings)
    catalog = load_book_catalog(settings.book_catalog_path)
    entry = get_book_for_month(catalog, month=m)
    return entry.as_dict()


def _format_book_info_message(info: dict, *, include_description: bool = True) -> str:
    month = info.get("month") or ""
    title = info.get("title") or "(미설정)"
    authors = info.get("authors") or "(미상)"
    meeting_at = info.get("meeting_at") or "(미설정)"
    page_count = info.get("page_count") or "(미상)"
    isbn = info.get("isbn") or ""
    published = info.get("published") or ""
    publisher = info.get("publisher") or ""
    info_link = info.get("info_link") or ""
    trailer_link = info.get("trailer_link") or ""
    trailer_links = info.get("trailer_links") or []
    description = info.get("description") or ""
    summary = info.get("summary") or ""

    meta_parts = [p for p in [publisher, published] if p]
    meta = " / ".join(meta_parts).strip()

    lines = [
        "📚 이달의 책 정보" + (f" ({month})" if month else ""),
        "",
        f"📖 책 제목: {title}",
        f"✍️ 저자: {authors}",
        f"📄 총 페이지: {page_count}",
        f"📅 모임 일정: {meeting_at}",
    ]
    if meta:
        lines.append(f"🏷️ 출판 정보: {meta}")
    if isbn:
        lines.append(f"🔢 ISBN: {isbn}")
    if info_link:
        lines.append(f"🔗 링크: {info_link}")
    trailers: list[str] = []
    if isinstance(trailer_links, list):
        trailers.extend([str(x).strip() for x in trailer_links if str(x).strip()])
    if trailer_link:
        trailers.append(str(trailer_link).strip())
    trailers = list(dict.fromkeys([t for t in trailers if t]))
    for i, t in enumerate(trailers, start=1):
        lines.append(f"🎬 영상{i}: {t}")
    if summary:
        lines.extend(["", "✨ 책 소개 요약", summary])
    elif include_description and description:
        lines.extend(["", "📌 소개", _truncate(description, max_len=700)])
    return "\n".join(lines)


def _book_video_links(info: dict) -> list[str]:
    trailer_link = info.get("trailer_link") or ""
    trailer_links = info.get("trailer_links") or []

    links: list[str] = []
    if isinstance(trailer_links, list):
        links.extend([str(x).strip() for x in trailer_links if str(x).strip()])
    elif isinstance(trailer_links, str) and trailer_links.strip():
        links.append(trailer_links.strip())
    if trailer_link:
        links.append(str(trailer_link).strip())
    return list(dict.fromkeys([link for link in links if link]))


def _format_book_videos_message(info: dict) -> Optional[str]:
    links = _book_video_links(info)
    if not links:
        return None
    month = info.get("month") or ""
    title = info.get("title") or "(미설정)"
    summary = (info.get("summary") or "").strip()

    lines = [
        "🎬 이번 책 관련 영상 자료",
        "",
        f"📚 책: {title}" + (f" ({month})" if month else ""),
    ]
    if summary:
        lines.extend(["", "책 소개 요약", summary])
    lines.extend(["", "함께 보면 좋은 영상"])
    for i, link in enumerate(links, start=1):
        lines.append(f"{i}. {link}")
    lines.extend(["", "읽는 흐름을 잡는 데 참고해보세요."])
    return "\n".join(lines)


def _format_month_plan_brief(month: str, plans: List[MonthlyWeeklyPlan]) -> str:
    if not plans:
        return "아직 4주 계획이 없어요."
    lines = [f"{month} 읽기 계획", ""]
    for plan in plans:
        lines.append(f"{plan.week_number}주차: p.{plan.start_page}-{plan.end_page} ({plan.scheduled_date})")
    return "\n".join(lines)


def _format_book_context_for_qa(info: dict) -> str:
    """Fuller context for LLM Q&A than the member-facing /book message."""
    lines = [
        f"월: {info.get('month') or ''}",
        f"제목: {info.get('title') or '(미설정)'}",
        f"저자: {info.get('authors') or '(미상)'}",
        f"페이지: {info.get('page_count') or '(미상)'}",
        f"모임 일정: {info.get('meeting_at') or '(미설정)'}",
    ]
    for label, key in [
        ("ISBN", "isbn"),
        ("출간일", "published"),
        ("출판사", "publisher"),
        ("도서 링크", "info_link"),
    ]:
        value = (info.get(key) or "").strip()
        if value:
            lines.append(f"{label}: {value}")

    links = _book_video_links(info)
    if links:
        lines.extend(["", "관련 영상 자료:"])
        for i, link in enumerate(links, start=1):
            lines.append(f"{i}. {link}")

    summary = (info.get("summary") or "").strip()
    if summary:
        lines.extend(["", "요약:", summary])

    description = (info.get("description") or "").strip()
    if description:
        lines.extend(["", "책 소개 원문:", _truncate(description, max_len=1800)])

    toc = (info.get("toc") or "").strip()
    if toc:
        lines.extend(["", "목차:", _truncate(toc, max_len=1800)])

    return "\n".join(lines).strip()


def _build_mention_keyword_reply(text: str, info: dict, plans: List[MonthlyWeeklyPlan]) -> Optional[str]:
    q = (text or "").strip().lower()
    month = info.get("month") or ""
    if "취향" in q or "taste" in q:
        return "취향 분석 기능은 현재 운영진만 사용할 수 있어요."
    if any(
        k in q
        for k in [
            "이번달 책",
            "이달의 책",
            "이번 달 책",
            "책 뭐",
            "읽어야 할 책",
            "읽는 책",
            "뭐였지",
            "뭐지",
            "book",
        ]
    ):
        return _format_book_info_message(info, include_description=False)
    if any(k in q for k in ["모임", "언제", "meeting"]):
        return "\n".join(
            [
                f"{month} 모임 정보",
                f"- 책: {info.get('title') or '(미설정)'}",
                f"- 모임 일정: {info.get('meeting_at') or '(미설정)'}",
            ]
        )
    if any(k in q for k in ["요약", "summary", "무슨 내용", "어떤 내용"]):
        summary = info.get("summary") or ""
        if summary:
            return "\n".join([f"{month} 책 요약", "", summary])
        return _format_book_info_message(info, include_description=True)
    if any(k in q for k in ["계획", "plan", "어디까지", "주차"]):
        return _format_month_plan_brief(month, plans)
    return None


def _get_openai_mention_answer(
    api_key: str,
    model: str,
    *,
    question: str,
    month: str,
    info: dict,
    plans: List[MonthlyWeeklyPlan],
) -> str:
    plan_text = _format_month_plan_brief(month, plans)
    info_text = _format_book_context_for_qa(info)
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "너는 북클럽 운영 봇이다. 한국어로 답한다. "
                    "외부 웹을 실시간으로 검색할 수는 없지만, 사용자가 '검색해서 알려줘'라고 말해도 "
                    "사과나 거절로 끝내지 말고 제공된 책 소개, 목차, 주차 계획, 링크 자료를 바탕으로 최대한 자세히 답한다. "
                    "실제로 검색했다고 말하거나 제공되지 않은 사실을 지어내지 않는다. "
                    "부족한 정보는 '저장된 자료 기준으로는'이라고 선을 긋고, 북클럽에서 읽기 좋은 관점과 질문을 함께 제안한다."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"[현재 기준 월]\n{month}\n\n"
                    f"[책/모임 정보]\n{info_text}\n\n"
                    f"[주차 계획]\n{plan_text}\n\n"
                    f"[질문]\n{question}"
                ),
            },
        ],
        temperature=0.4,
    )
    return (resp.choices[0].message.content or "").strip()


_BRACKET_SECTION_LINE = re.compile(r"^【[^】]+】\s*$")


def _normalize_book_summary_copy(text: str) -> str:
    """Strip accidental section labels / list markers if the model ignores prompt rules."""
    out: list[str] = []
    for raw in text.splitlines():
        st = raw.strip()
        if _BRACKET_SECTION_LINE.match(st):
            continue
        if st.startswith("- "):
            indent = raw[: len(raw) - len(raw.lstrip())]
            raw = indent + st[2:].lstrip()
        out.append(raw.rstrip())
    s = "\n".join(out).strip()
    while "\n\n\n" in s:
        s = s.replace("\n\n\n", "\n\n")
    return s


def _get_openai_book_summary(api_key: str, model: str, *, title: str, authors: str, description: str) -> str:
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "너는 한국어 출판/온라인 서점에서 책을 파는 시니어 북 마케터이자 카피라이터다. "
                    "제공된 책 소개문(원문)만 근거로, 독자가 이 글을 읽는 순간 ‘지금 당장 서점에 가서 사거나, "
                    "집에서 바로 책장에서 꺼내 펼치고 싶다’는 충동이 들게 써라. "
                    "사실은 지키되 문장은 감각적이고 밀도 있게. 가짜 서평·가짜 언론/유명인 인용·허위 사실은 금지."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"책 제목: {title}\n"
                    f"저자: {authors}\n\n"
                    f"책 소개(원문, 이것만 근거):\n{description}\n\n"
                    "출력은 ‘하나의 자연스러운 책 추천 글’이어야 한다. 아래 요소를 섹션 제목 없이 한 흐름으로 녹여내라.\n"
                    "(표지에 박을 법한 첫인상의 훅 + 온라인 서점 상세설명처럼 구체적인 매력 + 추천사에 나올 법한 "
                    "따뜻한 설득 + 북클럽에서 같이 읽자는 가벼운 초대)\n\n"
                    "형식 규칙:\n"
                    "- 한국어만. 이모지 금지.\n"
                    "- 【】, ‘표지/서점/추천사’ 같은 라벨·소제목·구역 나누기 금지.\n"
                    "- 줄 맨앞의 하이픈 불릿, 1. 2. 같은 번호 목록 금지.\n"
                    "- 문단은 2~4개 정도로 나눠도 좋다(빈 줄로만 구분). 각 문단 안에서는 문장이 리듬감 있게 이어지게.\n"
                    "- 전체 분량: 대략 12~20문장(짧은 카피가 아니라 ‘읽을 만한’ 길이).\n"
                    "- 독자를 끌어당기는 짧은 핵심 코멘트를 큰따옴표로 1~2줄 넣어도 좋다. "
                    "단, 누가 말했다는 식의 가짜 화자·이름·직함·매체는 붙이지 말 것.\n"
                    "- ‘혁신의 아이콘/선두주자/탐구합니다/여정을 그려냅니다’ 같은 공허한 PR 남발과 "
                    "같은 주어로 시작하는 문장 연속 반복을 피할 것.\n"
                    "- 책 제목은 과하게 반복하지 말 것(필요할 때만).\n"
                    "- 질문으로 끝내지 말 것.\n"
                ),
            },
        ],
        temperature=0.92,
        max_tokens=1100,
    )
    return (resp.choices[0].message.content or "").strip()


def _weekly_quiz_json_from_llm_payload(data: dict) -> str:
    quiz = data.get("quiz")
    if not isinstance(quiz, dict):
        return "{}"
    q = (quiz.get("question") or "").strip()
    raw_opts = quiz.get("options")
    if not isinstance(raw_opts, list):
        return "{}"
    opts = [str(o).strip()[:100] for o in raw_opts[:4]]
    if len(opts) != 4 or not all(opts) or len(set(opts)) != 4:
        return "{}"
    try:
        ci = int(quiz.get("correct_index"))
    except (TypeError, ValueError):
        return "{}"
    if ci not in (0, 1, 2, 3):
        return "{}"
    expl = (quiz.get("explanation") or "").strip()[:200]
    q = q[:280]
    payload = {"question": q, "options": opts, "correct_index": ci, "explanation": expl}
    return json.dumps(payload, ensure_ascii=False)

_WEEKLY_SUMMARY_LINE_SOFT_MIN = 8
_WEEKLY_SUMMARY_LINE_TARGET = 10
_WEEKLY_SUMMARY_LINE_CAP = 12


def _get_openai_weekly_plan_bundle(
    api_key: str,
    model: str,
    *,
    title: str,
    authors: str,
    description: str,
    month: str,
    week_number: int,
    start_page: int,
    end_page: int,
) -> Tuple[str, str, str, str]:
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "너는 온라인 북클럽 운영진을 돕는 에디터다. 책 소개(원문)만 근거로 주차별 콘텐츠를 한국어 JSON으로 만든다. "
                    "원문에 없는 인물·사건·결말·구체 장면은 지어내지 않고, 추측·상상으로 사실 단정도 하지 않는다. "
                    "summary_lines는 **해당 주차 페이지 구간(p.시작–끝)**에 초점을 맞춘 **구체적이고 읽기에 도움이 되는 안내**여야 한다. "
                    "책 소개 원문 안에서 명시된 주제·톤·구조·설정 맥락만 활용하고, 장면 단위 줄거리·등장 이름·결말을 지어내지 않는다. "
                    "각 줄은 한 가지 명확한 역할을 갖도록 촘촘히 다듬되(예: 이 구간 독법, 북클럽에서 물을 질문, 소개문 근거의 주제 하나 짚기 등), 주차마다 접근 각도와 문구가 분명히 달라야 한다. "
                    "퀴즈는 아주 쉬운 객관식 1문항(보기 4개)으로, 이 주차 페이지를 아직 안 읽어도 부담 없는 수준으로 낸다. "
                    "토론 주제는 스포일러 없이 생각을 확장하는 질문 한 덩어리로 쓴다."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"월: {month}\n"
                    f"책 제목: {title}\n"
                    f"저자: {authors}\n"
                    f"주차: {week_number}주차\n"
                    f"읽을 범위: p.{start_page}-{end_page}\n\n"
                    f"책 소개(원문):\n{description}\n\n"
                    "반드시 아래 키만 갖는 JSON 한 개로만 응답한다(앞뒤 설명 문장 금지).\n"
                    "{\n"
                    '  "summary_lines": [\n'
                    f'    "실제 줄1: 반드시 p.{start_page}-{end_page} 이 구간 독서에 직결되는 첫 문장",\n'
                    '    "실제 줄2~: 위와 겹치지 않는 다른 조언·질문·주제 각각 한 줄로", ...\n'
                    "  ],\n"
                    '  "encouragement": "응원 한 줄",\n'
                    '  "quiz": {\n'
                    '    "question": "짧은 질문(스포일러 금지)",\n'
                    '    "options": ["보기1","보기2","보기3","보기4"],\n'
                    '    "correct_index": 0,\n'
                    '    "explanation": "정답 후 한 줄 설명(선택, 짧게)"\n'
                    "  },\n"
                    '  "discussion": "모임에서 나눌 토론 질문 또는 화두 1~2문장"\n'
                    "}\n"
                    "중요 규칙:\n"
                    f"- summary_lines는 **{_WEEKLY_SUMMARY_LINE_SOFT_MIN}개 이상, 최대 {_WEEKLY_SUMMARY_LINE_CAP}개** 문자열(한 줄 하나). 목표 분량은 **약 {_WEEKLY_SUMMARY_LINE_TARGET}줄**. "
                    "짧은 뻔한 헤더 문장 반복 금지, 추상어만 늘어놓은 문장 피하기. 책 소개에 없는 디테일·인용·등장 이름을 만들어내면 안 된다.\n"
                    f"- 각 줄 정보 밀도를 높이라. 이 구간 진도 안에서 무엇을 유의해서 읽을지, 책 소개에 근거한 주제 하나, 북클럽 대화 거리, 페이지 넘김 속도 조절·메모 포인트 등을 섞어도 좋다(단, 허위 사실 금지).\n"
                    f"- summary_lines는 **반드시 p.{start_page}-{end_page} 구간**(전체 페이지 수 대비 이 묶음 진도 의미 포함) 안에서 무엇을 어떻게 읽을지 구체적으로. "
                    "책 전체를 한 줄로 압축한 문단처럼 들리면 안 된다.\n"
                    "- 다른 주차와 초점 어구·독법·질문 유형이 겹치면 안 된다. 같은 문단을 다른 주차에 재사용하면 안 된다.\n"
                    "보기는 정확히 4개, 서로 다르게. "
                    f"correct_index는 0~3 정수. quiz·discussion은 기존대로 과도하게 길지 않게."
                ),
            },
        ],
        temperature=0.72,
        max_tokens=2600,
        response_format={"type": "json_object"},
    )
    raw = (resp.choices[0].message.content or "").strip()
    summary = ""
    encouragement = "이번 주도 한 걸음씩 같이 읽어봐요."
    discussion = ""
    quiz_json = "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()][:_WEEKLY_SUMMARY_LINE_CAP]
        summary = "\n".join(lines) if lines else f"p.{start_page}-{end_page} 분량을 여유 있게 읽어 가면 돼요."
        return summary, encouragement, quiz_json, discussion

    lines_in = data.get("summary_lines")
    if isinstance(lines_in, list):
        parts = [str(x).strip() for x in lines_in if str(x).strip()][:_WEEKLY_SUMMARY_LINE_CAP]
        summary = "\n".join(parts)
    if not summary.strip():
        summary = (data.get("summary") or "").strip() or f"p.{start_page}-{end_page} 분량을 여유 있게 읽어 가면 돼요."
    enc = (data.get("encouragement") or "").strip()
    if enc:
        encouragement = enc[:300]
    discussion = (data.get("discussion") or "").strip()[:800]
    quiz_json = _weekly_quiz_json_from_llm_payload(data)
    capped = [ln.strip() for ln in summary.splitlines() if ln.strip()][:_WEEKLY_SUMMARY_LINE_CAP]
    return "\n".join(capped), encouragement, quiz_json, discussion


def _parse_quiz_for_poll(quiz_json: str) -> Optional[Tuple[str, List[str], int, Optional[str]]]:
    if not quiz_json or quiz_json.strip() in ("", "{}"):
        return None
    try:
        d = json.loads(quiz_json)
    except json.JSONDecodeError:
        return None
    q = (d.get("question") or "").strip()
    options = d.get("options")
    if not isinstance(options, list) or len(options) != 4:
        return None
    opts = [str(o).strip()[:100] for o in options]
    if not all(opts):
        return None
    try:
        correct = int(d.get("correct_index"))
    except (TypeError, ValueError):
        return None
    if correct not in (0, 1, 2, 3):
        return None
    expl_raw = (d.get("explanation") or "").strip()
    explanation: Optional[str] = expl_raw[:200] if expl_raw else None
    return (q, opts, correct, explanation)


def _plan_has_valid_quiz(plan: MonthlyWeeklyPlan) -> bool:
    return _parse_quiz_for_poll(plan.quiz_json) is not None


async def _send_weekly_check_and_quiz(bot: Bot, *, chat_id: str, cfg: WeeklyCheckConfig, quiz_json: str) -> None:
    text, markup = build_weekly_check_message(cfg)
    msg = await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
    # Best-effort: remember last message id for admin delete.
    try:
        settings = getattr(bot, "_app_settings", None)  # type: ignore[attr-defined]
        if settings is not None:
            _remember_last_member_message(settings, message_id=getattr(msg, "message_id"))
    except Exception:
        pass
    parsed = _parse_quiz_for_poll(quiz_json)
    if not parsed:
        return
    q_raw, options, correct_id, explanation = parsed
    label = f"[{cfg.month} {cfg.week_number}주차] "
    room = max(24, 300 - len(label))
    question = label + q_raw[:room]
    try:
        poll_msg = await bot.send_poll(
            chat_id=chat_id,
            question=question,
            options=options,
            type="quiz",
            correct_option_id=correct_id,
            is_anonymous=True,
            allows_multiple_answers=False,
            explanation=explanation,
        )
        try:
            settings = getattr(bot, "_app_settings", None)  # type: ignore[attr-defined]
            if settings is not None:
                _remember_last_member_message(settings, message_id=getattr(poll_msg, "message_id"))
        except Exception:
            pass
    except TelegramError:
        logger.warning("weekly quiz poll failed", exc_info=True)


async def _send_weekly_check_only(bot: Bot, *, chat_id: str, cfg: WeeklyCheckConfig) -> None:
    safe_cfg = WeeklyCheckConfig(
        month=cfg.month,
        week_number=cfg.week_number,
        book_title=cfg.book_title,
        range_label=cfg.range_label,
        next_range_label=cfg.next_range_label,
        summary=cfg.summary,
        encouragement=cfg.encouragement,
        discussion_topic="",
        show_quiz_teaser=False,
    )
    text, markup = build_weekly_check_message(safe_cfg)
    msg = await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
    try:
        settings = getattr(bot, "_app_settings", None)  # type: ignore[attr-defined]
        if settings is not None:
            _remember_last_member_message(settings, message_id=getattr(msg, "message_id"))
    except Exception:
        pass


def _chunk_text_for_telegram(text: str, limit: int = 3900) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    rest = text
    while rest:
        if len(rest) <= limit:
            chunks.append(rest)
            break
        cut = rest.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    return chunks


def _format_weekly_engagement_preview(plan: MonthlyWeeklyPlan) -> str:
    lines = [
        f"📋 {plan.month} {plan.week_number}주차 콘텐츠 (저장본 미리보기)",
        f"- 시작일(스케줄): {plan.scheduled_date}",
        f"- 읽기 범위: p.{plan.start_page}-{plan.end_page}",
        "",
        "━━ 요약 ━━",
        plan.summary.strip() or "(없음)",
        "",
        "━━ 응원 한 줄 ━━",
        plan.encouragement.strip() or "(없음)",
        "",
        "━━ 미니 퀴즈 ━━",
    ]
    parsed = _parse_quiz_for_poll(plan.quiz_json)
    if not parsed:
        lines.append("(유효한 퀴즈 없음 — /rebuild_weekly 로 다시 만들 수 있어요)")
    else:
        q, opts, correct_id, expl = parsed
        lines.append(f"질문: {q}")
        for i, o in enumerate(opts):
            mark = " ← 정답" if i == correct_id else ""
            lines.append(f"  {i + 1}) {o}{mark}")
        if expl:
            lines.extend(["", f"해설: {expl}"])
    lines.extend(["", "━━ 토론 주제 ━━", (plan.discussion_topic or "").strip() or "(없음)"])
    lines.extend(
        [
            "",
            "마음에 들면 /send_weekly_quiz 또는 /send_weekly_topic 으로 멤버방에 보내세요.",
        ]
    )
    return "\n".join(lines)


def _build_weekly_page_ranges(total_pages: int) -> List[Tuple[int, int]]:
    base = total_pages // 4
    remainder = total_pages % 4
    ranges: List[Tuple[int, int]] = []
    current = 1
    for idx in range(4):
        size = base + (1 if idx < remainder else 0)
        end = current + size - 1
        ranges.append((current, max(current, end)))
        current = end + 1
    return ranges


def _build_month_week_schedule(meeting_dt: datetime) -> List[str]:
    meeting_date = meeting_dt.date()
    return [datetime.fromordinal(meeting_date.toordinal() - 28 + 7 * i).strftime("%Y-%m-%d") for i in range(4)]


def _load_monthly_weekly_plans(settings: Settings, *, month: str) -> List[MonthlyWeeklyPlan]:
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            return list_monthly_weekly_plans_postgres(conn, month=month)
        finally:
            conn.close()
    conn = connect_sqlite(settings.db_path)
    try:
        return list_monthly_weekly_plans_sqlite(conn, month=month)
    finally:
        conn.close()


async def send_due_weekly_checks(app: Application) -> int:
    settings: Settings = app.bot_data["settings"]
    # Use configured timezone so "Friday" and date boundaries match the club's locale.
    try:
        from zoneinfo import ZoneInfo

        today_iso = datetime.now(tz=ZoneInfo(settings.timezone or "Asia/Seoul")).strftime("%Y-%m-%d")
    except Exception:
        today_iso = datetime.now().strftime("%Y-%m-%d")
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            due_plans = list_due_unsent_weekly_plans_postgres(conn, today_iso=today_iso)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            due_plans = list_due_unsent_weekly_plans_sqlite(conn, today_iso=today_iso)
        finally:
            conn.close()

    sent_count = 0
    for plan in due_plans:
        month_plans = _load_monthly_weekly_plans(settings, month=plan.month)
        cfg = _weekly_check_cfg_from_plans(plan.month, plan.week_number, month_plans)
        if cfg is None:
            continue
        info = _load_club_book_info(settings, month=plan.month)
        cfg = WeeklyCheckConfig(**{**cfg.__dict__, "book_title": (info.get("title") or "").strip()})
        # 자동 발송은 '진도 체크'만. 퀴즈/토론은 운영진이 필요할 때 별도 전송.
        await _send_weekly_check_only(app.bot, chat_id=settings.member_chat_id, cfg=cfg)
        if is_postgres_url(settings.database_url):
            conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
            try:
                mark_weekly_plan_sent_postgres(conn, month=plan.month, week_number=plan.week_number)
            finally:
                conn.close()
        else:
            conn = connect_sqlite(settings.db_path)
            try:
                mark_weekly_plan_sent_sqlite(conn, month=plan.month, week_number=plan.week_number)
            finally:
                conn.close()
        sent_count += 1
    return sent_count


def _format_weekly_stats_message(
    month: str,
    week_number: int,
    plans: List[MonthlyWeeklyPlan],
    stats: List[object],
    members: List[object],
    *,
    include_members: bool = False,
) -> str:
    counts = {"done": 0, "partial": 0, "not_yet": 0}
    for stat in stats:
        counts[getattr(stat, "status")] = int(getattr(stat, "count"))
    total = sum(counts.values())
    plan = next((p for p in plans if p.week_number == week_number), None)
    lines = [
        f"{month} {week_number}주차 진도 통계",
        f"- 응답 수: {total}명",
        f"- 완료: {counts['done']}명",
        f"- 부분: {counts['partial']}명",
        f"- 아직: {counts['not_yet']}명",
    ]
    if plan is not None:
        lines.insert(1, f"- 범위: p.{plan.start_page}-{plan.end_page}")
    if include_members and members:
        status_map = {
            "done": "완료",
            "partial": "부분",
            "not_yet": "아직",
        }
        for key in ("done", "partial", "not_yet"):
            labels = []
            for m in members:
                if getattr(m, "status") != key:
                    continue
                username = getattr(m, "telegram_username")
                full_name = getattr(m, "full_name")
                uid = getattr(m, "telegram_user_id")
                labels.append(f"@{username}" if username else (full_name or str(uid)))
            if labels:
                lines.extend(["", f"{status_map[key]} 명단", ", ".join(labels)])
    return "\n".join(lines)


def _weekly_check_cfg_from_plans(month: str, week_number: int, plans: List[MonthlyWeeklyPlan]) -> Optional[WeeklyCheckConfig]:
    plan = next((p for p in plans if p.week_number == week_number), None)
    if plan is None:
        return None
    next_plan = next((p for p in plans if p.week_number == week_number + 1), None)
    encouragement = plan.encouragement
    if week_number == 4:
        encouragement = "이제 모임 전까지 남은 부분만 차근차근 읽어보면 돼요. 끝까지 화이팅이에요."
    return WeeklyCheckConfig(
        month=month,
        week_number=week_number,
        book_title="",
        range_label=f"p.{plan.start_page}-{plan.end_page}",
        next_range_label=(f"p.{next_plan.start_page}-{next_plan.end_page}" if next_plan else ""),
        summary=plan.summary,
        encouragement=encouragement,
        discussion_topic=plan.discussion_topic or "",
        show_quiz_teaser=_plan_has_valid_quiz(plan),
    )


async def cmd_build_book_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    # Members should never trigger OpenAI calls. Show cached book info instead.
    is_admin = await _is_member_of(settings.admin_chat_id, update, context)
    if not is_admin:
        info = _load_club_book_info(settings)
        await msg.reply_text(_format_book_info_message(info))
        return

    if not await _require_admin(update, context):
        return
    if not settings.openai_api_key:
        await msg.reply_text("이 기능을 사용하려면 운영진이 OPENAI_API_KEY를 설정해야 해요.")
        return

    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    force = bool(context.args and context.args[0].strip().lower() in ("force", "--force", "-f"))

    existing_summary = (info.get("summary") or "").strip()
    if existing_summary and not force:
        await msg.reply_text("이미 저장된 책 요약이 있어요. (재생성 필요하면: /build_book_summary force)\n\n" + existing_summary)
        return

    # Cooldown guard (admin에서도 연타 방지)
    last_iso = _get_month_setting(settings, month=month, key="book_summary_generated_at_iso")
    last_dt = _parse_iso_dt(last_iso)
    if last_dt and not force:
        delta_min = (datetime.utcnow() - last_dt).total_seconds() / 60.0
        if delta_min < _BUILD_COOLDOWN_MINUTES:
            await msg.reply_text(
                f"방금 생성한 요약이 있어요. {_BUILD_COOLDOWN_MINUTES}분 쿨다운 중입니다. "
                "필요하면 /build_book_summary force 로 재생성할 수 있어요."
            )
            return
    title = (info.get("title") or "").strip()
    authors = (info.get("authors") or "").strip() or "미상"
    description = (info.get("description") or "").strip()
    if not title or title == "(미설정)":
        await msg.reply_text("먼저 /set_book 또는 /book_select 로 책을 확정해줘요.")
        return

    searched_description = await _search_book_description_for_summary(
        title=title,
        authors=authors,
        api_key=settings.google_books_api_key,
    )
    description = searched_description or description
    if not description:
        await msg.reply_text(
            "책 제목/저자로 다시 찾아봤지만 소개를 충분히 찾지 못했어요. 다른 검색 결과를 선택하거나 수동 설명이 필요해요."
        )
        return

    try:
        summary = await asyncio.to_thread(
            _get_openai_book_summary,
            settings.openai_api_key,
            settings.openai_summary_model,
            title=title,
            authors=authors,
            description=description,
        )
    except Exception:
        logger.info("Failed to build book summary", exc_info=True)
        await msg.reply_text("지금은 책 요약을 만들지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    if not summary:
        await msg.reply_text("요약 결과가 비어있어요. 잠시 후 다시 시도해줘요.")
        return
    # 카피는 한 덩어리 서술형이라 줄 수 제한으로 잘리지 않게 길이만 완화 제한
    summary = _normalize_book_summary_copy(summary)
    summary = "\n".join(ln.rstrip() for ln in summary.splitlines()).strip()
    if len(summary) > 3200:
        summary = summary[:3190] + "…"

    _set_month_setting(settings, month=month, key="book_summary", value=summary)
    _set_month_setting(settings, month=month, key="book_summary_generated_at_iso", value=_now_iso())

    await msg.reply_text("책 요약을 저장했어요.\n\n" + summary)


async def cmd_build_month_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    # Members should never trigger OpenAI calls. Show cached plan instead.
    is_admin = await _is_member_of(settings.admin_chat_id, update, context)
    if not is_admin:
        await cmd_show_month_plan(update, context)
        return

    if not await _require_admin(update, context):
        return
    if not settings.openai_api_key:
        await msg.reply_text("이 기능을 사용하려면 운영진이 OPENAI_API_KEY를 설정해야 해요.")
        return

    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    force = bool(context.args and context.args[0].strip().lower() in ("force", "--force", "-f"))

    # Cache: if plan exists, don't call LLM again unless forced.
    existing = _load_monthly_weekly_plans(settings, month=month)
    if len(existing) >= 4 and not force:
        await msg.reply_text(
            "이미 저장된 4주 계획이 있어요. 멤버는 /show_month_plan 으로 확인하면 됩니다.\n"
            "재생성 필요하면: /build_month_plan force"
        )
        return

    # Cooldown guard (admin에서도 연타 방지)
    last_iso = _get_month_setting(settings, month=month, key="month_plan_generated_at_iso")
    last_dt = _parse_iso_dt(last_iso)
    if last_dt and not force:
        delta_min = (datetime.utcnow() - last_dt).total_seconds() / 60.0
        if delta_min < _BUILD_COOLDOWN_MINUTES:
            await msg.reply_text(
                f"방금 생성한 4주 계획이 있어요. {_BUILD_COOLDOWN_MINUTES}분 쿨다운 중입니다. "
                "필요하면 /build_month_plan force 로 재생성할 수 있어요."
            )
            return
    title = (info.get("title") or "").strip()
    authors = (info.get("authors") or "").strip() or "미상"
    description = (info.get("description") or "").strip()
    meeting_at = (info.get("meeting_at") or "").strip()
    page_count_raw = (info.get("page_count") or "").strip()
    meeting_dt = _parse_meeting_date_for_plan(meeting_at)
    if not title:
        await msg.reply_text(
            "\n".join(
                [
                    "책 정보가 아직 없어요.",
                    f"- 운영진이 `{settings.book_catalog_path}` 에서 해당 월(YYYY-MM)의 title/authors 등을 채워줘요.",
                ]
            )
        )
        return
    if meeting_dt is None:
        await msg.reply_text(
            "\n".join(
                [
                    "모임 일정이 아직 없어요.",
                    f"- 운영진이 `{settings.book_catalog_path}` 에서 해당 월(YYYY-MM)의 meeting_at 을 채워줘요.",
                ]
            )
        )
        return
    if not page_count_raw.isdigit():
        await msg.reply_text(
            "\n".join(
                [
                    "총 페이지 수가 아직 없어요.",
                    f"- 운영진이 `{settings.book_catalog_path}` 에서 해당 월(YYYY-MM)의 page_count 를 채워줘요.",
                ]
            )
        )
        return
    if not description:
        await msg.reply_text("책 소개가 아직 없어요. 책 검색 후 선택한 책으로 진행해줘요.")
        return

    page_ranges = _build_weekly_page_ranges(int(page_count_raw))
    schedule_dates = _build_month_week_schedule(meeting_dt)

    try:
        generated: List[Tuple[str, str, str, str]] = []
        for idx, (start_page, end_page) in enumerate(page_ranges, start=1):
            generated.append(
                await asyncio.to_thread(
                    _get_openai_weekly_plan_bundle,
                    settings.openai_api_key,
                    settings.openai_summary_model,
                    title=title,
                    authors=authors,
                    description=description,
                    month=month,
                    week_number=idx,
                    start_page=start_page,
                    end_page=end_page,
                )
            )
    except Exception:
        logger.info("Failed to build monthly weekly plan", exc_info=True)
        await msg.reply_text("지금은 4주 계획을 만들지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            for idx, (start_page, end_page) in enumerate(page_ranges, start=1):
                summary, encouragement, quiz_json, discussion_topic = generated[idx - 1]
                upsert_monthly_weekly_plan_postgres(
                    conn,
                    month=month,
                    week_number=idx,
                    start_page=start_page,
                    end_page=end_page,
                    summary=summary,
                    encouragement=encouragement,
                    scheduled_date=schedule_dates[idx - 1],
                    quiz_json=quiz_json,
                    discussion_topic=discussion_topic,
                )
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            for idx, (start_page, end_page) in enumerate(page_ranges, start=1):
                summary, encouragement, quiz_json, discussion_topic = generated[idx - 1]
                upsert_monthly_weekly_plan_sqlite(
                    conn,
                    month=month,
                    week_number=idx,
                    start_page=start_page,
                    end_page=end_page,
                    summary=summary,
                    encouragement=encouragement,
                    scheduled_date=schedule_dates[idx - 1],
                    quiz_json=quiz_json,
                    discussion_topic=discussion_topic,
                )
        finally:
            conn.close()

    _set_month_setting(settings, month=month, key="month_plan_generated_at_iso", value=_now_iso())

    preview_lines = [f"{month} 4주 계획을 저장했어요.", "- 각 주차에 미니 퀴즈(투표)·토론 주제가 함께 들어가요."]
    for idx, (start_page, end_page) in enumerate(page_ranges, start=1):
        preview_lines.append(f"- {idx}주차: p.{start_page}-{end_page} / 시작 {schedule_dates[idx - 1]}")
    await msg.reply_text("\n".join(preview_lines))


async def cmd_show_month_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_member_or_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    is_admin = await _is_member_of(settings.admin_chat_id, update, context)
    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    page_count_raw = (info.get("page_count") or "").strip()
    plans = _load_monthly_weekly_plans(settings, month=month)
    if not plans:
        await msg.reply_text("아직 4주 계획이 없어요. 운영진이 /build_month_plan 을 먼저 실행해줘요.")
        return
    lines = [f"📘 {month} 4주 읽기 계획", ""]
    if not is_admin:
        lines.append("(주차별 미니 퀴즈·토론은 해당 주 시작일에 단체방에서 공개돼요.)")
        lines.append("")
    for plan in plans:
        first_line = ""
        if plan.summary:
            first_line = next((ln.strip() for ln in plan.summary.splitlines() if ln.strip()), "")
        extras: List[str] = []
        if is_admin:
            if _plan_has_valid_quiz(plan):
                extras.append("🧩 미니 퀴즈 1문항(발송 시 채팅에 퀴즈 투표로 올라가요)")
            disc = (plan.discussion_topic or "").strip()
            if disc:
                preview = disc if len(disc) <= 140 else disc[:137] + "…"
                extras.append(f"💬 토론: {preview}")
        lines.extend(
            [
                f"━━━━━━━━━━",
                f"✅ {plan.week_number}주차  ({plan.scheduled_date})",
                f"📖 범위: p.{plan.start_page}-{plan.end_page}",
                (f"📝 안내: {first_line}" if first_line else "📝 안내: (요약 없음)"),
                *extras,
                "",
            ]
        )

    if page_count_raw.isdigit():
        total_pages = int(page_count_raw)
        if total_pages > 0:
            per_day = (total_pages + 30 - 1) // 30
            lines.extend(
                [
                    "━━━━━━━━━━",
                    f"💪 30일 기준으로 하루 약 {per_day}p만 꾸준히 읽어도 충분해요. 오늘도 화이팅!",
                ]
            )
    await msg.reply_text("\n".join(lines))


async def cmd_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_member_or_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    info = _load_club_book_info(settings)
    await msg.reply_text(_format_book_info_message(info))

async def cmd_book_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_member_or_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    if not context.args:
        await msg.reply_text("사용법: /book_month 2026-04")
        return
    month = _parse_month_yyyy_mm(context.args[0])
    if not month:
        await msg.reply_text("사용법: /book_month 2026-04")
        return
    info = _load_club_book_info(settings, month=month)
    await msg.reply_text(_format_book_info_message(info))


async def cmd_set_puzzle_cover(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    if not settings.progress_game_enabled:
        await msg.reply_text("퍼즐 기능이 비활성화되어 있어요. PROGRESS_GAME_ENABLED=true 로 켜주세요.")
        return

    reply = getattr(msg, "reply_to_message", None)
    photos = getattr(reply, "photo", None) if reply else None
    if not photos:
        await msg.reply_text("사용법: 대표 이미지가 있는 사진 메시지에 답장한 뒤 /set_puzzle_cover 를 실행해줘요.")
        return

    month = _get_active_month(settings)
    largest = photos[-1]
    file_id = getattr(largest, "file_id", None)
    if not file_id:
        await msg.reply_text("대표 이미지 저장에 실패했어요. 다른 사진으로 다시 시도해줘요.")
        return

    seed = int(datetime.utcnow().timestamp())
    _save_month_puzzle_meta(settings, month=month, file_id=file_id, seed=seed)
    await msg.reply_text(f"{month} 퍼즐 대표 이미지를 저장했어요.")


async def cmd_show_puzzle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    if not settings.progress_game_enabled:
        await msg.reply_text("퍼즐 기능이 비활성화되어 있어요. PROGRESS_GAME_ENABLED=true 로 켜주세요.")
        return

    month = _get_active_month(settings)
    info = _load_club_book_info(settings, month=month)
    page_count_raw = (info.get("page_count") or "").strip()
    if not page_count_raw.isdigit():
        await msg.reply_text(
            "\n".join(
                [
                    "총 페이지 수가 아직 없어요.",
                    f"- 운영진이 `{settings.book_catalog_path}` 에서 해당 월(YYYY-MM)의 page_count 를 채워줘요.",
                ]
            )
        )
        return
    total_pages = int(page_count_raw)
    if not context.args or not context.args[0].isdigit():
        await msg.reply_text("사용법: /show_puzzle <읽은페이지>\n예) /show_puzzle 120")
        return
    pages_read = int(context.args[0])
    percent = calculate_progress_percent(pages_read=pages_read, total_pages=total_pages)
    revealed = calculate_revealed_tiles(progress_percent=percent, total_tiles=settings.progress_game_grid_size)

    file_id, seed = _load_month_puzzle_meta(settings, month=month)
    text_grid = render_text_grid(revealed_tiles=revealed, total_tiles=settings.progress_game_grid_size, cols=10)
    caption = "\n".join(
        [
            f"{month} 퍼즐 진행도",
            f"- 책: {info.get('title') or '(미설정)'}",
            f"- 읽은 페이지: {pages_read}/{total_pages}p ({percent}%)",
            f"- 공개 칸: {revealed}/{settings.progress_game_grid_size}",
            "",
            text_grid,
        ]
    )
    if not file_id or seed is None:
        await msg.reply_text(caption + "\n\n대표 이미지가 아직 없어요. 사진에 답장한 뒤 /set_puzzle_cover 를 실행해줘요.")
        return

    try:
        telegram_file = await context.bot.get_file(file_id)
        image_bytes = bytes(await telegram_file.download_as_bytearray())
        puzzle_bytes = render_image_puzzle(
            image_bytes=image_bytes,
            revealed_tiles=revealed,
            total_tiles=settings.progress_game_grid_size,
            seed=seed,
        )
    except Exception:
        logger.info("Failed to render puzzle image", exc_info=True)
        await msg.reply_text(caption + "\n\n이미지 퍼즐 생성에 실패해서 텍스트 퍼즐만 보여줘요.")
        return

    await context.bot.send_photo(
        chat_id=msg.chat_id,
        photo=BytesIO(puzzle_bytes),
        caption=caption,
    )


async def cmd_send_book_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    info = _load_club_book_info(settings)
    text = _format_book_info_message(info)
    sent = await context.bot.send_message(chat_id=settings.member_chat_id, text=text)
    _remember_last_member_message(settings, message_id=sent.message_id)
    await msg.reply_text("멤버 단체방에 책 정보를 전송했어요.")


async def cmd_test_book_videos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    if msg is None:
        return
    settings: Settings = context.application.bot_data["settings"]
    info = _load_club_book_info(settings)
    text = _format_book_videos_message(info)
    if text is None:
        await msg.reply_text("현재 대상 책에 등록된 영상 링크가 없어요. data/book_catalog.json의 trailer_link/trailer_links를 확인해줘요.")
        return
    await msg.reply_text(
        "\n".join(
            [
                "🧪 운영진 테스트 미리보기",
                "멤버방에는 발송되지 않았어요.",
                "",
                text,
            ]
        )
    )


async def cmd_send_book_videos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    if msg is None:
        return
    settings: Settings = context.application.bot_data["settings"]
    info = _load_club_book_info(settings)
    text = _format_book_videos_message(info)
    if text is None:
        await msg.reply_text("현재 대상 책에 등록된 영상 링크가 없어요. data/book_catalog.json의 trailer_link/trailer_links를 확인해줘요.")
        return
    sent = await context.bot.send_message(chat_id=settings.member_chat_id, text=text)
    _remember_last_member_message(settings, message_id=sent.message_id)
    await msg.reply_text("멤버 단체방에 이번 책 관련 영상 자료를 전송했어요.")


def _parse_bookmark_args(arg_text: str) -> tuple[Optional[int], str]:
    """
    Supported formats:
    - /bookmark <text>
    - /bookmark <page> | <text>
    - /bookmark p<page> | <text>   (e.g. p120 | ...)
    """
    raw = (arg_text or "").strip()
    if not raw:
        return None, ""

    if "|" not in raw:
        return None, raw

    left, right = raw.split("|", 1)
    left = left.strip()
    right = right.strip()
    if not right:
        return None, ""

    left_norm = left.lower().lstrip("p").strip()
    if left_norm.isdigit():
        return int(left_norm), right

    return None, raw  # fallback: treat whole as text if page parsing fails


async def cmd_bookmark(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    arg_text = " ".join(context.args) if context.args else ""
    page, text = _parse_bookmark_args(arg_text)
    if not text:
        await msg.reply_text("사용법: /bookmark 120 | 인상 깊은 문장\n또는: /bookmark 인상 깊은 문장")
        return

    full_name = " ".join([p for p in [user.first_name, user.last_name] if p]).strip() or None
    max_per_user = min(100, max(1, int(settings.bookmarks_max_per_user)))

    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            insert_bookmark_postgres(
                conn,
                telegram_user_id=user.id,
                telegram_username=user.username,
                full_name=full_name,
                page=page,
                text=text,
            )
            enforce_bookmarks_limit_postgres(
                conn, telegram_user_id=user.id, max_per_user=max_per_user
            )
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            insert_bookmark_sqlite(
                conn,
                telegram_user_id=user.id,
                telegram_username=user.username,
                full_name=full_name,
                page=page,
                text=text,
            )
            enforce_bookmarks_limit_sqlite(conn, telegram_user_id=user.id, max_per_user=max_per_user)
        finally:
            conn.close()

    page_part = f"(p.{page}) " if page is not None else ""
    await msg.reply_text(f"저장했어요. {page_part}{text}")


async def cmd_bookmarks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    limit = 10
    query = None
    if context.args:
        if context.args[0].isdigit():
            limit = max(1, min(30, int(context.args[0])))
            if len(context.args) > 1:
                query = " ".join(context.args[1:]).strip() or None
        else:
            query = " ".join(context.args).strip() or None

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=user.id, query=query, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=user.id, query=query, limit=limit)
        finally:
            conn.close()

    if not items:
        await msg.reply_text("저장된 문장이 아직 없어요. /bookmark 로 먼저 저장해보세요.")
        return

    header = "내가 저장한 문장(최근 순, 기본 10개)" if not query and limit == 10 else "내가 저장한 문장(최근 순)"
    if query:
        header = f"검색 결과: {query}"
    lines = [header]
    for i, b in enumerate(items, start=1):
        page_part = f"p.{b.page} " if b.page is not None else ""
        lines.append(f"- #{b.id} {page_part}\"{b.text}\"")
    await msg.reply_text("\n".join(lines))


async def cmd_bookmark_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    if not context.args:
        await msg.reply_text("사용법: /bookmark_edit #id 수정할 문장\n또는: /bookmark_edit #id 페이지번호 | 수정할 문장")
        return

    first = context.args[0].strip()
    if not first.startswith("#") or not first[1:].isdigit():
        await msg.reply_text("사용법: /bookmark_edit #id 페이지번호 | 수정할 문장\n예) /bookmark_edit #3 57 | 수정할 문장")
        return

    bookmark_id = int(first[1:])
    raw = " ".join(context.args[1:]) if len(context.args) > 1 else ""
    page, text = _parse_bookmark_args(raw)
    if not text:
        await msg.reply_text("수정할 문장을 입력해주세요. 예) /bookmark_edit #3 57 | 수정할 문장")
        return

    ok = False
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            ok = update_bookmark_postgres(
                conn, bookmark_id=bookmark_id, telegram_user_id=user.id, page=page, text=text
            )
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            ok = update_bookmark_sqlite(
                conn, bookmark_id=bookmark_id, telegram_user_id=user.id, page=page, text=text
            )
        finally:
            conn.close()

    if not ok:
        await msg.reply_text("해당 #id의 책갈피를 찾지 못했어요. /bookmarks 로 확인해보세요.")
        return

    page_part = f"(p.{page}) " if page is not None else ""
    await msg.reply_text(f"수정했어요. #{bookmark_id} {page_part}{text}")


async def cmd_bookmark_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    if not context.args:
        await msg.reply_text("사용법: /bookmark_delete #id\n예) /bookmark_delete #3")
        return

    first = context.args[0].strip()
    if not first.startswith("#") or not first[1:].isdigit():
        await msg.reply_text("사용법: /bookmark_delete #id\n예) /bookmark_delete #3")
        return

    bookmark_id = int(first[1:])
    ok = False
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            ok = delete_bookmark_postgres(conn, bookmark_id=bookmark_id, telegram_user_id=user.id)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            ok = delete_bookmark_sqlite(conn, bookmark_id=bookmark_id, telegram_user_id=user.id)
        finally:
            conn.close()

    if not ok:
        await msg.reply_text("해당 #id의 책갈피를 찾지 못했어요. /bookmarks 로 확인해보세요.")
        return

    await msg.reply_text(f"삭제했어요. #{bookmark_id}")


def _extract_keywords(texts: List[str]) -> List[str]:
    # Lightweight keyword extraction (space-based) with better Korean stopwords.
    stop = {
        "그리고",
        "그런데",
        "하지만",
        "그래서",
        "또",
        "또는",
        "이건",
        "저건",
        "그것",
        "이것",
        "저것",
        "저는",
        "나는",
        "내가",
        "너무",
        "정말",
        "그냥",
        "진짜",
        "약간",
        "보이는",
        "것",
        "것조차",
        "수",
        "등",
    }
    tokens: List[str] = []
    for t in texts:
        # Drop author suffix like " - 마르쿠스 아우렐리우스"
        t = t.split(" - ")[0]
        for raw in t.replace("\n", " ").split():
            w = raw.strip(".,!?\"'()[]{}:;~`")
            if len(w) < 2:
                continue
            if w in stop:
                continue
            tokens.append(w)
    return [w for w, _ in Counter(tokens).most_common(8)]


def _dot(a: List[float], b: List[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _l2(a: List[float]) -> float:
    return max(1e-12, sum(x * x for x in a) ** 0.5)


def _cosine(a: List[float], b: List[float]) -> float:
    return _dot(a, b) / (_l2(a) * _l2(b))


def _cluster_embeddings(vectors: List[List[float]], threshold: float = 0.82) -> List[List[int]]:
    """
    Greedy clustering by cosine similarity against cluster centroid.
    Returns clusters of indices.
    """
    clusters: List[List[int]] = []
    centroids: List[List[float]] = []

    for idx, v in enumerate(vectors):
        best_i = -1
        best_sim = -1.0
        for ci, c in enumerate(centroids):
            sim = _cosine(v, c)
            if sim > best_sim:
                best_sim = sim
                best_i = ci

        if best_sim >= threshold and best_i >= 0:
            clusters[best_i].append(idx)
            # update centroid (simple mean)
            inds = clusters[best_i]
            dim = len(v)
            new_c = [0.0] * dim
            for j in inds:
                vv = vectors[j]
                for k in range(dim):
                    new_c[k] += vv[k]
            n = float(len(inds))
            centroids[best_i] = [x / n for x in new_c]
        else:
            clusters.append([idx])
            centroids.append(v)

    # sort by size desc
    clusters.sort(key=len, reverse=True)
    return clusters


_EMBED_BATCH_SIZE = 16
_EMBED_MAX_CHARS = 5500


def _truncate_for_embedding(text: str, max_chars: int) -> str:
    s = (text or "").strip()
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1] + "…"


def _get_openai_embeddings(api_key: str, model: str, texts: List[str]) -> List[List[float]]:
    """Fetch embeddings; truncates long lines and batches requests to reduce OpenAI errors."""
    client = OpenAI(api_key=api_key)
    trimmed = [_truncate_for_embedding(t, _EMBED_MAX_CHARS) for t in texts]
    vectors: List[List[float]] = []
    for i in range(0, len(trimmed), _EMBED_BATCH_SIZE):
        batch = trimmed[i : i + _EMBED_BATCH_SIZE]
        resp = client.embeddings.create(model=model, input=batch)
        data = sorted(resp.data, key=lambda d: d.index)
        vectors.extend([d.embedding for d in data])
    if len(vectors) != len(texts):
        raise TypeError(f"embeddings length mismatch: got {len(vectors)} for {len(texts)} texts")
    return vectors


def _taste_snapshot_from_bookmarks(
    *,
    bookmarks: List[Bookmark],
    embeddings: List[List[float]],
    max_clusters: int,
) -> Tuple[str, List[str]]:
    if len(bookmarks) <= 2:
        # With too little data, don't pretend to infer themes.
        reps = []
        for b in bookmarks[:2]:
            page_part = f"p.{b.page} " if b.page is not None else ""
            reps.append(f"- {page_part}\"{b.text}\"")
        header = "\n".join(
            [
                "내 독서 취향 스냅샷",
                f"- 분석 기준: 최근 {len(bookmarks)}개 책갈피",
                "",
                "아직 책갈피가 적어서 취향을 요약하기엔 데이터가 부족해요.",
                "문장을 몇 개 더 저장하면 테마가 더 잘 잡혀요.",
                "",
            ]
        )
        return header + "\n".join(reps), []

    texts = [b.text for b in bookmarks]
    clusters = _cluster_embeddings(embeddings, threshold=0.82)[:max_clusters]

    sections: List[str] = []
    theme_lines: List[str] = []
    for ci, inds in enumerate(clusters, start=1):
        cluster_texts = [texts[i] for i in inds]
        kws = _extract_keywords(cluster_texts)[:5]
        theme = " / ".join(kws) if kws else "주제"
        theme_lines.append(theme)

        # representative quotes: first 2 items in cluster order
        reps = []
        for i in inds[:2]:
            b = bookmarks[i]
            page_part = f"p.{b.page} " if b.page is not None else ""
            reps.append(f"- {page_part}\"{b.text}\"")

        sections.append("\n".join([f"{ci}) {theme}", *reps]))

    header = "\n".join(
        [
            "내 독서 취향 스냅샷",
            f"- 분석 기준: 최근 {len(bookmarks)}개 책갈피",
            f"- 주요 테마: {', '.join(theme_lines)}" if theme_lines else "- 주요 테마: (분석 중)",
            "",
        ]
    )
    return header + "\n\n".join(sections), theme_lines


def _select_representative_bookmarks(
    bookmarks: List[Bookmark],
    embeddings: List[List[float]],
    *,
    max_clusters: int,
    max_quotes: int,
) -> List[Bookmark]:
    if not bookmarks:
        return []
    if len(bookmarks) <= max_quotes:
        return bookmarks
    clusters = _cluster_embeddings(embeddings, threshold=0.82)[: max(1, max_clusters)]
    reps: List[Bookmark] = []
    for inds in clusters:
        for i in inds[:2]:
            reps.append(bookmarks[i])
            if len(reps) >= max_quotes:
                return reps
    return reps[:max_quotes]


def _build_taste_summary_prompt(quotes: List[Bookmark]) -> str:
    lines: List[str] = []
    for b in quotes:
        page_part = f"(p.{b.page}) " if b.page is not None else ""
        lines.append(f"- {page_part}{b.text}")
    return "\n".join(lines)


_TASTE_LLM_MAX_ITEMS = 100
_TASTE_LLM_PER_ITEM_CHARS = 2000
_TASTE_LLM_TOTAL_CHARS = 14000


def _load_global_book_title(settings: Settings) -> Optional[str]:
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            return get_setting_postgres(conn, key="book_title")
        finally:
            conn.close()
    conn = connect_sqlite(settings.db_path)
    try:
        return get_setting_sqlite(conn, key="book_title")
    finally:
        conn.close()


def _pack_bookmarks_for_taste_llm(bookmarks: List[Bookmark]) -> Tuple[str, int, int]:
    """Pack bookmarks into one blob for the LLM. Returns (text, included_count, fetched_count)."""
    fetched = len(bookmarks)
    parts: List[str] = []
    total_chars = 0
    included = 0
    for b in bookmarks[:_TASTE_LLM_MAX_ITEMS]:
        raw = (b.text or "").strip()
        if not raw:
            continue
        piece = _truncate_for_embedding(raw, _TASTE_LLM_PER_ITEM_CHARS)
        page = f"p.{b.page} " if b.page is not None else ""
        line = f"[#{b.id}] {page}{piece}"
        add_len = len(line) + 1
        if total_chars + add_len > _TASTE_LLM_TOTAL_CHARS:
            break
        parts.append(line)
        total_chars += add_len
        included += 1
    return "\n".join(parts), included, fetched


def _get_openai_taste_summary_card(
    api_key: str,
    model: str,
    *,
    bulk_text: str,
    meta_note: str,
    book_title: Optional[str],
) -> str:
    """Single-message 취향 써머리: 전체 북마크 인풋, 5~10줄, 이모지·정렬감 있는 톤."""
    client = OpenAI(api_key=api_key)
    book_part = ""
    if book_title and str(book_title).strip():
        book_part = f"\n[참고: 모임 책 제목] {str(book_title).strip()}\n"
    system = (
        "너는 텔레그램 북클럽 봇이 보내는 메시지를 쓰는 독서 코치야. "
        "입력은 한 독자가 저장한 책갈피(인용·메모) 전체다. 그 텍스트만 근거로 독서 취향을 조망한다. "
        "근거 없는 추측·심리 진단·단정적 평가는 금지. '~로 보이는 경향'처럼 완곡하게. 한국어만."
    )
    user = (
        f"{meta_note}{book_part}"
        "아래는 이 독자의 책갈피를 시간 순으로 모은 것이다 (최신 위주, 분량 제한이 있으면 메타에 적혀 있음).\n\n"
        f"{bulk_text}\n\n"
        "출력 형식 (텔레그램에 그대로 보낼 한 덩어리 문자열):\n"
        "- 전체 줄 수 **5줄 이상 10줄 이하** (빈 줄 없이, 줄마다 한 가지 생각).\n"
        "- 1줄째: 인사/후크. 예시 느낌 — '📚 …하시는 독자이시군요!' 처럼 따뜻하게 (문장은 인용 그대로 복붙하지 말 것).\n"
        "- 2~(끝-2)줄: 저장 문장들을 **전체적으로 조망**한 취향 요약. 주제·가치·정서·문장 취향이 어떻게 겹치는지. "
        "필요하면 줄 앞에 가벼운 이모지 1개(✨ 💭 🔖 등)만 붙여도 됨. `-` `1.` 같은 마크다운 목록은 쓰지 말 것.\n"
        "- 마지막에서 두 번째 줄: 오늘도 잠깐이라도 읽기를 권하는 한 줄 (이모지 1개 포함).\n"
        "- 마지막 줄: 기억에 남는 문장을 더 모으라는 독려 한 줄 (이모지 1개 포함, 질문으로 끝내지 말 것).\n"
        "- 책갈피가 매우 적으면 한두 줄 안에 한계를 짧게 인정하고, 가능한 범위만 말하기.\n"
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.55,
        max_tokens=450,
    )
    return (resp.choices[0].message.content or "").strip()


async def _reply_telegram_chunks(msg, text: str, chunk_size: int = 4000) -> None:
    t = (text or "").strip()
    if not t:
        await msg.reply_text("응답이 비어 있어요.")
        return
    for i in range(0, len(t), chunk_size):
        await msg.reply_text(t[i : i + chunk_size])


def _get_openai_taste_summary(api_key: str, model: str, prompt: str) -> str:
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "너는 온라인 독서모임을 돕는 도우미야. 사용자의 책갈피 문장들을 보고 독서 취향을 한국어로 요약해.",
            },
            {
                "role": "user",
                "content": (
                    "아래는 내가 저장한 책갈피 문장들이야.\n\n"
                    f"{prompt}\n\n"
                    "요청:\n"
                    "- 한국어로 1~3문장만\n"
                    "- 질문 금지\n"
                    "- 목록/불릿 금지\n"
                    "- 단정적인 진단/평가 톤 금지 (부드럽게 '경향' 정도로)\n"
                ),
            },
        ],
        temperature=0.5,
    )
    text = (resp.choices[0].message.content or "").strip()
    return text


async def cmd_taste_retired(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """구 `/taste` — 비노출 유지: 대체 명령을 안내 문구에 쓰지 않음."""
    if not await _require_private_chat(update):
        return
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    if msg is not None:
        await msg.reply_text("지원하지 않는 명령이에요.")


async def cmd_taste_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Admin-only: bulk bookmarks → LLM 취향 써머리 카드 (5~10줄).
    if not await _require_private_chat(update):
        return
    if not await _require_admin(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    openai_key = (settings.openai_api_key or "").strip()
    if not openai_key:
        await msg.reply_text("OPENAI_API_KEY 가 설정되어 있어야 해요.")
        return

    fetch_limit = min(100, max(1, int(settings.bookmarks_max_per_user)))
    items: List[Bookmark] = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=user.id, limit=fetch_limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=user.id, limit=fetch_limit)
        finally:
            conn.close()

    items = [b for b in items if (b.text or "").strip()]
    if not items:
        await msg.reply_text("요약할 책갈피가 없어요.")
        return

    bulk_text, included, fetched = _pack_bookmarks_for_taste_llm(items)
    if not bulk_text.strip():
        await msg.reply_text("책갈피 텍스트가 비어 있어요.")
        return

    if included < fetched:
        meta_note = f"(※ 저장 책갈피 {fetched}개 중 길이·개수 제한으로 최근 {included}개만 포함)\n"
    else:
        meta_note = f"(※ 저장 책갈피 {included}개 포함)\n"

    book_title = _load_global_book_title(settings)

    try:
        summary = await _with_openai_retries(
            _get_openai_taste_summary_card,
            openai_key,
            settings.openai_summary_model,
            bulk_text=bulk_text,
            meta_note=meta_note,
            book_title=book_title,
        )
    except AuthenticationError:
        await msg.reply_text("OpenAI API 키가 올바르지 않은 것 같아요. (OPENAI_API_KEY 확인 필요)")
        return
    except RateLimitError as e:
        await msg.reply_text(_rate_limit_hint(e))
        return
    except APIConnectionError:
        await msg.reply_text("네트워크 문제로 요약을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return
    except APIError as e:
        logger.info("OpenAI APIError while generating taste summary (bulk LLM)", exc_info=True)
        await msg.reply_text(f"OpenAI 오류로 요약을 만들지 못했어요. ({e.__class__.__name__})")
        return
    except Exception as e:
        logger.info("Failed to generate taste summary (bulk LLM)", exc_info=True)
        await msg.reply_text(f"지금은 요약을 만들지 못했어요. ({e.__class__.__name__})")
        return

    if not summary:
        await msg.reply_text("요약 결과가 비어있어요. 잠시 후 다시 시도해줘요.")
        return

    await _reply_telegram_chunks(msg, summary)


async def cmd_diag_taste(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only diagnostic for taste pipeline."""
    if not await _require_private_chat(update):
        return
    if not await _require_admin(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    embeddings_provider = _norm_flag(settings.embeddings_provider)
    has_openai_key = bool((settings.openai_api_key or "").strip())

    is_member = await _is_member_of(settings.member_chat_id, update, context)
    is_admin = await _is_member_of(settings.admin_chat_id, update, context)

    lines = [
        "취향 진단 (/taste_summary) — 책갈피 일괄 + 채팅완성(LLM)",
        f"- EMBEDDINGS_PROVIDER: {embeddings_provider or '(empty)'} (선택, /taste_summary 에는 불필요)",
        f"- has OPENAI_API_KEY: {'yes' if has_openai_key else 'no'}",
        f"- member chat membership check: {'ok' if is_member else 'fail'}",
        f"- admin chat membership check: {'ok' if is_admin else 'fail'}",
    ]

    # OpenAI smoke test: chat completion always relevant; embeddings optional (taste_member/club_taste).
    if has_openai_key:
        try:
            text = await asyncio.to_thread(
                _get_openai_taste_summary,
                (settings.openai_api_key or "").strip(),
                settings.openai_summary_model,
                "- diag",
            )
            lines.append(f"- chat completion call: {'ok' if bool((text or '').strip()) else 'empty'}")
        except Exception as e:
            lines.append(f"- chat completion call: fail ({e.__class__.__name__})")

        if embeddings_provider == "openai":
            try:
                _ = await asyncio.to_thread(
                    _get_openai_embeddings,
                    (settings.openai_api_key or "").strip(),
                    settings.openai_embeddings_model,
                    ["diag"],
                )
                lines.append("- embeddings call: ok (taste_member/club_taste 용)")
            except Exception as e:
                lines.append(f"- embeddings call: fail ({e.__class__.__name__})")
        else:
            lines.append("- embeddings call: skipped (taste_member/club_taste 만 필요)")

    await msg.reply_text("\n".join(lines))


async def cmd_set_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    _ = context
    await msg.reply_text(
        "\n".join(
            [
                "이제 /set_book 은 사용하지 않아요.",
                f"- 책 정보는 파일에서 관리해요: `{settings.book_catalog_path}`",
                "- 예) data/book_catalog.json 의 해당 월(YYYY-MM) 항목의 title/authors 등을 수정해줘요.",
            ]
        )
    )

def _parse_meeting_args(args: List[str]) -> Optional[str]:
    """
    Accepts:
    - YYYY-MM-DD
    - YYYY-MM-DD HH:MM
    Returns a normalized string stored in DB (local-time string).
    """
    if not args:
        return None
    if len(args) == 1:
        raw = args[0].strip()
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            return None
        return dt.strftime("%Y-%m-%d")
    if len(args) >= 2:
        raw = (args[0].strip() + " " + args[1].strip()).strip()
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M")
        except ValueError:
            return None
        return dt.strftime("%Y-%m-%d %H:%M")
    return None


async def cmd_set_meeting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    _ = context
    await msg.reply_text(
        "\n".join(
            [
                "이제 /set_meeting 은 사용하지 않아요.",
                f"- 모임 일정은 파일에서 관리해요: `{settings.book_catalog_path}`",
                "- 해당 월(YYYY-MM) 항목의 meeting_at 값을 수정해줘요. (예: 2026-04-10 20:00)",
            ]
        )
    )


async def cmd_show_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    info = _load_club_book_info(settings)
    month = info.get("month") or ""
    title = info.get("title") or "(미설정)"
    meeting_at = info.get("meeting_at") or "(미설정)"
    await msg.reply_text(
        "\n".join(
            [
                "📚 이달의 책 안내",
                f"🗓️ 기준 월: {month}",
                f"📖 책 제목: {title}",
                f"📅 모임 일정: {meeting_at}",
            ]
        )
    )


async def cmd_set_pages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    _ = context
    await msg.reply_text(
        "\n".join(
            [
                "이제 /set_pages 는 사용하지 않아요.",
                f"- 총 페이지 수는 파일에서 관리해요: `{settings.book_catalog_path}`",
                "- 해당 월(YYYY-MM) 항목의 page_count 값을 수정해줘요. (예: \"320\")",
            ]
        )
    )


def _parse_meeting_date_for_plan(meeting_at: str) -> Optional[datetime]:
    raw = (meeting_at or "").strip()
    if not raw:
        return None
    # stored formats: YYYY-MM-DD or YYYY-MM-DD HH:MM
    try:
        if len(raw) == 10:
            return datetime.strptime(raw, "%Y-%m-%d")
        return datetime.strptime(raw, "%Y-%m-%d %H:%M")
    except ValueError:
        return None


async def cmd_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Member-facing plan (prefer saved 4-week plan)
    if not await _require_member_or_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    title = info.get("title") or "(미설정)"
    meeting_at = info.get("meeting_at") or ""
    page_count_raw = info.get("page_count") or ""
    saved_plans = _load_monthly_weekly_plans(settings, month=month)
    if saved_plans:
        lines = [f"읽기 계획표 — {title}", f"- 기준 월: {month}", ""]
        for plan in saved_plans:
            lines.extend(
                [
                    f"{plan.week_number}주차 ({plan.scheduled_date})",
                    f"- 범위: p.{plan.start_page}-{plan.end_page}",
                    plan.summary,
                    plan.encouragement,
                    "",
                ]
            )
        await msg.reply_text("\n".join(lines).strip())
        return

    meeting_dt = _parse_meeting_date_for_plan(meeting_at)
    if meeting_dt is None:
        await msg.reply_text(
            "\n".join(
                [
                    "모임 일정이 아직 없어요.",
                    f"- 운영진이 `{settings.book_catalog_path}` 에서 해당 월(YYYY-MM)의 meeting_at 을 채워줘요.",
                ]
            )
        )
        return
    if not page_count_raw.isdigit():
        await msg.reply_text(
            "\n".join(
                [
                    "총 페이지 수가 아직 없어요.",
                    f"- 운영진이 `{settings.book_catalog_path}` 에서 해당 월(YYYY-MM)의 page_count 를 채워줘요.",
                ]
            )
        )
        return
    total_pages = int(page_count_raw)
    if total_pages <= 0:
        await msg.reply_text(
            "\n".join(
                [
                    "총 페이지 수가 올바르지 않아요.",
                    f"- 운영진이 `{settings.book_catalog_path}` 에서 해당 월(YYYY-MM)의 page_count 를 확인해줘요.",
                ]
            )
        )
        return

    # Defaults (later we can make these configurable)
    buffer_days = 2
    # Plan from today
    today = datetime.now().date()
    meeting_date = meeting_dt.date()
    days_total = (meeting_date - today).days
    if days_total <= 0:
        await msg.reply_text("모임 날짜가 오늘이거나 이미 지났어요. 일정 설정을 확인해줘요.")
        return
    days_readable = max(1, days_total - buffer_days)
    pages_to_read = total_pages
    ppd = (pages_to_read + days_readable - 1) // days_readable  # ceil
    ppd = max(5, ppd)

    # Weekly plan: chunk by 7-day blocks starting today (readable days only)
    weeks = max(1, (days_readable + 6) // 7)
    lines: List[str] = []
    cur = 1
    remaining_days = days_readable
    for w in range(weeks):
        week_days = min(7, remaining_days)
        week_pages = min(total_pages - cur + 1, week_days * ppd)
        start = cur
        end = min(total_pages, start + week_pages - 1)

        start_day_ord = today.toordinal() + (days_readable - remaining_days)
        end_day_ord = start_day_ord + week_days - 1
        start_str = datetime.fromordinal(start_day_ord).strftime("%m/%d")
        end_str = datetime.fromordinal(end_day_ord).strftime("%m/%d")

        lines.append(f"{w+1}주차 ({start_str}–{end_str}): p.{start}–{end} (약 {ppd}p/일)")

        cur = end + 1
        remaining_days -= week_days
        if cur > total_pages:
            break

    if cur <= total_pages:
        lines.append(f"(남은 페이지 p.{cur}–{total_pages}는 주차 내에서 조금 더 읽어야 해요.)")

    header = "\n".join(
        [
            f"읽기 계획표 — {title}",
            f"- 모임까지 D-{days_total} (여유 {buffer_days}일 포함)",
            f"- 총 {total_pages}p / 읽는날 {days_readable}일 / 하루 약 {ppd}p",
            "",
        ]
    )
    plan_body = "\n".join(lines)
    footer = "\n\n" + "여유일은 밀린 분량 정리/복습용으로 비워뒀어요."
    await msg.reply_text(header + plan_body + footer)


async def cmd_taste_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Admin-facing taste snapshot for a given member.
    if not await _require_admin(update, context):
        return

    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    if not context.args:
        await msg.reply_text("사용법: /taste_member @username\n또는: /taste_member <telegram_user_id>")
        return
    target_raw = context.args[0].strip()
    target_user_id: Optional[int] = None
    target_label = target_raw

    if target_raw.isdigit():
        target_user_id = int(target_raw)
    elif target_raw.startswith("@"):
        # Resolve via DB (works if the member has interacted/saved at least once)
        if is_postgres_url(settings.database_url):
            conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
            try:
                target_user_id = find_user_id_by_username_postgres(conn, username=target_raw)
            finally:
                conn.close()
        else:
            conn = connect_sqlite(settings.db_path)
            try:
                target_user_id = find_user_id_by_username_sqlite(conn, username=target_raw)
            finally:
                conn.close()
        if target_user_id is None:
            await msg.reply_text("해당 @username을 찾지 못했어요. (그 멤버가 책갈피/진도체크를 한 번이라도 해야 조회 가능해요.)")
            return
    else:
        await msg.reply_text("사용법: /taste_member @username\n또는: /taste_member <telegram_user_id>")
        return

    limit = max(1, min(100, int(settings.taste_bookmarks_limit)))
    max_clusters = max(1, min(8, int(settings.taste_max_clusters)))

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=target_user_id, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=target_user_id, limit=limit)
        finally:
            conn.close()

    if not items:
        await msg.reply_text("해당 멤버의 책갈피가 아직 없어요.")
        return

    if settings.embeddings_provider != "openai" or not settings.openai_api_key:
        await msg.reply_text("EMBEDDINGS_PROVIDER=openai 와 OPENAI_API_KEY 설정이 필요해요.")
        return

    items = [b for b in items if (b.text or "").strip()]
    if not items:
        await msg.reply_text("해당 멤버의 책갈피 문장이 비어있어요.")
        return

    texts = [b.text for b in items]
    try:
        embeddings = await asyncio.to_thread(
            _get_openai_embeddings,
            settings.openai_api_key,
            settings.openai_embeddings_model,
            texts,
        )
    except AuthenticationError:
        await msg.reply_text("OpenAI API 키가 올바르지 않은 것 같아요. (OPENAI_API_KEY 확인 필요)")
        return
    except RateLimitError:
        await msg.reply_text("요청이 너무 많아서 잠시 제한됐어요. 잠깐 후 다시 시도해줘요.")
        return
    except APIConnectionError:
        await msg.reply_text("네트워크 문제로 취향 분석을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return
    except APIError as e:
        logger.info("OpenAI APIError while fetching embeddings (taste_member)", exc_info=True)
        await msg.reply_text(f"OpenAI 오류로 취향 분석을 불러오지 못했어요. ({e.__class__.__name__})")
        return
    except Exception as e:
        logger.info("Failed to fetch embeddings for admin taste_member", exc_info=True)
        await msg.reply_text(f"지금은 취향 분석을 불러오지 못했어요. ({e.__class__.__name__})")
        return

    try:
        snapshot, _themes = _taste_snapshot_from_bookmarks(
            bookmarks=items, embeddings=embeddings, max_clusters=max_clusters
        )
    except Exception as e:
        logger.info("Failed to build taste_member snapshot", exc_info=True)
        await msg.reply_text(f"취향 스냅샷 생성에 실패했어요. ({e.__class__.__name__}: {str(e)[:120]})")
        return

    await msg.reply_text(f"운영진용 멤버 취향 스냅샷 ({target_label})\n\n" + snapshot)


async def cmd_club_taste(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Admin-facing aggregate taste snapshot for the whole club.
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    if settings.embeddings_provider != "openai" or not settings.openai_api_key:
        await msg.reply_text("EMBEDDINGS_PROVIDER=openai 와 OPENAI_API_KEY 설정이 필요해요.")
        return

    limit = max(50, min(300, int(settings.taste_bookmarks_limit) * 4))
    max_clusters = max(2, min(8, int(settings.taste_max_clusters)))

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_recent_bookmarks_all_postgres(conn, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_recent_bookmarks_all_sqlite(conn, limit=limit)
        finally:
            conn.close()

    if not items:
        await msg.reply_text("아직 북클럽 책갈피가 없어요.")
        return

    texts = [b.text for b in items]
    try:
        embeddings = await asyncio.to_thread(
            _get_openai_embeddings,
            settings.openai_api_key,
            settings.openai_embeddings_model,
            texts,
        )
    except Exception:
        logger.info("Failed to fetch embeddings for club_taste", exc_info=True)
        await msg.reply_text("지금은 북클럽 취향 분석을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    try:
        snapshot, _themes = _taste_snapshot_from_bookmarks(
            bookmarks=items, embeddings=embeddings, max_clusters=max_clusters
        )
    except Exception as e:
        logger.info("Failed to build club_taste snapshot", exc_info=True)
        await msg.reply_text(f"북클럽 취향 스냅샷 생성에 실패했어요. ({e.__class__.__name__}: {str(e)[:120]})")
        return

    await msg.reply_text("북클럽 전체 취향 스냅샷(종합)\n\n" + snapshot)


async def cmd_send_weekly_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(update, context):
        return
    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    plans = _load_monthly_weekly_plans(settings, month=month)
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    cfg = _weekly_check_cfg_from_plans(month, week_number, plans)
    if cfg is None:
        await update.message.reply_text("해당 월의 주차 계획이 없어요. 먼저 /build_month_plan 을 실행해줘요.")
        return
    cfg = WeeklyCheckConfig(**{**cfg.__dict__, "book_title": (info.get("title") or "").strip()})
    await _send_weekly_check_only(context.bot, chat_id=settings.member_chat_id, cfg=cfg)
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            mark_weekly_plan_sent_postgres(conn, month=month, week_number=week_number)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            mark_weekly_plan_sent_sqlite(conn, month=month, week_number=week_number)
        finally:
            conn.close()
    await update.message.reply_text(f"{month} {week_number}주차 진도 체크 메시지를 전송했어요.")


async def cmd_test_weekly_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Admin-only dry run: render the weekly check in the admin chat only.
    This does not send to MEMBER_CHAT_ID and does not mark the plan as sent.
    """
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    chat = update.effective_chat
    if msg is None or chat is None:
        return

    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    plans = _load_monthly_weekly_plans(settings, month=month)
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    cfg = _weekly_check_cfg_from_plans(month, week_number, plans)
    if cfg is None:
        await msg.reply_text("해당 월의 주차 계획이 없어요. 먼저 /sync_catalog_plans 또는 /build_month_plan 을 실행해줘요.")
        return

    cfg = WeeklyCheckConfig(**{**cfg.__dict__, "book_title": (info.get("title") or "").strip()})
    safe_cfg = WeeklyCheckConfig(
        month=cfg.month,
        week_number=cfg.week_number,
        range_label=cfg.range_label,
        book_title=cfg.book_title,
        next_range_label=cfg.next_range_label,
        summary=cfg.summary,
        encouragement=cfg.encouragement,
        discussion_topic="",
        show_quiz_teaser=False,
    )
    text, _markup = build_weekly_check_message(safe_cfg)
    await context.bot.send_message(
        chat_id=chat.id,
        text="\n".join(
            [
                "🧪 운영진 테스트 미리보기",
                "멤버방에는 발송되지 않았고, 발송 완료 처리도 하지 않았어요.",
                "버튼은 테스트 중 실수로 진도 통계가 섞이지 않도록 표시하지 않습니다.",
                "",
                text,
            ]
        ),
    )


async def cmd_send_weekly_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(update, context):
        return
    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    plans = _load_monthly_weekly_plans(settings, month=month)
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    plan = next((p for p in plans if p.week_number == week_number), None)
    cfg = _weekly_check_cfg_from_plans(month, week_number, plans)
    if plan is None or cfg is None:
        await update.message.reply_text("해당 월의 주차 계획이 없어요. 먼저 /build_month_plan 을 실행해줘요.")
        return
    cfg = WeeklyCheckConfig(**{**cfg.__dict__, "book_title": (info.get("title") or "").strip()})
    if not _plan_has_valid_quiz(plan):
        await update.message.reply_text("저장된 미니 퀴즈가 없어요. /build_month_plan force 로 다시 생성해줘요.")
        return
    await _send_weekly_check_and_quiz(context.bot, chat_id=settings.member_chat_id, cfg=cfg, quiz_json=plan.quiz_json)
    await update.message.reply_text(f"{month} {week_number}주차 미니 퀴즈(투표)를 전송했어요.")


async def cmd_send_weekly_topic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(update, context):
        return
    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    plans = _load_monthly_weekly_plans(settings, month=month)
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    plan = next((p for p in plans if p.week_number == week_number), None)
    if plan is None:
        await update.message.reply_text("해당 월의 주차 계획이 없어요. 먼저 /build_month_plan 을 실행해줘요.")
        return
    topic = (plan.discussion_topic or "").strip()
    if not topic:
        await update.message.reply_text("저장된 토론 주제가 없어요. /build_month_plan force 로 다시 생성해줘요.")
        return
    sent = await context.bot.send_message(
        chat_id=settings.member_chat_id, text=f"{month} {week_number}주차 토론 주제\n\n{topic}"
    )
    _remember_last_member_message(settings, message_id=sent.message_id)
    await update.message.reply_text(f"{month} {week_number}주차 토론 주제를 전송했어요.")


async def cmd_delete_last_member_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Admin utility: delete the last message the bot sent to MEMBER_CHAT_ID.
    Requires that the message_id was remembered at send time.
    """
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    mid = _load_last_member_message_id(settings)
    if mid is None:
        await msg.reply_text("삭제할 메시지 기록이 없어요. (last_member_message_id 비어있음)")
        return

    try:
        ok = await context.bot.delete_message(chat_id=settings.member_chat_id, message_id=mid)
        # python-telegram-bot returns True on success (or raises)
        _ = ok
        await msg.reply_text(f"멤버방에서 마지막 메시지를 삭제했어요. (message_id={mid})")
        # Clear stored id so we don't delete something unintended later.
        try:
            _set_global_setting(settings, key=_LAST_MEMBER_MESSAGE_ID_KEY, value="")
        except Exception:
            pass
    except TelegramError as e:
        await msg.reply_text(
            "\n".join(
                [
                    "메시지 삭제에 실패했어요.",
                    f"- message_id: {mid}",
                    f"- 에러: {e.__class__.__name__}",
                    "",
                    "가능한 원인: 봇 권한 부족(can_delete_messages), 너무 오래된 메시지, 이미 삭제됨 등",
                ]
            )
        )


async def cmd_delete_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Admin utility: reply to a message, then run /delete_reply to delete that replied message.
    This works even for messages that were sent before we started tracking message_id.
    """
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    chat = update.effective_chat
    if msg is None or chat is None:
        return
    reply = getattr(msg, "reply_to_message", None)
    if reply is None:
        await msg.reply_text("삭제할 메시지에 답장(Reply)한 뒤 `/delete_reply` 를 실행해줘요.")
        return
    target_mid = getattr(reply, "message_id", None)
    if not isinstance(target_mid, int):
        await msg.reply_text("답장한 메시지의 message_id를 읽지 못했어요.")
        return
    try:
        await context.bot.delete_message(chat_id=chat.id, message_id=target_mid)
        # Best-effort: also delete the command message to reduce clutter.
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=msg.message_id)
        except Exception:
            pass
    except TelegramError as e:
        await msg.reply_text(
            "\n".join(
                [
                    "메시지 삭제에 실패했어요.",
                    f"- chat_id: {chat.id}",
                    f"- message_id: {target_mid}",
                    f"- 에러: {e.__class__.__name__}",
                    "",
                    "가능한 원인: 봇 권한 부족(can_delete_messages), 이미 삭제됨 등",
                ]
            )
        )


def _truncate_plain(s: str, *, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 1)].rstrip() + "…"


def _build_weekly_summary_from_book(entry: dict, *, start_page: int, end_page: int) -> str:
    """
    Lightweight weekly reading note without calling LLM.
    For richer week-by-week summaries (and quiz/topic), use /build_month_plan.
    """
    base = (entry.get("summary") or "").strip()
    if not base:
        base = (entry.get("description") or "").strip()
    return _truncate_plain(base, max_len=240) if base else ""


def _sync_month_plans_from_catalog(settings: Settings, *, force: bool = False) -> tuple[int, list[str], list[str]]:
    """
    Upsert 4-week plans for every month present in the catalog file.
    Uses only catalog data (page_count, meeting_at, summary/description).
    """
    catalog = load_book_catalog(settings.book_catalog_path)
    if not catalog:
        return 0, [f"카탈로그가 비어있어요: {settings.book_catalog_path}"], []

    updated = 0
    warnings: list[str] = []
    details: list[str] = []
    for month, entry in catalog.items():
        if not isinstance(entry, dict):
            continue
        m = _parse_month_yyyy_mm(str(month))
        if not m:
            continue
        page_count_raw = str(entry.get("page_count") or "").strip()
        meeting_at = str(entry.get("meeting_at") or "").strip()
        if not page_count_raw.isdigit():
            warnings.append(f"- {m}: page_count가 없거나 숫자가 아니에요.")
            continue
        meeting_dt = _parse_meeting_date_for_plan(meeting_at)
        if meeting_dt is None:
            warnings.append(f"- {m}: meeting_at이 없거나 형식이 올바르지 않아요. (예: 2026-06-19 22:00)")
            continue

        if not force:
            existing = _load_monthly_weekly_plans(settings, month=m)
            if len(existing) >= 4:
                # Keep any existing (possibly LLM-generated) plans unless forced.
                details.append(f"- {m}: 기존 4주 계획 유지 (safe mode)")
                continue

        total_pages = int(page_count_raw)
        if total_pages <= 0:
            warnings.append(f"- {m}: page_count가 올바르지 않아요.")
            continue

        page_ranges = _build_weekly_page_ranges(total_pages)
        schedule_dates = _build_month_week_schedule(meeting_dt)

        def _encouragement(w: int) -> str:
            if w == 1:
                return "첫 주차예요. 가볍게 워밍업하면서 리듬을 만들어봐요!"
            if w == 2:
                return "중반으로 들어가요. 이번 주도 꾸준히만 가면 충분해요."
            if w == 3:
                return "마지막 스퍼트 전 주차예요. 핵심 아이디어를 정리해보면 좋아요."
            return "마무리 주차예요. 모임 전까지 남은 부분만 차근차근 읽어보면 돼요. 끝까지 화이팅!"

        if is_postgres_url(settings.database_url):
            conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
            try:
                for idx, (start_page, end_page) in enumerate(page_ranges, start=1):
                    upsert_monthly_weekly_plan_postgres(
                        conn,
                        month=m,
                        week_number=idx,
                        start_page=start_page,
                        end_page=end_page,
                        summary=_build_weekly_summary_from_book(entry, start_page=start_page, end_page=end_page),
                        encouragement=_encouragement(idx),
                        scheduled_date=schedule_dates[idx - 1],
                        quiz_json="",
                        discussion_topic="",
                    )
                    updated += 1
                details.append(f"- {m}: 4주 계획 {'덮어쓰기' if force else '생성/보강'}")
            finally:
                conn.close()
        else:
            conn = connect_sqlite(settings.db_path)
            try:
                for idx, (start_page, end_page) in enumerate(page_ranges, start=1):
                    upsert_monthly_weekly_plan_sqlite(
                        conn,
                        month=m,
                        week_number=idx,
                        start_page=start_page,
                        end_page=end_page,
                        summary=_build_weekly_summary_from_book(entry, start_page=start_page, end_page=end_page),
                        encouragement=_encouragement(idx),
                        scheduled_date=schedule_dates[idx - 1],
                        quiz_json="",
                        discussion_topic="",
                    )
                    updated += 1
                details.append(f"- {m}: 4주 계획 {'덮어쓰기' if force else '생성/보강'}")
            finally:
                conn.close()

    return updated, warnings, details


async def cmd_sync_catalog_plans(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    force_words = {"force", "--force", "-f", "강제", "덮어쓰기", "overwrite"}
    force = bool(context.args and any((arg or "").strip().lower() in force_words for arg in context.args))
    active_month = _get_active_month(settings)
    active_info = _load_club_book_info(settings, month=active_month)
    updated, warnings, details = _sync_month_plans_from_catalog(settings, force=force)
    lines = [
        "✅ 카탈로그 기반 4주 계획을 DB에 반영했어요.",
        f"- 카탈로그: `{settings.book_catalog_path}`",
        f"- 현재 대상 책: {active_month} / {active_info.get('title') or '(미설정)'}",
        f"- upsert된 주차 플랜 수: {updated}",
        f"- mode: {'force' if force else 'safe'} (기존 4주 계획이 있으면 {'덮어씀' if force else '유지'})",
        "",
        "진도 체크는 자동 발송되지 않고, /send_weekly_check [주차]로 수동 발송됩니다.",
    ]
    if details:
        lines.extend(["", "처리 내역", *details])
    if not force and updated == 0:
        lines.extend(
            [
                "",
                "기존 요약을 덮어쓰려면 아래처럼 실행해줘요.",
                "/sync_catalog_plans force",
            ]
        )
    if warnings:
        lines.extend(["", "⚠️ 일부 월은 건너뛰었어요.", *warnings])
    await msg.reply_text("\n".join(lines))

async def cmd_preview_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    if msg is None:
        return
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    plans = _load_monthly_weekly_plans(settings, month=month)
    plan = next((p for p in plans if p.week_number == week_number), None)
    if plan is None:
        await msg.reply_text("해당 월의 주차 계획이 없어요. 먼저 /build_month_plan 을 실행해줘요.")
        return
    body = _format_weekly_engagement_preview(plan)
    for chunk in _chunk_text_for_telegram(body):
        await msg.reply_text(chunk)


async def cmd_rebuild_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    if msg is None:
        return
    if not settings.openai_api_key:
        await msg.reply_text("이 기능을 사용하려면 운영진이 OPENAI_API_KEY를 설정해야 해요.")
        return
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    plans = _load_monthly_weekly_plans(settings, month=month)
    plan = next((p for p in plans if p.week_number == week_number), None)
    if plan is None:
        await msg.reply_text("해당 월의 주차 계획이 없어요. 먼저 /build_month_plan 을 실행해줘요.")
        return
    book = _load_club_book_info(settings, month=plan.month)
    title = (book.get("title") or "").strip()
    authors = (book.get("authors") or "").strip() or "미상"
    description = (book.get("description") or "").strip()
    if not title:
        await msg.reply_text("책이 아직 확정되지 않았어요.")
        return
    if not description:
        searched = await _search_book_description_for_summary(
            title=title,
            authors=authors,
            api_key=settings.google_books_api_key,
        )
        description = (searched or "").strip()
    if not description:
        await msg.reply_text("책 소개를 찾지 못했어요. 책 검색·선택으로 소개를 채운 뒤 다시 시도해줘요.")
        return

    await msg.reply_text(f"{plan.month} {week_number}주차만 다시 생성 중…")
    try:
        summary, encouragement, quiz_json, discussion_topic = await asyncio.to_thread(
            _get_openai_weekly_plan_bundle,
            settings.openai_api_key,
            settings.openai_summary_model,
            title=title,
            authors=authors,
            description=description,
            month=plan.month,
            week_number=week_number,
            start_page=plan.start_page,
            end_page=plan.end_page,
        )
    except Exception:
        logger.info("Failed to rebuild single week plan", exc_info=True)
        await msg.reply_text("지금은 해당 주차를 다시 만들지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            upsert_monthly_weekly_plan_postgres(
                conn,
                month=plan.month,
                week_number=week_number,
                start_page=plan.start_page,
                end_page=plan.end_page,
                summary=summary,
                encouragement=encouragement,
                scheduled_date=plan.scheduled_date,
                quiz_json=quiz_json,
                discussion_topic=discussion_topic,
            )
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            upsert_monthly_weekly_plan_sqlite(
                conn,
                month=plan.month,
                week_number=week_number,
                start_page=plan.start_page,
                end_page=plan.end_page,
                summary=summary,
                encouragement=encouragement,
                scheduled_date=plan.scheduled_date,
                quiz_json=quiz_json,
                discussion_topic=discussion_topic,
            )
        finally:
            conn.close()

    refreshed = _load_monthly_weekly_plans(settings, month=plan.month)
    new_plan = next((p for p in refreshed if p.week_number == week_number), None)
    if new_plan is None:
        await msg.reply_text("저장은 됐는데 다시 불러오지 못했어요. /preview_weekly 로 확인해줘요.")
        return
    await msg.reply_text("저장했어요. 아래는 새 버전 미리보기예요.")
    body = _format_weekly_engagement_preview(new_plan)
    for chunk in _chunk_text_for_telegram(body):
        await msg.reply_text(chunk)


async def cmd_weekly_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    month = _get_active_month(settings)
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    plans = _load_monthly_weekly_plans(settings, month=month)
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            stats = list_weekly_progress_stats_postgres(conn, month=month, week_number=week_number)
            members = list_weekly_progress_members_postgres(conn, month=month, week_number=week_number)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            stats = list_weekly_progress_stats_sqlite(conn, month=month, week_number=week_number)
            members = list_weekly_progress_members_sqlite(conn, month=month, week_number=week_number)
        finally:
            conn.close()
    await msg.reply_text(_format_weekly_stats_message(month, week_number, plans, stats, members))


async def cmd_weekly_stats_detail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    month = _get_active_month(settings)
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    plans = _load_monthly_weekly_plans(settings, month=month)
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            stats = list_weekly_progress_stats_postgres(conn, month=month, week_number=week_number)
            members = list_weekly_progress_members_postgres(conn, month=month, week_number=week_number)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            stats = list_weekly_progress_stats_sqlite(conn, month=month, week_number=week_number)
            members = list_weekly_progress_members_sqlite(conn, month=month, week_number=week_number)
        finally:
            conn.close()
    await msg.reply_text(
        _format_weekly_stats_message(month, week_number, plans, stats, members, include_members=True)
    )


async def cmd_share_weekly_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    month = _get_active_month(settings)
    week_number = 1
    if context.args and context.args[0].isdigit():
        week_number = max(1, min(4, int(context.args[0])))
    plans = _load_monthly_weekly_plans(settings, month=month)
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            stats = list_weekly_progress_stats_postgres(conn, month=month, week_number=week_number)
            members = list_weekly_progress_members_postgres(conn, month=month, week_number=week_number)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            stats = list_weekly_progress_stats_sqlite(conn, month=month, week_number=week_number)
            members = list_weekly_progress_members_sqlite(conn, month=month, week_number=week_number)
        finally:
            conn.close()
    text = _format_weekly_stats_message(month, week_number, plans, stats, members)
    await context.bot.send_message(chat_id=settings.member_chat_id, text=text)
    await msg.reply_text("멤버 단체방에 주차 통계를 공유했어요.")


async def cmd_my_progress(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member_or_admin(update, context):
        return
    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    month = _get_active_month(settings)
    if context.args:
        parsed = _parse_month_yyyy_mm(context.args[0])
        if not parsed:
            await msg.reply_text("사용법: /my_progress\n또는: /my_progress 2026-04")
            return
        month = parsed

    plans = _load_monthly_weekly_plans(settings, month=month)
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            status_map = get_user_weekly_status_map_postgres(conn, month=month, telegram_user_id=user.id)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            status_map = get_user_weekly_status_map_sqlite(conn, month=month, telegram_user_id=user.id)
        finally:
            conn.close()

    label_map = {
        "done": "✅ 완료",
        "partial": "🟡 부분",
        "not_yet": "🔴 아직",
    }
    lines = [f"{month} 내 진도 현황", ""]
    for week_number in range(1, 5):
        plan = next((p for p in plans if p.week_number == week_number), None)
        status = label_map.get(status_map.get(week_number, ""), "미응답")
        if plan is not None:
            lines.append(f"{week_number}주차 p.{plan.start_page}-{plan.end_page}: {status}")
        else:
            lines.append(f"{week_number}주차: {status}")
    await msg.reply_text("\n".join(lines))


async def on_mentioned_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # 비용 보호: 멘션 Q&A는 운영진만 응답
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    if msg is None or not getattr(msg, "text", None):
        return
    bot_username = getattr(context.bot, "username", None) or ""
    if not bot_username:
        return

    text = msg.text or ""
    mention_token = f"@{bot_username}"
    mention_token_lower = mention_token.lower()

    entities = getattr(msg, "entities", None) or []
    has_mention = False
    for entity in entities:
        entity_type = getattr(entity, "type", "")
        offset = int(getattr(entity, "offset", 0))
        length = int(getattr(entity, "length", 0))
        piece = text[offset : offset + length]
        if entity_type == "mention" and piece.lower() == mention_token_lower:
            has_mention = True
            break
        if entity_type == "text_mention":
            target_user = getattr(entity, "user", None)
            if target_user is not None and getattr(target_user, "id", None) == getattr(context.bot, "id", None):
                has_mention = True
                break

    if not has_mention and mention_token_lower not in text.lower():
        return

    question = text.replace(mention_token, "").replace(mention_token_lower, "").strip()
    if not question:
        return

    settings: Settings = context.application.bot_data["settings"]
    active_month = _get_active_month(settings)
    target_month = _extract_target_month_from_question(question, active_month)
    info = _load_club_book_info(settings, month=target_month)
    month = info.get("month") or target_month
    plans = _load_monthly_weekly_plans(settings, month=month)

    keyword_reply = _build_mention_keyword_reply(question, info, plans)
    if keyword_reply:
        await msg.reply_text(keyword_reply)
        return

    if not settings.openai_api_key:
        await msg.reply_text("지금은 이 질문에 답할 추가 AI 설정이 없어요. /book, /plan 같은 명령으로 확인해줘요.")
        return

    try:
        answer = await asyncio.to_thread(
            _get_openai_mention_answer,
            settings.openai_api_key,
            settings.openai_summary_model,
            question=question,
            month=month,
            info=info,
            plans=plans,
        )
    except Exception:
        logger.info("Failed to answer mention question", exc_info=True)
        await msg.reply_text("지금은 이 질문에 답하지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    if not answer:
        await msg.reply_text("지금은 이 질문에 답하지 못했어요. /book 또는 /plan으로 확인해줘요.")
        return
    await msg.reply_text(answer)


async def on_progress_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Callbacks should only be accepted from actual book-club members group participants.
    settings: Settings = context.application.bot_data["settings"]
    if not await _is_member_of(settings.member_chat_id, update, context):
        if update.callback_query is not None:
            await update.callback_query.answer("북클럽 멤버만 응답할 수 있어요.", show_alert=True)
        return
    query = update.callback_query
    if query is None:
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 4 or parts[0] != "progress":
        await query.answer("알 수 없는 요청이에요.", show_alert=True)
        return

    _, month, week_raw, status = parts
    try:
        week_number = int(week_raw)
    except ValueError:
        await query.answer("주차 정보가 올바르지 않아요.", show_alert=True)
        return

    user = query.from_user
    full_name = " ".join([p for p in [user.first_name, user.last_name] if p]).strip() or None

    settings: Settings = context.application.bot_data["settings"]
    if is_postgres_url(settings.database_url):
        pg_conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            insert_progress_event_postgres(
                pg_conn,
                telegram_user_id=user.id,
                telegram_username=user.username,
                full_name=full_name,
                week_number=week_number,
                status=status,
            )
            upsert_weekly_progress_status_postgres(
                pg_conn,
                month=month,
                week_number=week_number,
                telegram_user_id=user.id,
                telegram_username=user.username,
                full_name=full_name,
                status=status,
            )
        finally:
            pg_conn.close()
    else:
        sqlite_conn = connect_sqlite(settings.db_path)
        try:
            insert_progress_event(
                sqlite_conn,
                telegram_user_id=user.id,
                telegram_username=user.username,
                full_name=full_name,
                week_number=week_number,
                status=status,
            )
            upsert_weekly_progress_status_sqlite(
                sqlite_conn,
                month=month,
                week_number=week_number,
                telegram_user_id=user.id,
                telegram_username=user.username,
                full_name=full_name,
                status=status,
            )
        finally:
            sqlite_conn.close()

    status_label = {"done": "✅ 완료", "partial": "🟡 부분", "not_yet": "🔴 아직"}.get(status, status)
    await query.answer(f"저장됨: {status_label}")

    # Show lightweight confirmation in chat without spamming group thread too much:
    try:
        await query.edit_message_reply_markup(reply_markup=query.message.reply_markup)
    except Exception:
        pass


def build_application(settings: Settings) -> Application:
    app = Application.builder().token(settings.telegram_bot_token).build()

    app.bot_data["settings"] = settings
    # Allow helper functions that only receive a Bot to access settings.
    setattr(app.bot, "_app_settings", settings)

    # Create tables on startup
    if is_postgres_url(settings.database_url):
        pg_conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            init_db_postgres(pg_conn)
        finally:
            pg_conn.close()
    else:
        sqlite_conn = connect_sqlite(settings.db_path)
        try:
            init_db_sqlite(sqlite_conn)
        finally:
            sqlite_conn.close()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("guide", cmd_guide))
    app.add_handler(CommandHandler("about", cmd_about))
    app.add_handler(CommandHandler("bookmark", cmd_bookmark))
    app.add_handler(CommandHandler("bookmarks", cmd_bookmarks))
    app.add_handler(CommandHandler("bookmark_edit", cmd_bookmark_edit))
    app.add_handler(CommandHandler("bookmark_delete", cmd_bookmark_delete))
    app.add_handler(CommandHandler("taste", cmd_taste_retired))
    app.add_handler(CommandHandler("taste_summary", cmd_taste_summary))
    app.add_handler(CommandHandler("diag_taste", cmd_diag_taste))
    app.add_handler(CommandHandler("set_book", cmd_set_book))
    app.add_handler(CommandHandler("set_meeting", cmd_set_meeting))
    app.add_handler(CommandHandler("set_pages", cmd_set_pages))
    app.add_handler(CommandHandler("show_book", cmd_show_book))
    app.add_handler(CommandHandler("set_puzzle_cover", cmd_set_puzzle_cover))
    app.add_handler(CommandHandler("show_puzzle", cmd_show_puzzle))
    app.add_handler(CommandHandler("taste_member", cmd_taste_member))
    app.add_handler(CommandHandler("club_taste", cmd_club_taste))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("test_weekly_check", cmd_test_weekly_check))
    app.add_handler(CommandHandler("send_weekly_check", cmd_send_weekly_check))
    app.add_handler(CommandHandler("send_weekly_quiz", cmd_send_weekly_quiz))
    app.add_handler(CommandHandler("send_weekly_topic", cmd_send_weekly_topic))
    app.add_handler(CommandHandler("preview_weekly", cmd_preview_weekly))
    app.add_handler(CommandHandler("rebuild_weekly", cmd_rebuild_weekly))
    app.add_handler(CommandHandler("book_search", cmd_book_search))
    app.add_handler(CommandHandler("book_select", cmd_book_select))
    app.add_handler(CommandHandler("build_book_summary", cmd_build_book_summary))
    app.add_handler(CommandHandler("build_month_plan", cmd_build_month_plan))
    app.add_handler(CommandHandler("show_month_plan", cmd_show_month_plan))
    app.add_handler(CommandHandler("send_book_info", cmd_send_book_info))
    app.add_handler(CommandHandler("test_book_videos", cmd_test_book_videos))
    app.add_handler(CommandHandler("send_book_videos", cmd_send_book_videos))
    app.add_handler(CommandHandler("book", cmd_book))
    app.add_handler(CommandHandler("book_month", cmd_book_month))
    app.add_handler(CommandHandler("set_month", cmd_set_month))
    app.add_handler(CommandHandler("sync_catalog_plans", cmd_sync_catalog_plans))
    app.add_handler(CommandHandler("delete_last", cmd_delete_last_member_message))
    app.add_handler(CommandHandler("delete_reply", cmd_delete_reply))
    app.add_handler(CommandHandler("plan", cmd_plan))
    app.add_handler(CommandHandler("my_progress", cmd_my_progress))
    app.add_handler(CommandHandler("weekly_stats", cmd_weekly_stats))
    app.add_handler(CommandHandler("weekly_stats_detail", cmd_weekly_stats_detail))
    app.add_handler(CommandHandler("share_weekly_stats", cmd_share_weekly_stats))
    app.add_handler(CallbackQueryHandler(on_progress_callback, pattern=r"^progress:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_mentioned_text))

    return app

