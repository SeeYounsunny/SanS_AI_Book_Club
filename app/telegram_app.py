from __future__ import annotations

import asyncio
import logging
from collections import Counter
from datetime import datetime
from typing import List, Optional, Tuple

import httpx

from openai import OpenAI
from openai import APIConnectionError, APIError, AuthenticationError, RateLimitError

from telegram import ChatMember, Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

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

logger = logging.getLogger(__name__)


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
        "운영자: /send_weekly_check 로 주간 진도체크를 보낼 수 있어요.",
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

    text = "\n".join(
        [
            "운영진용 명령어",
            "",
            "- /chatid: 현재 채팅의 chat_id 확인 (Railway 변수 MEMBER_CHAT_ID/ADMIN_CHAT_ID 설정용)",
            "- /send_weekly_check: 북클럽 단체방에 주간 진도 체크 메시지 전송",
            "- /set_book: 현재 책 제목 설정 (예: /set_book 아무도 미워하지 않는 자의 죽음)",
            "- /set_meeting: 모임 일정 설정 (예: /set_meeting 2026-04-10 또는 /set_meeting 2026-04-10 20:00)",
            "- /set_month: 설정/조회 기준 월 설정 (예: /set_month 2026-04)",
            "- /book_search: 책 검색 (Google Books) (예: /book_search 아무도 미워하지 않는 자의 죽음)",
            "- /book_select: 검색 결과 중 책 확정 (예: /book_select 1)",
            "- /build_book_summary: 확정된 책 소개를 1~3줄로 요약(선택)",
            "- /build_month_plan: 모임 날짜 기준 4주 계획 생성",
            "- /show_month_plan: 4주 계획 미리보기",
            "- /send_book_info: 확정된 책 요약을 멤버 단체방에 전송",
            "- /set_pages: 총 페이지 수 수동 설정(보정) (예: /set_pages 320)",
            "- /show_book: (기준 월) 책/모임 일정 확인",
            "- /weekly_stats [주차]: 주차별 응답 통계",
            "- /weekly_stats_detail [주차]: 주차별 멤버 상태 상세",
            "- /share_weekly_stats [주차]: 주차별 통계를 단체방에 공유",
            "- /taste_member: 특정 멤버 취향 스냅샷 보기 (예: /taste_member @username 또는 /taste_member 123456789)",
            "- /club_taste: 북클럽 전체 취향 스냅샷(종합)",
            "- /taste_summary: (멤버 1:1) 취향 요약 1~3줄 (LLM)",
            "",
            "빠른 시작",
            "1) 봇을 독서모임 그룹에 초대",
            "2) 그룹에서 /chatid 로 chat_id 복사",
            "3) Railway Variables에 MEMBER_CHAT_ID/ADMIN_CHAT_ID로 저장",
            "4) 운영진 방에서 /send_weekly_check 실행",
        ]
    )

    await msg.reply_text(text)


async def cmd_guide(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Member-facing usage guide. Keep this usable in the member group only.
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
            "이번 책 정보",
            "- /book",
            "- /book_month 2026-04",
            "- /plan",
            "- /my_progress",
            "- 단체방 주간 진도체크 버튼으로 상태를 남겨주세요.",
            "- 이전 주차 메시지가 남아 있으면, 같은 버튼을 다시 눌러 상태를 업데이트할 수 있어요.",
            "- 1:1 대화 바로가기: " + (dm_link or "봇 프로필에서 개인 대화를 열어주세요."),
            "",
            "책갈피(문장 메모) — 1:1 대화에서만",
            "- 저장: /bookmark 인상 깊은 문장",
            "- 보기: /bookmarks",
            "- 수정: /bookmark_edit #id 수정할 문장",
            "- 삭제: /bookmark_delete #id",
            "",
            "취향 스냅샷 — 1:1 대화에서만",
            "- /taste",
            "- /taste_summary",
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
            "- 🧭 취향 요약: 저장한 문장을 바탕으로 내 독서 취향을 조금씩 알아가요. (/taste, 베타)",
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


def _get_active_month(settings: Settings) -> str:
    # Stored in club_settings as "active_month" (YYYY-MM). Fallback to current month.
    active = None
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            active = get_setting_postgres(conn, key="active_month")
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            active = get_setting_sqlite(conn, key="active_month")
        finally:
            conn.close()
    parsed = _parse_month_yyyy_mm(active or "")
    return parsed or _current_month_yyyy_mm()


def _set_active_month(settings: Settings, month: str) -> None:
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_setting_postgres(conn, key="active_month", value=month)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            set_setting_sqlite(conn, key="active_month", value=month)
        finally:
            conn.close()


async def cmd_set_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    if not context.args:
        month = _get_active_month(settings)
        await msg.reply_text(f"현재 기준 월: {month}\n사용법: /set_month 2026-04")
        return
    month = _parse_month_yyyy_mm(context.args[0])
    if not month:
        await msg.reply_text("사용법: /set_month 2026-04")
        return
    _set_active_month(settings, month)
    await msg.reply_text(f"기준 월을 설정했어요: {month}")


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
    lines = ["책 검색 결과 (Google Books)", ""]
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

    def _set_monthly(key: str, value: Optional[str]) -> None:
        if not value:
            return
        if is_postgres_url(settings.database_url):
            conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
            try:
                set_month_setting_postgres(conn, month=month, key=key, value=value)
            finally:
                conn.close()
        else:
            conn = connect_sqlite(settings.db_path)
            try:
                set_month_setting_sqlite(conn, month=month, key=key, value=value)
            finally:
                conn.close()

    # Persist monthly. Also store book_title in legacy global key for compatibility where needed.
    _set_monthly("book_title", title)
    _set_monthly("book_authors", authors)
    _set_monthly("book_isbn", isbn)
    _set_monthly("book_published", published)
    _set_monthly("book_publisher", publisher)
    _set_monthly("book_info_link", info_link)
    if isinstance(pages, int):
        _set_monthly("book_page_count", str(pages))
    if description:
        _set_monthly("book_description", description)

    # keep global book_title for taste_summary prompt fallback
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_setting_postgres(conn, key="book_title", value=title)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            set_setting_sqlite(conn, key="book_title", value=title)
        finally:
            conn.close()

    context.user_data.pop("book_search_results", None)
    await msg.reply_text(
        "\n".join(
            [
                "책을 확정했어요.",
                f"- 월: {month}",
                f"- 제목: {title}",
                f"- 저자: {authors or '(미상)'}",
                f"- 페이지: {str(pages) + 'p' if isinstance(pages, int) else '(미상)'}",
                f"- ISBN: {isbn or '(미상)'}",
            ]
        )
    )


def _load_club_book_info(settings: Settings, *, month: Optional[str] = None) -> dict:
    m = _parse_month_yyyy_mm(month or "") or _get_active_month(settings)

    def _get_monthly(key: str) -> Optional[str]:
        if is_postgres_url(settings.database_url):
            conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
            try:
                v = get_month_setting_postgres(conn, month=m, key=key)
            finally:
                conn.close()
        else:
            conn = connect_sqlite(settings.db_path)
            try:
                v = get_month_setting_sqlite(conn, month=m, key=key)
            finally:
                conn.close()
        if v is not None:
            return v
        # fallback to legacy global storage
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

    return {
        "month": m,
        "title": _get_monthly("book_title"),
        "authors": _get_monthly("book_authors"),
        "isbn": _get_monthly("book_isbn"),
        "page_count": _get_monthly("book_page_count"),
        "published": _get_monthly("book_published"),
        "publisher": _get_monthly("book_publisher"),
        "info_link": _get_monthly("book_info_link"),
        "description": _get_monthly("book_description"),
        "summary": _get_monthly("book_summary"),
        "meeting_at": _get_monthly("meeting_at"),
    }


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
    description = info.get("description") or ""
    summary = info.get("summary") or ""

    meta_parts = [p for p in [publisher, published] if p]
    meta = " / ".join(meta_parts).strip()

    lines = [
        "이달의 책 정보" + (f" ({month})" if month else ""),
        "",
        f"- 제목: {title}",
        f"- 저자: {authors}",
        f"- 총 페이지: {page_count}",
        f"- 모임 일정: {meeting_at}",
    ]
    if meta:
        lines.append(f"- 출판 정보: {meta}")
    if isbn:
        lines.append(f"- ISBN: {isbn}")
    if info_link:
        lines.append(f"- 링크: {info_link}")
    if summary:
        lines.extend(["", "요약", summary])
    elif include_description and description:
        lines.extend(["", "소개", _truncate(description, max_len=700)])
    return "\n".join(lines)


def _get_openai_book_summary(api_key: str, model: str, *, title: str, authors: str, description: str) -> str:
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "너는 온라인 북클럽 운영진의 카피라이터야. 목표는 '멤버가 당장 읽고 싶어지게' 만드는 것이다. "
                    "책 소개문을 바탕으로 멤버 단체방에 보낼 2~3줄 임팩트 메시지를 한국어로 쓴다."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"책 제목: {title}\n"
                    f"저자: {authors}\n\n"
                    f"책 소개(원문):\n{description}\n\n"
                    "요청:\n"
                    "- 한국어로 2~3줄(줄바꿈 포함)\n"
                    "- 질문 금지\n"
                    "- 과장 금지(허위/과대 금지), 단정적 평가 금지\n"
                    "- '어떤 책인지'가 3초 안에 감 오게\n"
                    "- 핵심 매력 포인트 2개 + 마지막에 짧은 독려 1줄\n"
                    "- 불릿/번호/이모지 금지\n"
                ),
            },
        ],
        temperature=0.7,
    )
    return (resp.choices[0].message.content or "").strip()


def _get_openai_weekly_plan_text(
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
) -> Tuple[str, str]:
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "너는 온라인 북클럽 운영진을 돕는 에디터다. 책 소개를 바탕으로 주차별 독서 안내문을 한국어로 만든다. "
                    "스포일러는 과하지 않게, 흐름과 기대 포인트 중심으로 쓴다."
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
                    f"책 소개:\n{description}\n\n"
                    "출력 형식:\n"
                    "SUMMARY:\n"
                    "3~5줄 요약\n\n"
                    "ENCOURAGEMENT:\n"
                    "한 줄 응원 문구\n\n"
                    "제약:\n"
                    "- 불릿/번호 금지\n"
                    "- SUMMARY는 3~5줄\n"
                    "- ENCOURAGEMENT는 1줄\n"
                    "- 읽고 싶어지게 만들되 과장 금지\n"
                ),
            },
        ],
        temperature=0.7,
    )
    text = (resp.choices[0].message.content or "").strip()
    summary = ""
    encouragement = ""
    if "ENCOURAGEMENT:" in text:
        before, after = text.split("ENCOURAGEMENT:", 1)
        summary = before.replace("SUMMARY:", "", 1).strip()
        encouragement = after.strip().splitlines()[0].strip() if after.strip() else ""
    else:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        summary = "\n".join(lines[:4])
        encouragement = lines[4] if len(lines) > 4 else "이번 주는 완독보다 흐름을 따라가는 데 집중해봐요."
    summary_lines = [ln.strip() for ln in summary.splitlines() if ln.strip()][:5]
    return "\n".join(summary_lines), encouragement or "이번 주도 한 걸음씩 같이 읽어봐요."


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
        text, markup = build_weekly_check_message(cfg)
        await app.bot.send_message(chat_id=settings.member_chat_id, text=text, reply_markup=markup)
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
        range_label=f"p.{plan.start_page}-{plan.end_page}",
        next_range_label=(f"p.{next_plan.start_page}-{next_plan.end_page}" if next_plan else ""),
        summary=plan.summary,
        encouragement=encouragement,
    )


async def cmd_build_book_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    if not settings.openai_api_key:
        await msg.reply_text("이 기능을 사용하려면 운영진이 OPENAI_API_KEY를 설정해야 해요.")
        return

    info = _load_club_book_info(settings)
    title = (info.get("title") or "").strip()
    authors = (info.get("authors") or "").strip() or "미상"
    description = (info.get("description") or "").strip()
    if not title or title == "(미설정)":
        await msg.reply_text("먼저 /set_book 또는 /book_select 로 책을 확정해줘요.")
        return
    if not description:
        await msg.reply_text("책 소개(description)가 없어서 요약을 만들 수 없어요. (다른 검색 결과를 선택해보세요.)")
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
    # Soft-enforce 2~3 lines
    lines = [ln.strip() for ln in summary.splitlines() if ln.strip()]
    if len(lines) < 2:
        # If model returned 1 line, keep it but add a gentle second line.
        lines = [lines[0] if lines else summary.strip(), "이번 주, 첫 페이지부터 같이 시작해요."]
    lines = lines[:3]
    summary = "\n".join(lines)

    month = info.get("month") or _get_active_month(settings)
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_month_setting_postgres(conn, month=month, key="book_summary", value=summary)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            set_month_setting_sqlite(conn, month=month, key="book_summary", value=summary)
        finally:
            conn.close()

    await msg.reply_text("책 요약을 저장했어요.\n\n" + summary)


async def cmd_build_month_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    if not settings.openai_api_key:
        await msg.reply_text("이 기능을 사용하려면 운영진이 OPENAI_API_KEY를 설정해야 해요.")
        return

    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    title = (info.get("title") or "").strip()
    authors = (info.get("authors") or "").strip() or "미상"
    description = (info.get("description") or "").strip()
    meeting_at = (info.get("meeting_at") or "").strip()
    page_count_raw = (info.get("page_count") or "").strip()
    meeting_dt = _parse_meeting_date_for_plan(meeting_at)
    if not title:
        await msg.reply_text("먼저 책을 확정해줘요. (/book_select 또는 /set_book)")
        return
    if meeting_dt is None:
        await msg.reply_text("먼저 모임 날짜를 설정해줘요. (/set_meeting)")
        return
    if not page_count_raw.isdigit():
        await msg.reply_text("먼저 총 페이지 수를 설정해줘요. (/set_pages)")
        return
    if not description:
        await msg.reply_text("책 소개가 아직 없어요. 책 검색 후 선택한 책으로 진행해줘요.")
        return

    page_ranges = _build_weekly_page_ranges(int(page_count_raw))
    schedule_dates = _build_month_week_schedule(meeting_dt)

    try:
        generated: List[Tuple[str, str]] = []
        for idx, (start_page, end_page) in enumerate(page_ranges, start=1):
            generated.append(
                await asyncio.to_thread(
                    _get_openai_weekly_plan_text,
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
                summary, encouragement = generated[idx - 1]
                upsert_monthly_weekly_plan_postgres(
                    conn,
                    month=month,
                    week_number=idx,
                    start_page=start_page,
                    end_page=end_page,
                    summary=summary,
                    encouragement=encouragement,
                    scheduled_date=schedule_dates[idx - 1],
                )
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            for idx, (start_page, end_page) in enumerate(page_ranges, start=1):
                summary, encouragement = generated[idx - 1]
                upsert_monthly_weekly_plan_sqlite(
                    conn,
                    month=month,
                    week_number=idx,
                    start_page=start_page,
                    end_page=end_page,
                    summary=summary,
                    encouragement=encouragement,
                    scheduled_date=schedule_dates[idx - 1],
                )
        finally:
            conn.close()

    preview_lines = [f"{month} 4주 계획을 저장했어요."]
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
    info = _load_club_book_info(settings)
    month = info.get("month") or _get_active_month(settings)
    plans = _load_monthly_weekly_plans(settings, month=month)
    if not plans:
        await msg.reply_text("아직 4주 계획이 없어요. 운영진이 /build_month_plan 을 먼저 실행해줘요.")
        return
    lines = [f"{month} 4주 읽기 계획", ""]
    for plan in plans:
        lines.extend(
            [
                f"{plan.week_number}주차 ({plan.scheduled_date})",
                f"- 범위: p.{plan.start_page}-{plan.end_page}",
                f"- 안내: {plan.summary.splitlines()[0] if plan.summary else ''}",
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


async def cmd_send_book_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    info = _load_club_book_info(settings)
    text = _format_book_info_message(info)
    await context.bot.send_message(chat_id=settings.member_chat_id, text=text)
    await msg.reply_text("멤버 단체방에 책 정보를 전송했어요.")


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


def _get_openai_embeddings(api_key: str, model: str, texts: List[str]) -> List[List[float]]:
    client = OpenAI(api_key=api_key)
    resp = client.embeddings.create(model=model, input=texts)
    # Ensure stable order by index
    data = sorted(resp.data, key=lambda d: d.index)
    vectors = [d.embedding for d in data]
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


async def cmd_taste(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Member-facing "taste snapshot" based on their bookmarks.
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    limit = max(1, min(100, int(settings.taste_bookmarks_limit)))
    max_clusters = max(1, min(8, int(settings.taste_max_clusters)))

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=user.id, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=user.id, limit=limit)
        finally:
            conn.close()

    if not items:
        await msg.reply_text("아직 저장된 책갈피가 없어요. 먼저 /bookmark 로 문장을 저장해보세요.")
        return

    if settings.embeddings_provider != "openai" or not settings.openai_api_key:
        await msg.reply_text(
            "취향 스냅샷(임베딩 기반)을 사용하려면 운영진이 EMBEDDINGS_PROVIDER=openai 와 OPENAI_API_KEY를 설정해야 해요."
        )
        return

    # Fetch embeddings in a thread to avoid blocking the event loop.
    # Ensure we don't send empty strings
    items = [b for b in items if (b.text or "").strip()]
    if not items:
        await msg.reply_text("분석할 수 있는 책갈피 문장이 없어요. /bookmark 로 문장을 저장해보세요.")
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
        logger.info("OpenAI APIError while fetching embeddings", exc_info=True)
        await msg.reply_text(f"OpenAI 오류로 취향 분석을 불러오지 못했어요. ({e.__class__.__name__})")
        return
    except Exception as e:
        logger.info("Failed to fetch embeddings", exc_info=True)
        await msg.reply_text(f"지금은 취향 분석을 불러오지 못했어요. ({e.__class__.__name__})")
        return

    try:
        snapshot, _themes = _taste_snapshot_from_bookmarks(
            bookmarks=items, embeddings=embeddings, max_clusters=max_clusters
        )
    except Exception as e:
        logger.info("Failed to build taste snapshot", exc_info=True)
        await msg.reply_text(f"취향 스냅샷 생성에 실패했어요. ({e.__class__.__name__}: {str(e)[:120]})")
        return

    await msg.reply_text(snapshot)


async def cmd_taste_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Member-facing 1~3 line summary (LLM) based on representative bookmarks.
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    if settings.embeddings_provider != "openai" or not settings.openai_api_key:
        await msg.reply_text("이 기능을 사용하려면 운영진이 OPENAI_API_KEY를 설정해야 해요.")
        return

    limit = max(3, min(100, int(settings.taste_bookmarks_limit)))
    max_clusters = max(1, min(8, int(settings.taste_max_clusters)))
    max_quotes = max(3, min(12, int(settings.taste_summary_max_quotes)))

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=user.id, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=user.id, limit=limit)
        finally:
            conn.close()

    items = [b for b in items if (b.text or "").strip()]
    if len(items) < 2:
        await msg.reply_text("요약을 만들기엔 책갈피가 아직 너무 적어요. 문장을 2개 이상 저장해줘요.")
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
        logger.info("Failed to fetch embeddings for taste_summary", exc_info=True)
        await msg.reply_text("지금은 요약을 만들기 위한 분석을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    reps = _select_representative_bookmarks(items, embeddings, max_clusters=max_clusters, max_quotes=max_quotes)
    prompt = _build_taste_summary_prompt(reps)

    # Include current book title (if configured) to ground the summary.
    book_title = None
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            book_title = get_setting_postgres(conn, key="book_title")
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            book_title = get_setting_sqlite(conn, key="book_title")
        finally:
            conn.close()
    if book_title:
        prompt = f"[현재 읽는 책: {book_title}]\n" + prompt

    try:
        summary = await asyncio.to_thread(
            _get_openai_taste_summary,
            settings.openai_api_key,
            settings.openai_summary_model,
            prompt,
        )
    except AuthenticationError:
        await msg.reply_text("OpenAI API 키가 올바르지 않은 것 같아요. (OPENAI_API_KEY 확인 필요)")
        return
    except RateLimitError:
        await msg.reply_text("요청이 너무 많아서 잠시 제한됐어요. 잠깐 후 다시 시도해줘요.")
        return
    except APIConnectionError:
        await msg.reply_text("네트워크 문제로 요약을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return
    except APIError as e:
        logger.info("OpenAI APIError while generating taste summary", exc_info=True)
        await msg.reply_text(f"OpenAI 오류로 요약을 만들지 못했어요. ({e.__class__.__name__})")
        return
    except Exception as e:
        logger.info("Failed to generate taste summary", exc_info=True)
        await msg.reply_text(f"지금은 요약을 만들지 못했어요. ({e.__class__.__name__})")
        return

    if not summary:
        await msg.reply_text("요약 결과가 비어있어요. 잠시 후 다시 시도해줘요.")
        return

    # Ensure 1~3 lines (soft cap)
    lines = [ln.strip() for ln in summary.splitlines() if ln.strip()]
    lines = lines[:3]
    encouragement = "오늘은 10분만 읽고 책갈피 하나 남겨봐요."
    if not lines:
        lines = [encouragement]
    elif len(lines) < 3:
        lines.append(encouragement)
    else:
        lines[-1] = f"{lines[-1]} {encouragement}"
    await msg.reply_text("\n".join(lines))


async def cmd_set_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    title = " ".join(context.args).strip() if context.args else ""
    if not title:
        await msg.reply_text("사용법: /set_book <책 제목>")
        return
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_setting_postgres(conn, key="book_title", value=title)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            set_setting_sqlite(conn, key="book_title", value=title)
        finally:
            conn.close()
    await msg.reply_text(f"현재 책을 설정했어요: {title}")

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

    meeting_at = _parse_meeting_args(context.args or [])
    if not meeting_at:
        await msg.reply_text("사용법: /set_meeting 2026-04-10\n또는: /set_meeting 2026-04-10 20:00")
        return

    month = _get_active_month(settings)
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_month_setting_postgres(conn, month=month, key="meeting_at", value=meeting_at)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            set_month_setting_sqlite(conn, month=month, key="meeting_at", value=meeting_at)
        finally:
            conn.close()

    await msg.reply_text(f"모임 일정을 설정했어요: {meeting_at} (월: {month})")


async def cmd_show_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    info = _load_club_book_info(settings)
    month = info.get("month") or ""
    title = info.get("title")
    meeting_at = info.get("meeting_at")
    await msg.reply_text(
        "\n".join(
            [
                f"기준 월: {month}",
                f"이달의 책: {title or '(미설정)'}",
                f"모임 일정: {meeting_at or '(미설정)'}",
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
    if not context.args or not context.args[0].isdigit():
        await msg.reply_text("사용법: /set_pages <총페이지>\n예) /set_pages 320")
        return
    pages = int(context.args[0])
    if pages <= 0 or pages > 5000:
        await msg.reply_text("총 페이지 수가 올바르지 않아요. (1~5000)")
        return

    month = _get_active_month(settings)
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_month_setting_postgres(conn, month=month, key="book_page_count", value=str(pages))
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            set_month_setting_sqlite(conn, month=month, key="book_page_count", value=str(pages))
        finally:
            conn.close()
    await msg.reply_text(f"총 페이지를 설정했어요: {pages}p (월: {month})")


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
        await msg.reply_text("모임 일정이 아직 없어요. 운영진이 /set_meeting 으로 먼저 설정해줘요.")
        return
    if not page_count_raw.isdigit():
        await msg.reply_text("총 페이지 수가 아직 없어요. 운영진이 /set_pages 로 먼저 설정해줘요.")
        return
    total_pages = int(page_count_raw)
    if total_pages <= 0:
        await msg.reply_text("총 페이지 수가 올바르지 않아요. 운영진이 /set_pages 를 다시 설정해줘요.")
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
    text, markup = build_weekly_check_message(cfg)

    await context.bot.send_message(
        chat_id=settings.member_chat_id,
        text=text,
        reply_markup=markup,
    )
    await update.message.reply_text(f"{month} {week_number}주차 진도 체크 메시지를 전송했어요.")


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
    app.add_handler(CommandHandler("taste", cmd_taste))
    app.add_handler(CommandHandler("taste_summary", cmd_taste_summary))
    app.add_handler(CommandHandler("set_book", cmd_set_book))
    app.add_handler(CommandHandler("set_meeting", cmd_set_meeting))
    app.add_handler(CommandHandler("set_pages", cmd_set_pages))
    app.add_handler(CommandHandler("show_book", cmd_show_book))
    app.add_handler(CommandHandler("taste_member", cmd_taste_member))
    app.add_handler(CommandHandler("club_taste", cmd_club_taste))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("send_weekly_check", cmd_send_weekly_check))
    app.add_handler(CommandHandler("book_search", cmd_book_search))
    app.add_handler(CommandHandler("book_select", cmd_book_select))
    app.add_handler(CommandHandler("build_book_summary", cmd_build_book_summary))
    app.add_handler(CommandHandler("build_month_plan", cmd_build_month_plan))
    app.add_handler(CommandHandler("show_month_plan", cmd_show_month_plan))
    app.add_handler(CommandHandler("send_book_info", cmd_send_book_info))
    app.add_handler(CommandHandler("book", cmd_book))
    app.add_handler(CommandHandler("book_month", cmd_book_month))
    app.add_handler(CommandHandler("set_month", cmd_set_month))
    app.add_handler(CommandHandler("plan", cmd_plan))
    app.add_handler(CommandHandler("my_progress", cmd_my_progress))
    app.add_handler(CommandHandler("weekly_stats", cmd_weekly_stats))
    app.add_handler(CommandHandler("weekly_stats_detail", cmd_weekly_stats_detail))
    app.add_handler(CommandHandler("share_weekly_stats", cmd_share_weekly_stats))
    app.add_handler(CallbackQueryHandler(on_progress_callback, pattern=r"^progress:"))

    return app

