from __future__ import annotations

import logging
from typing import Optional

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
    connect_postgres,
    connect_sqlite,
    delete_bookmark_postgres,
    delete_bookmark_sqlite,
    enforce_bookmarks_limit_postgres,
    enforce_bookmarks_limit_sqlite,
    init_db_postgres,
    init_db_sqlite,
    insert_bookmark_postgres,
    insert_bookmark_sqlite,
    insert_progress_event,
    insert_progress_event_postgres,
    is_postgres_url,
    list_bookmarks_postgres,
    list_bookmarks_sqlite,
    update_bookmark_postgres,
    update_bookmark_sqlite,
)
from app.reading_check import WeeklyCheckConfig, build_weekly_check_message

logger = logging.getLogger(__name__)


def _default_weekly_check_cfg() -> WeeklyCheckConfig:
    # Phase 1: hardcoded; later connect to Notion/DB "Books" + weekly plan.
    return WeeklyCheckConfig(week_number=1, range_label="Chapter 1 ~ 3")

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
            "사용 가능한 명령어",
            "",
            "- /start: 봇 소개",
            "- /guide: 사용법 안내",
            "- /chatid: 현재 채팅의 chat_id 확인 (Railway 변수 MEMBER_CHAT_ID/ADMIN_CHAT_ID 설정용)",
            "- /send_weekly_check: (운영진) 북클럽 단체방에 주간 진도 체크 메시지 전송",
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
    if msg is None:
        return

    text = "\n".join(
        [
            "북클럽 봇 사용 방법",
            "",
            "1) 진도 체크는 어떻게 하나요?",
            "- 북클럽 단체방에 '이번주 읽기 범위' 메시지가 올라오면",
            "- 아래 버튼 중 하나를 눌러주세요:",
            "  - ✅ 완료 / 🟡 부분 / 🔴 아직",
            "",
            "2) 내가 누른 기록은 저장되나요?",
            "- 네. 버튼을 누르면 이번 주차 상태가 저장돼요.",
            "",
            "3) 문장 메모(책갈피)는 어떻게 하나요?",
            "- 저장 예시1: /bookmark 페이지번호 | 여기 문장을 그대로 붙여넣기",
            "  예) /bookmark 57 | 여기 문장을 그대로 붙여넣기",
            "- 저장 예시2: /bookmark 여기 문장을 그대로 붙여넣기",
            "- 확인 예시: /bookmarks  (기본: 최근 10개)",
            "- 더 많이 보기 예시: /bookmarks 숫자  (숫자만큼 최근 저장 내용 표시)",
            "  예) /bookmarks 20",
            "- 검색 예시: /bookmarks 용기",
            "- 수정 예시: /bookmark_edit 12 페이지번호 | 수정한 문장",
            "- 삭제 예시: /bookmark_delete 12",
            "  (여기서 12는 /bookmarks 목록에 보이는 #id예요)",
            "",
            "4) 주의사항",
            "- 진도 체크 버튼은 '북클럽 단체방' 멤버만 사용할 수 있어요.",
            "- 책갈피 기능(/bookmark, /bookmarks, 수정/삭제)은 봇과의 1:1 대화에서만 사용할 수 있어요.",
            "- 책갈피는 1인당 최대 100개까지 저장돼요. 초과하면 오래된 것부터 자동 삭제돼요.",
            "",
            "추가 기능(퀴즈/토론 질문/리포트 등)이 생기면 이 안내를 업데이트할게요.",
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
            "AI Book Club Bot 소개",
            "",
            "이 봇은 온라인 독서모임 운영을 도와주는 텔레그램 봇이에요.",
            "현재는 매주 올라오는 '진도 체크'에 버튼으로 응답하고, 그 기록을 저장하는 기능을 제공해요.",
            "",
            "사용법은 /guide 를 참고해주세요.",
        ]
    )
    await msg.reply_text(text)


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
        await msg.reply_text("사용법: /bookmark_edit <id> 120 | 새 문장\n또는: /bookmark_edit <id> 새 문장")
        return

    if not context.args[0].isdigit():
        await msg.reply_text("사용법: /bookmark_edit <id> 120 | 새 문장")
        return

    bookmark_id = int(context.args[0])
    rest = " ".join(context.args[1:]) if len(context.args) > 1 else ""
    page, text = _parse_bookmark_args(rest)
    if not text:
        await msg.reply_text("수정할 문장을 입력해주세요. 예) /bookmark_edit 12 120 | 새 문장")
        return

    ok = False
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            ok = update_bookmark_postgres(conn, bookmark_id=bookmark_id, telegram_user_id=user.id, page=page, text=text)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            ok = update_bookmark_sqlite(conn, bookmark_id=bookmark_id, telegram_user_id=user.id, page=page, text=text)
        finally:
            conn.close()

    if not ok:
        await msg.reply_text("해당 id의 책갈피를 찾지 못했어요. /bookmarks 로 id를 확인해주세요.")
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

    if not context.args or not context.args[0].isdigit():
        await msg.reply_text("사용법: /bookmark_delete <id>")
        return

    bookmark_id = int(context.args[0])
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
        await msg.reply_text("해당 id의 책갈피를 찾지 못했어요. /bookmarks 로 id를 확인해주세요.")
        return

    await msg.reply_text(f"삭제했어요. #{bookmark_id}")


async def cmd_send_weekly_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(update, context):
        return

    cfg = _default_weekly_check_cfg()
    text, markup = build_weekly_check_message(cfg)

    await context.bot.send_message(
        chat_id=settings.member_chat_id,
        text=text,
        reply_markup=markup,
    )
    await update.message.reply_text("주간 진도 체크 메시지를 전송했어요.")


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
    if len(parts) != 3 or parts[0] != "progress":
        await query.answer("알 수 없는 요청이에요.", show_alert=True)
        return

    _, week_raw, status = parts
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
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("send_weekly_check", cmd_send_weekly_check))
    app.add_handler(CallbackQueryHandler(on_progress_callback, pattern=r"^progress:"))

    return app

