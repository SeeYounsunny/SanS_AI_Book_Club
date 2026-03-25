from __future__ import annotations

import logging

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
    connect_postgres,
    connect_sqlite,
    init_db_postgres,
    init_db_sqlite,
    insert_progress_event,
    insert_progress_event_postgres,
    is_postgres_url,
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
            "3) 주의사항",
            "- 진도 체크 버튼은 '북클럽 단체방' 멤버만 사용할 수 있어요.",
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
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("send_weekly_check", cmd_send_weekly_check))
    app.add_handler(CallbackQueryHandler(on_progress_callback, pattern=r"^progress:"))

    return app

