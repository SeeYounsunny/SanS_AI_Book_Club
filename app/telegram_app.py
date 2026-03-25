from __future__ import annotations

import logging

from telegram import Update
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


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "AI Reading Club Agent입니다.\n\n"
        "운영자: /send_weekly_check 로 주간 진도체크를 보낼 수 있어요.",
    )


async def cmd_send_weekly_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    cfg = _default_weekly_check_cfg()
    text, markup = build_weekly_check_message(cfg)

    await context.bot.send_message(
        chat_id=settings.telegram_chat_id,
        text=text,
        reply_markup=markup,
    )
    await update.message.reply_text("주간 진도 체크 메시지를 전송했어요.")


async def on_progress_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
    app.add_handler(CommandHandler("send_weekly_check", cmd_send_weekly_check))
    app.add_handler(CallbackQueryHandler(on_progress_callback, pattern=r"^progress:"))

    return app

