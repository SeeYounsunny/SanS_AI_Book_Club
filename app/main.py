from __future__ import annotations

import asyncio
import logging

from telegram.ext import Application

from app.config import get_settings
from app.telegram_app import build_application


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def _run_webhook(app: Application) -> None:
    settings = app.bot_data["settings"]

    await app.initialize()
    await app.bot.set_webhook(
        url=f"{settings.webhook_url.rstrip('/')}/telegram/webhook",
        secret_token=settings.webhook_secret_token or None,
    )

    # PTB's webhook server path is configured via webhook_url + url_path.
    # We'll use url_path="telegram/webhook" and bind to 0.0.0.0:<PORT>
    await app.start()
    await app.updater.start_webhook(
        listen="0.0.0.0",
        port=settings.port,
        url_path="telegram/webhook",
        secret_token=settings.webhook_secret_token or None,
    )

    await app.updater.wait_until_closed()
    await app.stop()
    await app.shutdown()


def _run_polling(app: Application) -> None:
    # Fallback for local testing only.
    app.run_polling(allowed_updates=["message", "callback_query"])


def main() -> None:
    settings = get_settings()
    app = build_application(settings)

    if settings.webhook_url:
        asyncio.run(_run_webhook(app))
    else:
        _run_polling(app)


if __name__ == "__main__":
    main()

