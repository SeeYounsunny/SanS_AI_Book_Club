from __future__ import annotations

import asyncio
import logging

from aiohttp import web
from telegram import Update
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

    await app.start()

    async def health(_: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def telegram_webhook(request: web.Request) -> web.Response:
        # Optional verification: Telegram sends X-Telegram-Bot-Api-Secret-Token if configured.
        expected = settings.webhook_secret_token
        if expected:
            provided = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
            if provided != expected:
                return web.Response(status=401, text="unauthorized")

        data = await request.json()
        update = Update.de_json(data, app.bot)
        await app.process_update(update)
        return web.json_response({"ok": True})

    aiohttp_app = web.Application()
    aiohttp_app.router.add_get("/", health)
    aiohttp_app.router.add_get("/healthz", health)
    aiohttp_app.router.add_post("/telegram/webhook", telegram_webhook)

    runner = web.AppRunner(aiohttp_app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=settings.port)
    await site.start()

    # Run forever until cancelled / terminated
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()
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

