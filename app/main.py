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


async def _run(app: Application) -> None:
    settings = app.bot_data["settings"]

    await app.initialize()
    await app.start()

    async def health(_: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def telegram_webhook(request: web.Request) -> web.Response:
        if not settings.webhook_url:
            return web.Response(status=400, text="WEBHOOK_URL not configured")

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

    async def ensure_webhook() -> None:
        if not settings.webhook_url:
            return
        url = f"{settings.webhook_url.rstrip('/')}/telegram/webhook"
        for attempt in range(1, 21):
            try:
                await app.bot.set_webhook(
                    url=url,
                    secret_token=settings.webhook_secret_token or None,
                )
                logging.getLogger(__name__).info("Webhook set to %s", url)
                return
            except Exception:
                wait_s = min(30, 2 * attempt)
                logging.getLogger(__name__).warning(
                    "Failed to set webhook (attempt %s). Retrying in %ss",
                    attempt,
                    wait_s,
                    exc_info=True,
                )
                await asyncio.sleep(wait_s)

    polling_queue = None
    if not settings.webhook_url:
        polling_queue = await app.updater.start_polling(
            allowed_updates=["message", "callback_query"],
        )
    else:
        asyncio.create_task(ensure_webhook())

    # Run forever until cancelled / terminated
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        if polling_queue is not None:
            await app.updater.stop()
        await runner.cleanup()
        await app.stop()
        await app.shutdown()


def main() -> None:
    settings = get_settings()
    app = build_application(settings)

    asyncio.run(_run(app))


if __name__ == "__main__":
    main()

