from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from aiohttp import web
from telegram import Update
from telegram.error import RetryAfter
from telegram.ext import Application

from app.config import get_settings
from app.telegram_app import build_application, send_due_weekly_checks


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def _run(app: Application) -> None:
    settings = app.bot_data["settings"]
    logger = logging.getLogger(__name__)

    await app.initialize()
    await app.start()

    def _webhook_target_url() -> str:
        base = (settings.webhook_url or "").strip()
        base = base.rstrip("/")
        # Allow either:
        # - WEBHOOK_URL="https://xxx.up.railway.app"
        # - WEBHOOK_URL="https://xxx.up.railway.app/telegram/webhook"
        if base.endswith("/telegram/webhook"):
            return base
        return f"{base}/telegram/webhook"

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
                logger.warning("Unauthorized webhook request: secret token mismatch")
                return web.Response(status=401, text="unauthorized")

        try:
            data = await request.json()
        except Exception:
            logger.warning("Invalid webhook JSON body", exc_info=True)
            return web.Response(status=400, text="invalid json")
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
            logger.warning("WEBHOOK_URL not configured; bot will not receive updates")
            return
        url = _webhook_target_url()
        # Best-effort: clear any previous webhook once (ignore failures)
        try:
            await app.bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            logger.info("delete_webhook failed (ignored)", exc_info=True)

        for attempt in range(1, 21):
            try:
                await app.bot.set_webhook(
                    url=url,
                    secret_token=settings.webhook_secret_token or None,
                )
                logger.info("Webhook set to %s", url)
                return
            except RetryAfter as e:
                # Telegram flood control: respect retry_after to avoid crash loops.
                wait_s = int(getattr(e, "retry_after", 1)) + 1
                logger.warning("Telegram flood control on setWebhook. Retrying in %ss", wait_s)
                await asyncio.sleep(wait_s)
            except Exception as e:
                wait_s = min(60, 2 * attempt)
                logger.warning(
                    "Failed to set webhook (attempt %s): %s. Retrying in %ss",
                    attempt,
                    str(e),
                    wait_s,
                    exc_info=True,
                )
                await asyncio.sleep(wait_s)

    async def weekly_scheduler() -> None:
        # Check every 60s; send due weekly plans only on Monday 09:00 local server time.
        while True:
            try:
                now = datetime.now()
                if now.weekday() == 0 and now.hour == 9 and now.minute == 0:
                    sent_count = await send_due_weekly_checks(app)
                    if sent_count:
                        logger.info("Sent %s due weekly check(s)", sent_count)
            except Exception:
                logger.warning("weekly scheduler iteration failed", exc_info=True)
            await asyncio.sleep(60)

    # Webhook only: no polling (getUpdates) fallback.
    asyncio.create_task(ensure_webhook())
    asyncio.create_task(weekly_scheduler())

    # Run forever until cancelled / terminated
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()
        await app.stop()
        await app.shutdown()


def main() -> None:
    settings = get_settings()
    app = build_application(settings)

    asyncio.run(_run(app))


if __name__ == "__main__":
    main()

