from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


@dataclass(frozen=True)
class WeeklyCheckConfig:
    month: str
    week_number: int
    book_title: str = ""
    range_label: str
    next_range_label: str = ""
    summary: str = ""
    encouragement: str = ""
    discussion_topic: str = ""
    show_quiz_teaser: bool = False


def build_weekly_check_message(cfg: WeeklyCheckConfig) -> Tuple[str, InlineKeyboardMarkup]:
    parts = [
        f"{cfg.month} {cfg.week_number}주차 진도 체크",
        "",
        (f"📚 책: {cfg.book_title}" if (cfg.book_title or "").strip() else "").strip(),
        f"📖 지난주 체크 범위: {cfg.range_label}",
    ]
    parts = [p for p in parts if p]
    if cfg.next_range_label:
        parts.extend(["", f"🗓 이번주 예고 범위: {cfg.next_range_label}"])
    if cfg.summary:
        parts.extend(["", "[이번주 흐름 - 요약]", cfg.summary])
    if cfg.encouragement:
        parts.extend(["", cfg.encouragement])
    if cfg.show_quiz_teaser:
        parts.extend(["", "📝 이번 주 미니 퀴즈는 이 메시지 바로 아래 투표(퀴즈)로 올라와요."])
    disc = (cfg.discussion_topic or "").strip()
    if disc:
        parts.extend(["", "💬 모임에서 나눠 볼 주제", disc])
    parts.extend(["", "지난주 범위를 얼마나 읽었는지 선택해주세요."])
    text = "\n".join(parts)

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ 완료", callback_data=f"progress:{cfg.month}:{cfg.week_number}:done"),
                InlineKeyboardButton("🟡 부분", callback_data=f"progress:{cfg.month}:{cfg.week_number}:partial"),
                InlineKeyboardButton("🔴 아직", callback_data=f"progress:{cfg.month}:{cfg.week_number}:not_yet"),
            ]
        ]
    )
    return text, keyboard

