from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


@dataclass(frozen=True)
class WeeklyCheckConfig:
    month: str
    week_number: int
    range_label: str
    next_range_label: str = ""
    summary: str = ""
    encouragement: str = ""


def build_weekly_check_message(cfg: WeeklyCheckConfig) -> Tuple[str, InlineKeyboardMarkup]:
    parts = [
        f"{cfg.month} {cfg.week_number}주차 진도 체크",
        "",
        f"📖 지난주 체크 범위: {cfg.range_label}",
    ]
    if cfg.next_range_label:
        parts.extend(["", f"🗓 이번주 예고 범위: {cfg.next_range_label}"])
    if cfg.summary:
        parts.extend(["", "### 이번주 흐름 - 요약", cfg.summary])
    if cfg.encouragement:
        parts.extend(["", cfg.encouragement])
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

