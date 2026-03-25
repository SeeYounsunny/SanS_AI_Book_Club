from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


@dataclass(frozen=True)
class WeeklyCheckConfig:
    week_number: int
    range_label: str


def build_weekly_check_message(cfg: WeeklyCheckConfig) -> Tuple[str, InlineKeyboardMarkup]:
    text = (
        f"이번주 읽기 범위\n\n"
        f"{cfg.range_label}\n\n"
        f"읽기 상태를 선택해주세요 (Week {cfg.week_number})"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ 완료", callback_data=f"progress:{cfg.week_number}:done"),
                InlineKeyboardButton("🟡 부분", callback_data=f"progress:{cfg.week_number}:partial"),
                InlineKeyboardButton("🔴 아직", callback_data=f"progress:{cfg.week_number}:not_yet"),
            ]
        ]
    )
    return text, keyboard

