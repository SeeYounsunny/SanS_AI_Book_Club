from __future__ import annotations

from io import BytesIO
import random
from typing import List

from PIL import Image, ImageDraw


def calculate_progress_percent(*, pages_read: int, total_pages: int) -> int:
    if total_pages <= 0:
        return 0
    pages = max(0, min(pages_read, total_pages))
    return max(0, min(100, int((pages / float(total_pages)) * 100)))


def calculate_revealed_tiles(*, progress_percent: int, total_tiles: int = 100) -> int:
    pct = max(0, min(progress_percent, 100))
    return max(0, min(total_tiles, int((pct / 100.0) * total_tiles)))


def build_reveal_order(*, total_tiles: int, seed: int) -> List[int]:
    order = list(range(total_tiles))
    rng = random.Random(seed)
    rng.shuffle(order)
    return order


def render_text_grid(*, revealed_tiles: int, total_tiles: int = 100, cols: int = 10) -> str:
    revealed = max(0, min(revealed_tiles, total_tiles))
    rows = []
    for start in range(0, total_tiles, cols):
        chars = []
        for idx in range(start, min(start + cols, total_tiles)):
            chars.append("■" if idx < revealed else "□")
        rows.append("".join(chars))
    return "\n".join(rows)


def render_image_puzzle(
    *,
    image_bytes: bytes,
    revealed_tiles: int,
    total_tiles: int = 100,
    seed: int,
    mask_color: tuple = (36, 36, 36, 230),
) -> bytes:
    image = Image.open(BytesIO(image_bytes)).convert("RGBA")
    size = min(image.width, image.height)
    left = (image.width - size) // 2
    top = (image.height - size) // 2
    image = image.crop((left, top, left + size, top + size)).resize((1000, 1000))

    overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    grid = int(total_tiles ** 0.5)
    tile_w = image.width // grid
    tile_h = image.height // grid
    order = build_reveal_order(total_tiles=total_tiles, seed=seed)
    open_tiles = set(order[: max(0, min(revealed_tiles, total_tiles))])

    for idx in range(total_tiles):
        row = idx // grid
        col = idx % grid
        x0 = col * tile_w
        y0 = row * tile_h
        x1 = image.width if col == grid - 1 else (col + 1) * tile_w
        y1 = image.height if row == grid - 1 else (row + 1) * tile_h
        if idx not in open_tiles:
            draw.rectangle((x0, y0, x1, y1), fill=mask_color)
        draw.rectangle((x0, y0, x1, y1), outline=(255, 255, 255, 60), width=1)

    result = Image.alpha_composite(image, overlay).convert("RGB")
    out = BytesIO()
    result.save(out, format="JPEG", quality=92)
    return out.getvalue()
