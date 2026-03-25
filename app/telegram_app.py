from __future__ import annotations

import asyncio
import logging
from collections import Counter
from typing import List, Optional, Tuple

from openai import OpenAI
from openai import APIConnectionError, APIError, AuthenticationError, RateLimitError

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
    Bookmark,
    connect_postgres,
    connect_sqlite,
    delete_bookmark_postgres,
    delete_bookmark_sqlite,
    enforce_bookmarks_limit_postgres,
    enforce_bookmarks_limit_sqlite,
    find_user_id_by_username_postgres,
    find_user_id_by_username_sqlite,
    init_db_postgres,
    init_db_sqlite,
    insert_bookmark_postgres,
    insert_bookmark_sqlite,
    insert_progress_event,
    insert_progress_event_postgres,
    is_postgres_url,
    get_setting_postgres,
    get_setting_sqlite,
    list_bookmarks_postgres,
    list_bookmarks_sqlite,
    list_recent_bookmarks_all_postgres,
    list_recent_bookmarks_all_sqlite,
    update_bookmark_postgres,
    update_bookmark_sqlite,
    set_setting_postgres,
    set_setting_sqlite,
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


async def _require_private_chat(update: Update) -> bool:
    chat = update.effective_chat
    msg = update.effective_message
    if chat is None:
        return False
    if getattr(chat, "type", None) != "private":
        if msg is not None:
            await msg.reply_text("이 기능은 봇과의 1:1 대화에서만 사용할 수 있어요.")
        return False
    return True


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
            "운영진용 명령어",
            "",
            "- /chatid: 현재 채팅의 chat_id 확인 (Railway 변수 MEMBER_CHAT_ID/ADMIN_CHAT_ID 설정용)",
            "- /send_weekly_check: 북클럽 단체방에 주간 진도 체크 메시지 전송",
            "- /set_book: 현재 책 제목 설정 (예: /set_book 아무도 미워하지 않는 자의 죽음)",
            "- /show_book: 현재 책/일정 설정 확인",
            "- /taste_member: 특정 멤버 취향 스냅샷 보기 (예: /taste_member @username 또는 /taste_member 123456789)",
            "- /club_taste: 북클럽 전체 취향 스냅샷(종합)",
            "- /taste_summary: (멤버 1:1) 취향 요약 1~3줄 (LLM)",
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
            "봇 소개",
            "- /about",
            "",
            "진도 체크",
            "- 북클럽 단체방에 올라오는 메시지에서 버튼(✅/🟡/🔴)을 눌러주세요.",
            "",
            "책갈피(문장 메모) — 1:1 대화에서만",
            "- 저장: /bookmark 인상 깊은 문장",
            "- 보기: /bookmarks",
            "- 수정: /bookmark_edit #id 수정할 문장",
            "- 삭제: /bookmark_delete #id",
            "",
            "취향 스냅샷 — 1:1 대화에서만",
            "- /taste",
            "- /taste_summary",
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
            "📚 AI Book Club Bot 소개",
            "",
            "이 봇은 독서모임을 ‘꾸준히, 즐겁게’ 이어가도록 돕는 북클럽 도우미예요.",
            "",
            "- ✅ 진도 체크: 매주 올라오는 진도 체크에 버튼으로 응답해요.",
            "- 🔖 북마크: 읽다가 마음에 든 문장을 책갈피처럼 저장해요.",
            "- 🧭 취향 요약: 저장한 문장을 바탕으로 내 독서 취향을 조금씩 알아가요. (/taste, 베타)",
            "",
            "사용법은 /guide 를 참고해주세요.",
        ]
    )
    await msg.reply_text(text)


def _parse_bookmark_args(arg_text: str) -> tuple[Optional[int], str]:
    """
    Supported formats:
    - /bookmark <text>
    - /bookmark <page> | <text>
    - /bookmark p<page> | <text>   (e.g. p120 | ...)
    """
    raw = (arg_text or "").strip()
    if not raw:
        return None, ""

    if "|" not in raw:
        return None, raw

    left, right = raw.split("|", 1)
    left = left.strip()
    right = right.strip()
    if not right:
        return None, ""

    left_norm = left.lower().lstrip("p").strip()
    if left_norm.isdigit():
        return int(left_norm), right

    return None, raw  # fallback: treat whole as text if page parsing fails


async def cmd_bookmark(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    arg_text = " ".join(context.args) if context.args else ""
    page, text = _parse_bookmark_args(arg_text)
    if not text:
        await msg.reply_text("사용법: /bookmark 120 | 인상 깊은 문장\n또는: /bookmark 인상 깊은 문장")
        return

    full_name = " ".join([p for p in [user.first_name, user.last_name] if p]).strip() or None
    max_per_user = min(100, max(1, int(settings.bookmarks_max_per_user)))

    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            insert_bookmark_postgres(
                conn,
                telegram_user_id=user.id,
                telegram_username=user.username,
                full_name=full_name,
                page=page,
                text=text,
            )
            enforce_bookmarks_limit_postgres(
                conn, telegram_user_id=user.id, max_per_user=max_per_user
            )
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            insert_bookmark_sqlite(
                conn,
                telegram_user_id=user.id,
                telegram_username=user.username,
                full_name=full_name,
                page=page,
                text=text,
            )
            enforce_bookmarks_limit_sqlite(conn, telegram_user_id=user.id, max_per_user=max_per_user)
        finally:
            conn.close()

    page_part = f"(p.{page}) " if page is not None else ""
    await msg.reply_text(f"저장했어요. {page_part}{text}")


async def cmd_bookmarks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    limit = 10
    query = None
    if context.args:
        if context.args[0].isdigit():
            limit = max(1, min(30, int(context.args[0])))
            if len(context.args) > 1:
                query = " ".join(context.args[1:]).strip() or None
        else:
            query = " ".join(context.args).strip() or None

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=user.id, query=query, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=user.id, query=query, limit=limit)
        finally:
            conn.close()

    if not items:
        await msg.reply_text("저장된 문장이 아직 없어요. /bookmark 로 먼저 저장해보세요.")
        return

    header = "내가 저장한 문장(최근 순, 기본 10개)" if not query and limit == 10 else "내가 저장한 문장(최근 순)"
    if query:
        header = f"검색 결과: {query}"
    lines = [header]
    for i, b in enumerate(items, start=1):
        page_part = f"p.{b.page} " if b.page is not None else ""
        lines.append(f"- #{b.id} {page_part}\"{b.text}\"")
    await msg.reply_text("\n".join(lines))


async def cmd_bookmark_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    if not context.args:
        await msg.reply_text("사용법: /bookmark_edit #id 수정할 문장\n또는: /bookmark_edit #id 페이지번호 | 수정할 문장")
        return

    first = context.args[0].strip()
    if not first.startswith("#") or not first[1:].isdigit():
        await msg.reply_text("사용법: /bookmark_edit #id 페이지번호 | 수정할 문장\n예) /bookmark_edit #3 57 | 수정할 문장")
        return

    bookmark_id = int(first[1:])
    raw = " ".join(context.args[1:]) if len(context.args) > 1 else ""
    page, text = _parse_bookmark_args(raw)
    if not text:
        await msg.reply_text("수정할 문장을 입력해주세요. 예) /bookmark_edit #3 57 | 수정할 문장")
        return

    ok = False
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            ok = update_bookmark_postgres(
                conn, bookmark_id=bookmark_id, telegram_user_id=user.id, page=page, text=text
            )
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            ok = update_bookmark_sqlite(
                conn, bookmark_id=bookmark_id, telegram_user_id=user.id, page=page, text=text
            )
        finally:
            conn.close()

    if not ok:
        await msg.reply_text("해당 #id의 책갈피를 찾지 못했어요. /bookmarks 로 확인해보세요.")
        return

    page_part = f"(p.{page}) " if page is not None else ""
    await msg.reply_text(f"수정했어요. #{bookmark_id} {page_part}{text}")


async def cmd_bookmark_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    if not context.args:
        await msg.reply_text("사용법: /bookmark_delete #id\n예) /bookmark_delete #3")
        return

    first = context.args[0].strip()
    if not first.startswith("#") or not first[1:].isdigit():
        await msg.reply_text("사용법: /bookmark_delete #id\n예) /bookmark_delete #3")
        return

    bookmark_id = int(first[1:])
    ok = False
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            ok = delete_bookmark_postgres(conn, bookmark_id=bookmark_id, telegram_user_id=user.id)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            ok = delete_bookmark_sqlite(conn, bookmark_id=bookmark_id, telegram_user_id=user.id)
        finally:
            conn.close()

    if not ok:
        await msg.reply_text("해당 #id의 책갈피를 찾지 못했어요. /bookmarks 로 확인해보세요.")
        return

    await msg.reply_text(f"삭제했어요. #{bookmark_id}")


def _extract_keywords(texts: List[str]) -> List[str]:
    # Lightweight keyword extraction (space-based) with better Korean stopwords.
    stop = {
        "그리고",
        "그런데",
        "하지만",
        "그래서",
        "또",
        "또는",
        "이건",
        "저건",
        "그것",
        "이것",
        "저것",
        "저는",
        "나는",
        "내가",
        "너무",
        "정말",
        "그냥",
        "진짜",
        "약간",
        "보이는",
        "것",
        "것조차",
        "수",
        "등",
    }
    tokens: List[str] = []
    for t in texts:
        # Drop author suffix like " - 마르쿠스 아우렐리우스"
        t = t.split(" - ")[0]
        for raw in t.replace("\n", " ").split():
            w = raw.strip(".,!?\"'()[]{}:;~`")
            if len(w) < 2:
                continue
            if w in stop:
                continue
            tokens.append(w)
    return [w for w, _ in Counter(tokens).most_common(8)]


def _dot(a: List[float], b: List[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _l2(a: List[float]) -> float:
    return max(1e-12, sum(x * x for x in a) ** 0.5)


def _cosine(a: List[float], b: List[float]) -> float:
    return _dot(a, b) / (_l2(a) * _l2(b))


def _cluster_embeddings(vectors: List[List[float]], threshold: float = 0.82) -> List[List[int]]:
    """
    Greedy clustering by cosine similarity against cluster centroid.
    Returns clusters of indices.
    """
    clusters: List[List[int]] = []
    centroids: List[List[float]] = []

    for idx, v in enumerate(vectors):
        best_i = -1
        best_sim = -1.0
        for ci, c in enumerate(centroids):
            sim = _cosine(v, c)
            if sim > best_sim:
                best_sim = sim
                best_i = ci

        if best_sim >= threshold and best_i >= 0:
            clusters[best_i].append(idx)
            # update centroid (simple mean)
            inds = clusters[best_i]
            dim = len(v)
            new_c = [0.0] * dim
            for j in inds:
                vv = vectors[j]
                for k in range(dim):
                    new_c[k] += vv[k]
            n = float(len(inds))
            centroids[best_i] = [x / n for x in new_c]
        else:
            clusters.append([idx])
            centroids.append(v)

    # sort by size desc
    clusters.sort(key=len, reverse=True)
    return clusters


def _get_openai_embeddings(api_key: str, model: str, texts: List[str]) -> List[List[float]]:
    client = OpenAI(api_key=api_key)
    resp = client.embeddings.create(model=model, input=texts)
    # Ensure stable order by index
    data = sorted(resp.data, key=lambda d: d.index)
    vectors = [d.embedding for d in data]
    if len(vectors) != len(texts):
        raise TypeError(f"embeddings length mismatch: got {len(vectors)} for {len(texts)} texts")
    return vectors


def _taste_snapshot_from_bookmarks(
    *,
    bookmarks: List[Bookmark],
    embeddings: List[List[float]],
    max_clusters: int,
) -> Tuple[str, List[str]]:
    if len(bookmarks) <= 2:
        # With too little data, don't pretend to infer themes.
        reps = []
        for b in bookmarks[:2]:
            page_part = f"p.{b.page} " if b.page is not None else ""
            reps.append(f"- {page_part}\"{b.text}\"")
        header = "\n".join(
            [
                "내 독서 취향 스냅샷",
                f"- 분석 기준: 최근 {len(bookmarks)}개 책갈피",
                "",
                "아직 책갈피가 적어서 취향을 요약하기엔 데이터가 부족해요.",
                "문장을 몇 개 더 저장하면 테마가 더 잘 잡혀요.",
                "",
            ]
        )
        return header + "\n".join(reps), []

    texts = [b.text for b in bookmarks]
    clusters = _cluster_embeddings(embeddings, threshold=0.82)[:max_clusters]

    sections: List[str] = []
    theme_lines: List[str] = []
    for ci, inds in enumerate(clusters, start=1):
        cluster_texts = [texts[i] for i in inds]
        kws = _extract_keywords(cluster_texts)[:5]
        theme = " / ".join(kws) if kws else "주제"
        theme_lines.append(theme)

        # representative quotes: first 2 items in cluster order
        reps = []
        for i in inds[:2]:
            b = bookmarks[i]
            page_part = f"p.{b.page} " if b.page is not None else ""
            reps.append(f"- {page_part}\"{b.text}\"")

        sections.append("\n".join([f"{ci}) {theme}", *reps]))

    header = "\n".join(
        [
            "내 독서 취향 스냅샷",
            f"- 분석 기준: 최근 {len(bookmarks)}개 책갈피",
            f"- 주요 테마: {', '.join(theme_lines)}" if theme_lines else "- 주요 테마: (분석 중)",
            "",
        ]
    )
    return header + "\n\n".join(sections), theme_lines


def _select_representative_bookmarks(
    bookmarks: List[Bookmark],
    embeddings: List[List[float]],
    *,
    max_clusters: int,
    max_quotes: int,
) -> List[Bookmark]:
    if not bookmarks:
        return []
    if len(bookmarks) <= max_quotes:
        return bookmarks
    clusters = _cluster_embeddings(embeddings, threshold=0.82)[: max(1, max_clusters)]
    reps: List[Bookmark] = []
    for inds in clusters:
        for i in inds[:2]:
            reps.append(bookmarks[i])
            if len(reps) >= max_quotes:
                return reps
    return reps[:max_quotes]


def _build_taste_summary_prompt(quotes: List[Bookmark]) -> str:
    lines: List[str] = []
    for b in quotes:
        page_part = f"(p.{b.page}) " if b.page is not None else ""
        lines.append(f"- {page_part}{b.text}")
    return "\n".join(lines)


def _get_openai_taste_summary(api_key: str, model: str, prompt: str) -> str:
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "너는 온라인 독서모임을 돕는 도우미야. 사용자의 책갈피 문장들을 보고 독서 취향을 한국어로 요약해.",
            },
            {
                "role": "user",
                "content": (
                    "아래는 내가 저장한 책갈피 문장들이야.\n\n"
                    f"{prompt}\n\n"
                    "요청:\n"
                    "- 한국어로 1~3문장만\n"
                    "- 질문 금지\n"
                    "- 목록/불릿 금지\n"
                    "- 단정적인 진단/평가 톤 금지 (부드럽게 '경향' 정도로)\n"
                ),
            },
        ],
        temperature=0.5,
    )
    text = (resp.choices[0].message.content or "").strip()
    return text


async def cmd_taste(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Member-facing "taste snapshot" based on their bookmarks.
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    limit = max(1, min(100, int(settings.taste_bookmarks_limit)))
    max_clusters = max(1, min(8, int(settings.taste_max_clusters)))

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=user.id, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=user.id, limit=limit)
        finally:
            conn.close()

    if not items:
        await msg.reply_text("아직 저장된 책갈피가 없어요. 먼저 /bookmark 로 문장을 저장해보세요.")
        return

    if settings.embeddings_provider != "openai" or not settings.openai_api_key:
        await msg.reply_text(
            "취향 스냅샷(임베딩 기반)을 사용하려면 운영진이 EMBEDDINGS_PROVIDER=openai 와 OPENAI_API_KEY를 설정해야 해요."
        )
        return

    # Fetch embeddings in a thread to avoid blocking the event loop.
    # Ensure we don't send empty strings
    items = [b for b in items if (b.text or "").strip()]
    if not items:
        await msg.reply_text("분석할 수 있는 책갈피 문장이 없어요. /bookmark 로 문장을 저장해보세요.")
        return

    texts = [b.text for b in items]
    try:
        embeddings = await asyncio.to_thread(
            _get_openai_embeddings,
            settings.openai_api_key,
            settings.openai_embeddings_model,
            texts,
        )
    except AuthenticationError:
        await msg.reply_text("OpenAI API 키가 올바르지 않은 것 같아요. (OPENAI_API_KEY 확인 필요)")
        return
    except RateLimitError:
        await msg.reply_text("요청이 너무 많아서 잠시 제한됐어요. 잠깐 후 다시 시도해줘요.")
        return
    except APIConnectionError:
        await msg.reply_text("네트워크 문제로 취향 분석을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return
    except APIError as e:
        logger.info("OpenAI APIError while fetching embeddings", exc_info=True)
        await msg.reply_text(f"OpenAI 오류로 취향 분석을 불러오지 못했어요. ({e.__class__.__name__})")
        return
    except Exception as e:
        logger.info("Failed to fetch embeddings", exc_info=True)
        await msg.reply_text(f"지금은 취향 분석을 불러오지 못했어요. ({e.__class__.__name__})")
        return

    try:
        snapshot, _themes = _taste_snapshot_from_bookmarks(
            bookmarks=items, embeddings=embeddings, max_clusters=max_clusters
        )
    except Exception as e:
        logger.info("Failed to build taste snapshot", exc_info=True)
        await msg.reply_text(f"취향 스냅샷 생성에 실패했어요. ({e.__class__.__name__}: {str(e)[:120]})")
        return

    await msg.reply_text(snapshot)


async def cmd_taste_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Member-facing 1~3 line summary (LLM) based on representative bookmarks.
    if not await _require_private_chat(update):
        return
    if not await _require_member(update, context):
        return

    msg = update.effective_message
    user = update.effective_user
    settings: Settings = context.application.bot_data["settings"]
    if msg is None or user is None:
        return

    if settings.embeddings_provider != "openai" or not settings.openai_api_key:
        await msg.reply_text("이 기능을 사용하려면 운영진이 OPENAI_API_KEY를 설정해야 해요.")
        return

    limit = max(3, min(100, int(settings.taste_bookmarks_limit)))
    max_clusters = max(1, min(8, int(settings.taste_max_clusters)))
    max_quotes = max(3, min(12, int(settings.taste_summary_max_quotes)))

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=user.id, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=user.id, limit=limit)
        finally:
            conn.close()

    items = [b for b in items if (b.text or "").strip()]
    if len(items) < 2:
        await msg.reply_text("요약을 만들기엔 책갈피가 아직 너무 적어요. 문장을 2개 이상 저장해줘요.")
        return

    texts = [b.text for b in items]
    try:
        embeddings = await asyncio.to_thread(
            _get_openai_embeddings,
            settings.openai_api_key,
            settings.openai_embeddings_model,
            texts,
        )
    except Exception:
        logger.info("Failed to fetch embeddings for taste_summary", exc_info=True)
        await msg.reply_text("지금은 요약을 만들기 위한 분석을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    reps = _select_representative_bookmarks(items, embeddings, max_clusters=max_clusters, max_quotes=max_quotes)
    prompt = _build_taste_summary_prompt(reps)

    # Include current book title (if configured) to ground the summary.
    book_title = None
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            book_title = get_setting_postgres(conn, key="book_title")
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            book_title = get_setting_sqlite(conn, key="book_title")
        finally:
            conn.close()
    if book_title:
        prompt = f"[현재 읽는 책: {book_title}]\n" + prompt

    try:
        summary = await asyncio.to_thread(
            _get_openai_taste_summary,
            settings.openai_api_key,
            settings.openai_summary_model,
            prompt,
        )
    except AuthenticationError:
        await msg.reply_text("OpenAI API 키가 올바르지 않은 것 같아요. (OPENAI_API_KEY 확인 필요)")
        return
    except RateLimitError:
        await msg.reply_text("요청이 너무 많아서 잠시 제한됐어요. 잠깐 후 다시 시도해줘요.")
        return
    except APIConnectionError:
        await msg.reply_text("네트워크 문제로 요약을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return
    except APIError as e:
        logger.info("OpenAI APIError while generating taste summary", exc_info=True)
        await msg.reply_text(f"OpenAI 오류로 요약을 만들지 못했어요. ({e.__class__.__name__})")
        return
    except Exception as e:
        logger.info("Failed to generate taste summary", exc_info=True)
        await msg.reply_text(f"지금은 요약을 만들지 못했어요. ({e.__class__.__name__})")
        return

    if not summary:
        await msg.reply_text("요약 결과가 비어있어요. 잠시 후 다시 시도해줘요.")
        return

    # Ensure 1~3 lines (soft cap)
    lines = [ln.strip() for ln in summary.splitlines() if ln.strip()]
    lines = lines[:3]
    encouragement = "오늘은 10분만 읽고 책갈피 하나 남겨봐요."
    if not lines:
        lines = [encouragement]
    elif len(lines) < 3:
        lines.append(encouragement)
    else:
        lines[-1] = f"{lines[-1]} {encouragement}"
    await msg.reply_text("\n".join(lines))


async def cmd_set_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    title = " ".join(context.args).strip() if context.args else ""
    if not title:
        await msg.reply_text("사용법: /set_book <책 제목>")
        return
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            set_setting_postgres(conn, key="book_title", value=title)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            set_setting_sqlite(conn, key="book_title", value=title)
        finally:
            conn.close()
    await msg.reply_text(f"현재 책을 설정했어요: {title}")


async def cmd_show_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return
    title = None
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            title = get_setting_postgres(conn, key="book_title")
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            title = get_setting_sqlite(conn, key="book_title")
        finally:
            conn.close()
    await msg.reply_text(f"현재 책: {title or '(미설정)'}")


async def cmd_taste_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Admin-facing taste snapshot for a given member.
    if not await _require_admin(update, context):
        return

    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    if not context.args:
        await msg.reply_text("사용법: /taste_member @username\n또는: /taste_member <telegram_user_id>")
        return
    target_raw = context.args[0].strip()
    target_user_id: Optional[int] = None
    target_label = target_raw

    if target_raw.isdigit():
        target_user_id = int(target_raw)
    elif target_raw.startswith("@"):
        # Resolve via DB (works if the member has interacted/saved at least once)
        if is_postgres_url(settings.database_url):
            conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
            try:
                target_user_id = find_user_id_by_username_postgres(conn, username=target_raw)
            finally:
                conn.close()
        else:
            conn = connect_sqlite(settings.db_path)
            try:
                target_user_id = find_user_id_by_username_sqlite(conn, username=target_raw)
            finally:
                conn.close()
        if target_user_id is None:
            await msg.reply_text("해당 @username을 찾지 못했어요. (그 멤버가 책갈피/진도체크를 한 번이라도 해야 조회 가능해요.)")
            return
    else:
        await msg.reply_text("사용법: /taste_member @username\n또는: /taste_member <telegram_user_id>")
        return

    limit = max(1, min(100, int(settings.taste_bookmarks_limit)))
    max_clusters = max(1, min(8, int(settings.taste_max_clusters)))

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_bookmarks_postgres(conn, telegram_user_id=target_user_id, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_bookmarks_sqlite(conn, telegram_user_id=target_user_id, limit=limit)
        finally:
            conn.close()

    if not items:
        await msg.reply_text("해당 멤버의 책갈피가 아직 없어요.")
        return

    if settings.embeddings_provider != "openai" or not settings.openai_api_key:
        await msg.reply_text("EMBEDDINGS_PROVIDER=openai 와 OPENAI_API_KEY 설정이 필요해요.")
        return

    items = [b for b in items if (b.text or "").strip()]
    if not items:
        await msg.reply_text("해당 멤버의 책갈피 문장이 비어있어요.")
        return

    texts = [b.text for b in items]
    try:
        embeddings = await asyncio.to_thread(
            _get_openai_embeddings,
            settings.openai_api_key,
            settings.openai_embeddings_model,
            texts,
        )
    except AuthenticationError:
        await msg.reply_text("OpenAI API 키가 올바르지 않은 것 같아요. (OPENAI_API_KEY 확인 필요)")
        return
    except RateLimitError:
        await msg.reply_text("요청이 너무 많아서 잠시 제한됐어요. 잠깐 후 다시 시도해줘요.")
        return
    except APIConnectionError:
        await msg.reply_text("네트워크 문제로 취향 분석을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return
    except APIError as e:
        logger.info("OpenAI APIError while fetching embeddings (taste_member)", exc_info=True)
        await msg.reply_text(f"OpenAI 오류로 취향 분석을 불러오지 못했어요. ({e.__class__.__name__})")
        return
    except Exception as e:
        logger.info("Failed to fetch embeddings for admin taste_member", exc_info=True)
        await msg.reply_text(f"지금은 취향 분석을 불러오지 못했어요. ({e.__class__.__name__})")
        return

    try:
        snapshot, _themes = _taste_snapshot_from_bookmarks(
            bookmarks=items, embeddings=embeddings, max_clusters=max_clusters
        )
    except Exception as e:
        logger.info("Failed to build taste_member snapshot", exc_info=True)
        await msg.reply_text(f"취향 스냅샷 생성에 실패했어요. ({e.__class__.__name__}: {str(e)[:120]})")
        return

    await msg.reply_text(f"운영진용 멤버 취향 스냅샷 ({target_label})\n\n" + snapshot)


async def cmd_club_taste(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Admin-facing aggregate taste snapshot for the whole club.
    if not await _require_admin(update, context):
        return
    msg = update.effective_message
    settings: Settings = context.application.bot_data["settings"]
    if msg is None:
        return

    if settings.embeddings_provider != "openai" or not settings.openai_api_key:
        await msg.reply_text("EMBEDDINGS_PROVIDER=openai 와 OPENAI_API_KEY 설정이 필요해요.")
        return

    limit = max(50, min(300, int(settings.taste_bookmarks_limit) * 4))
    max_clusters = max(2, min(8, int(settings.taste_max_clusters)))

    items = []
    if is_postgres_url(settings.database_url):
        conn = connect_postgres(settings.database_url)  # type: ignore[arg-type]
        try:
            items = list_recent_bookmarks_all_postgres(conn, limit=limit)
        finally:
            conn.close()
    else:
        conn = connect_sqlite(settings.db_path)
        try:
            items = list_recent_bookmarks_all_sqlite(conn, limit=limit)
        finally:
            conn.close()

    if not items:
        await msg.reply_text("아직 북클럽 책갈피가 없어요.")
        return

    texts = [b.text for b in items]
    try:
        embeddings = await asyncio.to_thread(
            _get_openai_embeddings,
            settings.openai_api_key,
            settings.openai_embeddings_model,
            texts,
        )
    except Exception:
        logger.info("Failed to fetch embeddings for club_taste", exc_info=True)
        await msg.reply_text("지금은 북클럽 취향 분석을 불러오지 못했어요. 잠시 후 다시 시도해줘요.")
        return

    try:
        snapshot, _themes = _taste_snapshot_from_bookmarks(
            bookmarks=items, embeddings=embeddings, max_clusters=max_clusters
        )
    except Exception as e:
        logger.info("Failed to build club_taste snapshot", exc_info=True)
        await msg.reply_text(f"북클럽 취향 스냅샷 생성에 실패했어요. ({e.__class__.__name__}: {str(e)[:120]})")
        return

    await msg.reply_text("북클럽 전체 취향 스냅샷(종합)\n\n" + snapshot)


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
    app.add_handler(CommandHandler("bookmark", cmd_bookmark))
    app.add_handler(CommandHandler("bookmarks", cmd_bookmarks))
    app.add_handler(CommandHandler("bookmark_edit", cmd_bookmark_edit))
    app.add_handler(CommandHandler("bookmark_delete", cmd_bookmark_delete))
    app.add_handler(CommandHandler("taste", cmd_taste))
    app.add_handler(CommandHandler("taste_summary", cmd_taste_summary))
    app.add_handler(CommandHandler("set_book", cmd_set_book))
    app.add_handler(CommandHandler("show_book", cmd_show_book))
    app.add_handler(CommandHandler("taste_member", cmd_taste_member))
    app.add_handler(CommandHandler("club_taste", cmd_club_taste))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("send_weekly_check", cmd_send_weekly_check))
    app.add_handler(CallbackQueryHandler(on_progress_callback, pattern=r"^progress:"))

    return app

