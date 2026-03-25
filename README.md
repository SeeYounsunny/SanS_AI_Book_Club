# AI Reading Club Agent (Telegram Bot)

온라인 독서모임 운영을 자동화하는 텔레그램 봇.

## What it does

- 멤버 단체방에 “진도 체크” 메시지 전송 (버튼: 완료/부분/아직)
- 멤버 1:1 채팅에서 책갈피(북마크) 저장/조회/수정/삭제
- 북마크 기반 취향 스냅샷(`/taste`) + 1~3줄 취향 요약(`/taste_summary`)
- 운영진용: 현재 책 제목 설정/조회(`/set_book`, `/show_book`), 멤버/클럽 취향 보기(`/taste_member`, `/club_taste`)

## Chat 구조 (중요)

- `MEMBER_CHAT_ID`: 북클럽 **멤버 단체방** chat id
- `ADMIN_CHAT_ID`: **운영진 단체방** chat id
- 운영진 명령은 **운영진 단체방 멤버만** 실행 가능
- 멤버 명령 중 북마크/취향 기능은 **멤버 1:1(private) 채팅에서만** 동작

chat id 확인은 운영진 전용 `/chatid`를 사용하세요.

## Requirements

- Python 3.11+
- (옵션) Docker

## Quick start (local)

1) 가상환경 생성/활성화

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2) 의존성 설치

```bash
pip install -r requirements.txt
```

3) 환경변수 설정

```bash
cp .env.example .env
```

`.env`에 `TELEGRAM_BOT_TOKEN` 등 값을 채운 뒤 실행:

```bash
python -m app.main
```

## Webhook (recommended)

서버에 HTTPS로 접근 가능한 도메인이 있을 때:

- `WEBHOOK_URL`: 예) `https://your-domain.com` (끝에 `/` 없이)
- `PORT`: 서버가 listen 할 포트 (Railway가 주입하는 값 사용 권장)
- `WEBHOOK_SECRET_TOKEN`: 선택(권장). Telegram이 보내는 요청 헤더 검증용

앱은 시작 시 `WEBHOOK_URL/telegram/webhook` 경로로 webhook을 자동 등록합니다.

헬스체크:

- `GET /` → `ok=true`
- `GET /healthz` → `ok=true`

## Railway deployment notes

- Railway에서 **Postgres 추가** → `DATABASE_URL` 자동 제공 (있으면 SQLite 대신 Postgres 사용)
- Railway에서 서비스 **Public Domain 생성** → 그 HTTPS 도메인을 `WEBHOOK_URL`로 설정
- `PORT`는 보통 Railway가 자동 주입하므로 별도 설정 불필요

필수 환경변수:

- `TELEGRAM_BOT_TOKEN`
- `WEBHOOK_URL`
- `MEMBER_CHAT_ID`
- `ADMIN_CHAT_ID`

선택(취향 기능 사용 시 필요):

- `OPENAI_API_KEY`
- `OPENAI_EMBEDDINGS_MODEL` (기본: `text-embedding-3-small`)
- `OPENAI_SUMMARY_MODEL` (기본: `gpt-4o-mini`)

## Docker

```bash
cp .env.example .env
docker compose up --build
```

SQLite DB는 `./data/reading_club.sqlite3`에 저장됩니다.

