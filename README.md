# AI Reading Club Agent (Telegram Bot)

온라인 독서모임 운영을 자동화하는 텔레그램 봇.

## What it does

- 월별 책/모임 정보를 바탕으로 **4주 읽기 계획** 생성
- 주차 시작일 오전 9시에 멤버 단체방으로 **주차 진도 체크** 메시지 자동 전송
- 멤버 1:1 채팅에서 책갈피(북마크) 저장/조회/수정/삭제
- 운영진용: **월별(YYYY-MM) 책/모임 관리**, 주차 통계 등
- 단체방에서 `@봇이름 질문` 형태로 현재 설정된 책/모임/계획을 묻는 Q&A

## 월별(이달의 책) 구조

- 책/모임 정보는 **월(YYYY-MM) 단위로 저장**됩니다.
- 운영진은 `/set_month 2026-04`로 “이번에 설정할 월”을 지정한 뒤 책/일정/페이지를 설정합니다.
- 멤버는 `/book`으로 **이번 달 책**, `/book_month 2026-04`로 **특정 월의 책**을 확인할 수 있어요.
- 다음 달/다다음 달 책도 미리 세팅해둘 수 있고, 실제 자동 발송은 각 주차 `scheduled_date` 기준으로 동작합니다.

## Chat 구조 (중요)

- `MEMBER_CHAT_ID`: 북클럽 **멤버 단체방** chat id
- `ADMIN_CHAT_ID`: **운영진 단체방** chat id
- 운영진 명령은 **운영진 단체방 멤버만** 실행 가능
- 멤버 명령 중 북마크·`/my_progress`는 **멤버 1:1(private) 채팅에서만** 동작

chat id 확인은 운영진 전용 `/chatid`를 사용하세요.

## 주요 명령어(요약)

- 멤버:
  - `/book`, `/book_month YYYY-MM`, `/plan`
  - (1:1) `/my_progress`, `/bookmark`, `/bookmarks`
- 운영진:
  - `/set_month YYYY-MM`, `/book_search`, `/book_select`, `/set_book`
  - `/build_book_summary`, `/send_book_info`
  - `/set_meeting`, `/set_pages`, `/show_book`
  - `/build_month_plan`, `/show_month_plan`
  - `/send_weekly_check [주차]`
  - `/weekly_stats [주차]`, `/weekly_stats_detail [주차]`, `/share_weekly_stats [주차]`

## 운영 팁

- **페이지 수는 수동 확인 권장**: Google Books의 한국어판 페이지 수가 실제 책과 다를 수 있습니다. 실제 페이지 수는 `/set_pages`로 덮어써주세요.
- **책 요약 생성**: `/build_book_summary`는 책 제목/저자를 기준으로 설명문을 다시 검색해 보강한 뒤 요약을 만듭니다.
- **주차 계획 재생성**: 페이지 수나 모임 날짜를 바꿨다면 `/build_month_plan`을 다시 실행해야 주차 범위가 갱신됩니다.
- **진도 상태 업데이트**: 이전 주차 메시지가 방에 남아 있으면 같은 버튼을 다시 눌러 최신 상태로 덮어쓸 수 있습니다.
- **멘션 Q&A**: 단체방에서 `@봇이름 이번달 책 뭐야?`, `@봇이름 다음달 모임 언제야?`처럼 질문할 수 있습니다. 그룹에서 안 되면 BotFather의 Group Privacy 설정을 확인하세요.

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

선택(OpenAI 연동 시):

- `OPENAI_API_KEY`
- `OPENAI_EMBEDDINGS_MODEL` (기본: `text-embedding-3-small`)
- `OPENAI_SUMMARY_MODEL` (기본: `gpt-4o-mini`)
- (선택) `GOOGLE_BOOKS_API_KEY` (책 검색 안정성/쿼터 개선)

권장:

- `OPENAI_API_KEY`: 책 요약 생성, 멘션 Q&A, 주차별 안내문 품질 향상에 사용

## Docker

```bash
cp .env.example .env
docker compose up --build
```

SQLite DB는 `./data/reading_club.sqlite3`에 저장됩니다.

