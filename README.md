# AI Reading Club Agent (Telegram Bot)

온라인 독서모임 운영을 자동화하는 텔레그램 봇 (Phase 1 MVP).

## What it does (Phase 1)

- 매주 “진도 체크” 메시지를 그룹에 전송 (버튼: 완료/부분/아직)
- 버튼 클릭 응답을 SQLite에 저장
- `/send_weekly_check`로 수동 전송 가능

## Requirements

- Python 3.11+
- (옵션) Docker

## Quick start (local)

1) 가상환경 생성/활성화

```bash
python -m venv .venv
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

`.env`에 `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`를 채운 뒤 실행:

```bash
python -m app.main
```

기본은 **웹훅 URL이 없으면 로컬 테스트용 폴링 모드**로 실행돼요.

## Webhook (recommended)

서버에 HTTPS로 접근 가능한 도메인이 있을 때:

- `WEBHOOK_URL`: 예) `https://your-domain.com`
- `PORT`: 컨테이너/서버에서 열 포트 (기본 8080)
- `WEBHOOK_SECRET_TOKEN`: 선택(권장). Telegram이 보내는 요청 헤더 검증용

앱은 시작 시 `WEBHOOK_URL/telegram/webhook` 경로로 webhook을 자동 등록합니다.

## Railway deployment notes

- Railway에서 **Postgres 추가** 후 `DATABASE_URL`이 환경변수로 제공됩니다. 이 값이 있으면 SQLite 대신 **Postgres**를 사용합니다.
- Railway 서비스 도메인이 생기면 `WEBHOOK_URL`에 그 **HTTPS 도메인**을 넣어주세요.
- Railway에서 포트는 보통 플랫폼이 주입하는 값을 쓰는 게 안전합니다. 필요하면 `PORT`를 Railway의 포트 변수에 맞춰 설정하세요.

## Docker

```bash
cp .env.example .env
docker compose up --build
```

SQLite DB는 `./data/reading_club.sqlite3`에 저장됩니다.

