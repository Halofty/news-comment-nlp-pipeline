# Streamlit 서빙과 OpenAI Batch Slack 알림

## 1. 구현 범위

Streamlit은 최종 PostgreSQL 테이블을 read-only SQL로 조회한다. 분석 건수·감정 분포·
상위 topic·최근 분석·Batch 비용을 보여 주고, end-to-end Airflow DAG의 마지막 task가
실제 Spark report를 다시 읽어 만든 `serving-snapshot.json`도 함께 표시한다.

Slack 알림은 OpenAI Batch를 새로 제출하지 않는다. 이미 제출된 `batch_id`를 polling하며
완료·실패·만료·취소의 terminal 상태가 되었을 때만 알림을 보낸다. 완료 상태에서는 결과 파일을
내려받아 manifest와 대조한 뒤 검증된 대표 감정·감정 점수·긍정/중립/부정 분포·
양극화 수준과 점수, topic 상위 5개를 함께 전송한다.

## 2. Dashboard 실행

```bash
docker compose up -d postgres
docker compose --profile serving up -d --build dashboard
```

브라우저에서 `http://localhost:8501`을 연다. 기본 조회 대상은 Compose 내부의
`news_pipeline` PostgreSQL이며 다음 항목을 확인한다.

- `document_analyses`의 분석 건수와 감정 분포
- `topics` 배열을 펼쳐 계산한 상위 topic
- `llm_batch_jobs`의 상태·token·비용
- `data/airflow-output/**/serving-snapshot.json` 중 최신 실행 회계

현재 실데이터 확인값은 분석 32행, Batch 32행이다.

## 3. Slack App Manifest로 생성

이 프로젝트는 Slack App 설정을 재현할 수 있도록
[`infra/slack/app-manifest.yml`](../../infra/slack/app-manifest.yml)을 사용한다.

1. Slack의 **Your Apps**에서 **Create New App**을 선택한다.
2. **From an app manifest**를 선택한다.
3. 알림을 사용할 Workspace를 고른다.
4. **YAML** 형식을 선택하고 `infra/slack/app-manifest.yml` 전체를 붙여 넣는다.
5. 설정 요약에서 App 이름과 `incoming-webhook` scope만 있는지 확인한 뒤 생성한다.
6. 생성된 App의 **Incoming Webhooks**에서 **Add New Webhook to Workspace**를 누른다.
7. 알림 채널을 선택하고 승인한 뒤 생성된 Webhook URL을 복사한다.

현재 알림은 단방향 메시지 전송만 하므로 Events API, Request URL, Socket Mode,
`chat:write` scope와 Bot Token은 필요하지 않다. 비공개 채널을 선택하려면 Webhook을
설치하는 사용자가 그 채널에 먼저 참여해야 한다.

Manifest API로 App 생성 자체를 자동화할 수도 있지만 단일 Workspace 로컬 프로젝트에는
Slack 화면에서 Manifest를 붙여 넣는 방식이 적합하다. Manifest API 자동화에는 12시간
후 만료되는 별도 App Configuration Token 관리가 추가로 필요하다.

## 4. Webhook 환경변수 설정

Slack Workspace에서 알림을 받을 채널용 Incoming Webhook URL을 만든 뒤 로컬 `.env`에
다음 값을 넣는다. URL은 비밀정보이므로 Git에 커밋하거나 로그에 출력하지 않는다.

```dotenv
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
OPENAI_BATCH_POLL_INTERVAL_SECONDS=60
OPENAI_BATCH_MONITOR_TIMEOUT_SECONDS=90000
```

Airflow Compose를 다시 생성하면 환경변수가 반영된다.

```bash
docker compose -f infra/airflow/docker-compose.airflow.yml up -d --force-recreate airflow
```

## 5. Airflow 알림 DAG

Airflow에서 `openai_batch_slack_notification`을 수동 실행하며 다음 Param을 넣는다.

| Param | 의미 |
|---|---|
| `batch_id` | 이미 제출한 OpenAI Batch ID |
| `manifest_path` | 요청 custom ID와 원문 hash를 가진 manifest |
| `output_root` | 상태·원본 응답·검증 결과·알림 상태 저장 위치 |
| `poll_interval_seconds` | Batch 조회 주기 |
| `timeout_seconds` | 최대 감시 시간, 기본 25시간 |
| `slack_enabled` | `true`면 실제 Webhook, `false`면 로그 preview |

결과 알림에는 Batch ID·데이터 날짜·대주제·시작 시각과 함께 terminal 상태·종료 시각·소요 시간·완료/실패
건수·검증 건수·대주제별 대표 감정·감정 점수·감정 분포·양극화·상위 topic이 전달된다.
날짜와 대주제는 manifest의 `period`와 `group`을 사용하고 시간은 OpenAI Batch의
`in_progress_at`과 terminal timestamp를 KST로 변환한다.
`notification-state.json`이 같은 Batch·상태·전달 모드의 재전송을 막는다. Preview와
실제 Webhook은 별도 모드이므로 Preview 기록이 향후 실제 전송을 차단하지 않는다.

## 6. CLI 실행

Airflow 없이도 동일 코드를 실행할 수 있다.

```bash
set -a
source .env
set +a
python -m jobs.watch_openai_batch \
  --batch-id batch_xxx \
  --manifest data/llm/.../manifest.jsonl \
  --result-output data/llm_response/slack/batch_xxx/results.raw.jsonl \
  --validated-output data/llm_response/slack/batch_xxx/results.validated.jsonl \
  --batch-state data/llm_response/slack/batch_xxx/batch-state.json \
  --notification-state data/llm_response/slack/batch_xxx/notification-state.json
```

`--dry-run-slack`을 추가하면 Slack 대신 payload를 표준 출력으로 확인한다.
`--once`는 한 번 조회하고 아직 완료되지 않았다면 정상 종료한다.

## 7. Airflow Task 실패 알림 (Error-Alert 연동)

OpenAI Batch 완료 알림과는 별도로, Airflow DAG의 **어떤 task든 재시도를 모두 소진하고
최종 실패했을 때** 자동으로 [Error-Alert](https://github.com/Halofty/Error-Alert)
서버의 `/ingest` 엔드포인트에 실패 사실을 보고한다. Error-Alert는 이 프로젝트와 별개로
사용자의 Termux/GalaxyBook에서 독립 프로세스(`ingest_server.py` + `socket_listener.py`)로
돌아가며, 여러 프로젝트가 공유하는 개인용 알림 서버다. 이 프로젝트 쪽은 실패 사실만
보고하고, 중복 제거·일간/주간 요약·실제 Slack 전송·버튼 상호작용은 전부 Error-Alert
서버의 책임이다.

구현은 `orchestration/error_notifications.py`의 `notify_task_failure`이며, DAG의
`default_args["on_failure_callback"]`으로 모든 task(mapped task 포함, 날짜별 index도
함께)에 한 번에 연결돼 있다. 보고 자체가 실패해도(서버 다운, 네트워크 불가, 토큰
불일치 등) 절대 예외를 다시 던지지 않는다 — 보고 실패가 원래 task 실패를 가리거나
DAG를 추가로 실패시키지 않도록 막기 위함이다.

### 연동 설정

1. Error-Alert 저장소의 안내대로 `ingest_server.py`와 `socket_listener.py`를 실행할
   기기(Termux/GalaxyBook 등)에 `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`,
   `SLACK_CHANNEL_ID`, `ALERT_SERVER_TOKEN`, `ALERT_SERVER_PORT`를 설정하고 두
   프로세스를 기동한다(이 부분은 이 저장소가 아니라 Error-Alert 저장소 몫이다).
2. Airflow 컨테이너가 그 `ingest_server.py`에 네트워크로 닿을 수 있는 주소를
   확인한다(같은 LAN이면 사설 IP, 아니면 리버스 프록시/터널 주소).
3. `.env`에 다음을 추가한다.

   ```dotenv
   ALERT_SERVER_URL=http://<ingest 서버 주소>:8001
   ALERT_SERVER_TOKEN=<Error-Alert의 ALERT_SERVER_TOKEN과 동일한 값>
   ```

4. Airflow Compose를 다시 생성해 환경변수를 반영한다.

   ```bash
   docker compose -f infra/airflow/docker-compose.airflow.yml up -d --force-recreate airflow
   ```

`ALERT_SERVER_URL` 또는 `ALERT_SERVER_TOKEN`이 비어 있으면 보고를 건너뛰고 경고
로그만 남긴다(DAG 자체는 막지 않는다).

### 보고 내용

Error-Alert의 `/ingest` 계약은 다섯 필드만 받는다: `dag_id`, `task_id`, `error_type`,
`message`, `occurred_at`. mapped task의 날짜 index, Run ID, 시도 횟수, Airflow 로그
링크는 별도 필드가 없어 `message` 문자열 끝에 `(run=... try=... log=...)` 형태로
덧붙인다.

| 필드 | 예시 |
|---|---|
| `dag_id` | `news_comment_end_to_end_pipeline` |
| `task_id` | `run_kafka_spark_batch[5]` — mapped task면 `[날짜 index]` 포함 |
| `error_type` | 예외 class 이름, 예: `OffsetOutOfRangeException` |
| `message` | 예외 메시지(800자 초과 시 생략 표시) + `(run=... try=... log=...)` |
| `occurred_at` | ISO 8601 UTC 시각 |

메시지는 800자로 자르며, API key·password·token·Slack Webhook URL로 보이는 값은
전송 전에 마스킹한다. 전체 stack trace나 Spark stdout/stderr 같은 상세 내용은
`message`에 담긴 로그 링크를 눌러 Airflow에서 확인한다.

### 검증

```bash
python -m pytest -q tests/test_error_notifications.py
```

`build_ingest_payload`는 네트워크 없이 payload 내용만 검증하고, `notify_task_failure`는
가짜 `request_func`와 가짜 Airflow context로 서버 설정이 없을 때 건너뛰는지, HTTP
오류나 연결 실패가 예외로 전파되지 않는지를 검증한다. 실제 Airflow 컨테이너에서
`airflow tasks test`로 일부러 실패하는 더미 task를 실행해, 서버 미설정 시 경고 로그가
남고 서버 연결 실패 시에도 원래 task 실패만 기록되는 것을 직접 확인했다.

## 8. 현재 보장과 한계

- 완료·실패·만료·취소 상태를 terminal 상태로 처리한다.
- 처리 시작 상태에서는 Slack 메시지를 보내지 않는다.
- 완료 결과는 manifest 대조와 JSON Schema 검증 후 감정·양극화 결과를 표시하고 topic을 집계한다.
- 알림 상태 파일로 일반적인 재실행의 중복 전송을 방지한다.
- 프로세스가 Slack 전송 직후 상태 파일 저장 전에 비정상 종료되면 중복 알림 가능성이
  남아 있다.
- 현재 Airflow 구현은 polling하는 동안 worker slot 하나를 사용한다. 운영 환경에서는
  OpenAI `batch.completed` webhook 또는 deferrable/reschedule sensor로 교체할 수 있다.
- 실제 완료 Batch 1건으로 Incoming Webhook POST와 `sent` 상태 저장을 검증했다.
- 에러 알림(`on_failure_callback`)은 재시도를 모두 소진해 task가 최종 `FAILED`로
  바뀔 때만 발생한다. 재시도 대기(`UP_FOR_RETRY`) 상태에서는 알리지 않는다.
- 이 프로젝트는 실패를 있는 그대로 `/ingest`에 보고할 뿐 중복 제거를 하지 않는다.
  같은 task를 재실행할 때마다, 또는 같은 원인으로 여러 mapped task가 동시에
  실패하면 그만큼 여러 건이 보고된다. 이를 하나로 묶어 요약하는 것은 Error-Alert
  서버 쪽 책임이다.
- Error-Alert 서버가 꺼져 있거나 네트워크로 닿지 않으면 보고가 조용히 실패하고
  로그만 남는다(Airflow tasks test로 직접 확인함). Airflow 쪽에 재시도나 큐잉은
  없으므로, 그 시간 동안의 실패는 Error-Alert에 기록되지 않는다.
- Spark를 `subprocess`로 실행하는 단계(`run_kafka_spark_batch` 등)의 에러 메시지는
  종료 코드 위주로 짧을 수 있다. 실제 원인(Spark stdout/stderr)은 메시지의 로그
  링크로 이동해 Airflow task 로그에서 확인한다.
