# Airflow 최종 단일 파이프라인

Airflow에는 `news_comment_end_to_end_pipeline` 하나만 활성 DAG로 노출한다. 과거
GDELT·Reddit·LLM·Slack 단계별 DAG 소스는 보존하지만 `dags/.airflowignore`로 제외한다.

## 실행 흐름

```text
Reddit 수집 ─┐
             ├→ TextEvent 병합·검증 → Spark → MinIO
Google News ─┘                              ↓
선택 대주제별 일별 polarization Batch 생성 → OpenAI 제출
→ 완료 대기·Schema v2 검증 → PostgreSQL → Slack → serving snapshot 읽기
```

`submit=false`이면 OpenAI 제출 이전의 요청 생성과 비용 검사까지 실행하고 종료한다.
`submit=true`이면 같은 DAG Run이 Batch 완료를 기다린 뒤 검증·저장·알림까지 수행한다.

## 서비스 시작

```bash
export AIRFLOW_UID="$(id -u)"
docker compose -f infra/airflow/docker-compose.airflow.yml up --build -d
```

Airflow UI는 `http://localhost:8082`에서 연다.

## 2012-02-02~28 경제·사회 dry-run

```json
{
  "start_date": "2012-02-02",
  "end_date": "2012-02-28",
  "limit": 0,
  "selected_groups": ["economy"],
  "batch_limit": 10,
  "submit": false
}
```

- 날짜 범위는 시작일과 종료일을 모두 포함한다.
- `limit=0`은 선택 subreddit의 각 날짜 댓글 전체를 뜻한다.
- `selected_groups`는 `politics`, `economy`, `technology`, `environment` 중 1~4개다.
- DAG는 날짜별 mapped task를 만들며, 수집·Spark·저장 전처리 단계는 동시에 최대
  2개, OpenAI Batch 단계는 동시에 최대 10개로 제한한다.
- OpenAI Batch는 날짜마다 하나이고 각 Batch의 요청 수는 선택한 대주제 수와 같다.
- `batch_limit`은 동시에 제출하고 완료를 기다릴 날짜별 OpenAI Batch 수이며 기본값은 10,
  허용 범위는 1~10이다. Airflow도 이 단계를 최대 10개까지만 동시에 시작하며,
  실행별 공유 파일 잠금이 실제 Batch 동시 진행 수를 `batch_limit`에 맞춘다.
  task가 실패하거나 종료되면 슬롯 잠금은 자동 해제된다.
- MinIO·PostgreSQL·Slack은 활성화되어 있으며 경로·Spark·모델 설정은 DAG 기본값을 사용한다.
- `submit=true`는 실제 비용이 발생하므로 preflight 비용 확인 후 사용한다.

## 과거 DAG

GDELT는 Reddit과 같은 분석 기간을 안정적으로 제공하지 못해 최종 파이프라인에서
제외했다. 기존 단계별 DAG와 GDELT Collector는 과거 과제와 검증 재현을 위해 소스만
보존한다. Airflow 메타데이터 정리 전 백업은
`data/airflow-home/airflow-before-final-dag-cleanup-20260906.db`에 있다.
