# Date 8 최종 데모 실행서

> 이 문서는 최종 통합 전 소규모 dry-run의 과거 시연 기록이다. 현재 실행은
> [`../../guides/end-to-end-execution.md`](../../guides/end-to-end-execution.md), 최신 실제
> 결과는 [`../../reports/latest-end-to-end-run.md`](../../reports/latest-end-to-end-run.md)를
> 기준으로 한다.

## 1. 안전 원칙

- 기존 OpenAI Batch 32건은 다시 제출하지 않는다.
- Airflow LLM 단계는 `submit=false`로 시연한다.
- `.env`의 API key와 MinIO·Airflow 비밀번호는 화면에 노출하지 않는다.
- `docker compose down -v`와 volume prune을 실행하지 않는다.

## 2. 중지된 서비스 다시 시작

현재 컨테이너를 `stop`으로 보존한 상태라면 다음 명령을 사용한다.

```bash
cd news-comment-nlp-pipeline
docker compose start
docker compose -f infra/airflow/docker-compose.airflow.yml start
docker compose --profile serving up -d dashboard
```

컨테이너가 삭제된 환경에서는 다음처럼 생성한다.

```bash
docker compose up -d kafka postgres minio spark-master spark-worker
docker compose run --rm minio-init
docker compose -f infra/airflow/docker-compose.airflow.yml up -d
docker compose --profile serving up -d dashboard
```

## 3. 서비스 상태 확인

```bash
docker compose ps
docker compose -f infra/airflow/docker-compose.airflow.yml ps
```

| 화면 | 주소 | 확인 내용 |
|---|---|---|
| Airflow | `http://localhost:8082` | DAG와 기존 성공 Run |
| MinIO Console | `http://localhost:9101` | 5개 bucket과 객체 |
| Spark Master | `http://localhost:8080` | Master·Worker 연결 |
| Spark Worker | `http://localhost:8081` | Executor와 core |
| Streamlit | `http://localhost:8501` | PostgreSQL 분석과 Airflow snapshot |

Airflow standalone 비밀번호가 바뀐 경우 다음 명령으로 현재 값을 확인한다.

```bash
docker compose -f infra/airflow/docker-compose.airflow.yml logs airflow \
  | grep "Password for user"
```

## 4. 발표 화면 순서

### 날짜를 코드 수정 없이 바꿔 한 번에 실행

Airflow의 `news_comment_end_to_end_pipeline`을 실행할 때 DAG Run configuration에 날짜
범위와 대주제를 넣는다. 범위는 날짜별 mapped task와 날짜별 독립 OpenAI Batch로
확장된다.

```json
{
  "start_date": "2012-02-01",
  "end_date": "2012-02-01",
  "limit": 0,
  "selected_groups": ["economy"],
  "batch_limit": 10,
  "submit": false
}
```

`start_date`와 `end_date`를 다르게 지정하면 시작일과 종료일을 포함한 전체 기간을
코드 수정 없이 재현할 수 있다. `limit=0`은 선택한 그룹의 각 날짜 데이터를 전부
수집한다. 양수에는 상한을 두지 않는다. `batch_limit`은 날짜별 OpenAI Batch의 최대
동시 진행 수이며 기본값은 10, 허용 범위는 1~10이다. Airflow도 이 단계를 최대
10개까지만 동시에 시작하며, 실행별 공유 파일 잠금으로 실제 Batch 동시 진행 수를
`batch_limit`에 맞춘다. task가 실패하거나 종료돼도 잠금이 자동 해제된다.
MinIO·PostgreSQL·Slack은 항상 활성화되고,
소스 모드·출력 경로·Spark·모델·비용 한도는 DAG 내부 기본값을 사용한다.

`selected_groups`에는 다음 코드 중 1~4개를 중복 없이 넣는다.

| 코드 | 대주제 | subreddit 수 |
|---|---|---:|
| `politics` | 정치·국제 | 5 |
| `economy` | 사회·경제 | 5 |
| `technology` | 기술·디지털 | 5 |
| `environment` | 환경·과학 | 5 |

예를 들어 사회·경제와 환경·과학을 함께 수집하려면 다음처럼 지정한다.

```json
"selected_groups": ["economy", "environment"]
```

### 화면 1 — 구성도

- `docs/architecture/system-architecture.html`
- 수집, ingestion, 처리, 저장, 분석, 관측·오케스트레이션 순서로 설명

### 화면 2 — Airflow

- DAG: `news_comment_end_to_end_pipeline`
- task 정의 8개와 날짜별 mapped task, 날짜 범위·대주제 parameter 확인
- `submit=false`인 점을 확인해 중복 유료 요청이 없음을 설명

### 화면 3 — MinIO

- `news-raw`: 수집 원본
- `news-processed`: Spark 결과
- `news-llm`: Batch 요청·응답
- `news-reports`: 실행 로그·보고서
- `news-checkpoints`: Spark 재시작 상태

### 화면 4 — 검증 결과

- [Date 7 보고서](../date7/date7.md)
- [경제·사회 분석 결과](../date7/economy-social-results-01-31.md)
- [MinIO checkpoint 복구](../../../analysis/reports/minio-checkpoint-recovery-validation.md)

### 화면 5 — Langfuse

- generation 32건
- 일별·월간 token과 비용
- 원문·prompt·응답 본문을 보내지 않은 metadata-only 경계

### 화면 6 — Streamlit

- PostgreSQL의 감정 분포·상위 topic·최근 분석 결과
- Airflow가 최종 task에서 저장된 report를 다시 읽어 만든 실행 snapshot

## 5. 저장 결과 확인

PostgreSQL이 실행 중일 때 LLM 테이블 수를 확인한다.

```bash
docker compose exec postgres psql -U news_pipeline -d news_pipeline -c \
  "SELECT 'llm_batch_jobs' AS table_name, count(*) FROM llm_batch_jobs
   UNION ALL SELECT 'llm_batch_requests', count(*) FROM llm_batch_requests
   UNION ALL SELECT 'document_analyses', count(*) FROM document_analyses;"
```

예상 결과는 세 테이블 각각 32행이다.

MinIO checkpoint 검증의 공개 집계는 다음과 같다.

```text
run rows: 99 → 0 → 50
stored rows: 149
unique event_id: 149
duplicate: 0
checkpoint objects: 43
```

## 6. 발표 전 최종 검사

```bash
.venv311/bin/python -m pytest -q
docker compose config --quiet
docker compose -f infra/airflow/docker-compose.airflow.yml config --quiet
docker compose -f infra/airflow/docker-compose.airflow.yml exec -T airflow \
  airflow dags list-import-errors --output json
```

기대 결과:

- 테스트 `127 passed`
- Compose 명령 exit code 0
- Airflow import 오류 `[]`

## 7. 발표 종료 후 자원 정리

데이터와 컨테이너 상태를 유지하며 실행 자원만 해제한다.

```bash
docker compose stop
docker compose -f infra/airflow/docker-compose.airflow.yml stop
```
