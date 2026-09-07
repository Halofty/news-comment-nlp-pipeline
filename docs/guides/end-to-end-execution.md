# 현재 end-to-end 실행 방법

이 문서는 `news_comment_end_to_end_pipeline` 한 번으로 수집 → Spark 처리 → MinIO 저장
→ OpenAI Batch → PostgreSQL 저장 → Slack 완료 알림 → serving snapshot 읽기를 재현하는
방법을 설명한다.

## 1. 사전 조건

- Docker와 Docker Compose가 실행 중이어야 한다.
- 프로젝트 `.env`에 OpenAI, Langfuse, Slack, PostgreSQL, MinIO 설정이 있어야 한다.
- 원본 Reddit archive와 수집한 웹 뉴스 파일이 프로젝트가 사용하는 로컬/MinIO 경로에
  있어야 한다.
- 자격 증명 값은 화면이나 Git에 올리지 않는다.

## 2. 서비스 시작

프로젝트 루트에서 다음을 실행한다.

```bash
docker compose up -d postgres minio spark-master spark-worker
docker compose --profile serving up -d dashboard
export AIRFLOW_UID="$(id -u)"
docker compose -f infra/airflow/docker-compose.airflow.yml up -d
```

| 서비스 | 주소 | 역할 |
|---|---|---|
| Airflow | `http://localhost:8082` | DAG 설정·실행·로그 확인 |
| Streamlit | `http://localhost:8501` | PostgreSQL 최종 결과 조회 |
| MinIO Console | `http://localhost:9101` | raw·processed·LLM·report 객체 확인 |
| Spark Master | `http://localhost:8080` | Spark application과 worker 확인 |
| Spark Worker | `http://localhost:8081` | executor와 resource 확인 |

로그인 ID와 비밀번호는 `.env`의 값을 사용하며 문서에는 실제 값을 기록하지 않는다.

## 3. DAG 설정과 Trigger

Airflow에서 `news_comment_end_to_end_pipeline`을 선택한 뒤 Trigger 화면의 JSON에 다음
항목을 지정한다.

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

| 항목 | 의미 |
|---|---|
| `start_date`, `end_date` | 양 끝을 포함하는 처리 날짜 범위 |
| `limit` | 날짜별 Reddit 최대 건수. `0`이면 제한 없음 |
| `selected_groups` | `politics`, `economy`, `technology`, `environment` 중 1~4개 |
| `batch_limit` | 동시에 제출·대기하는 날짜별 OpenAI Batch 수, 1~10 |
| `submit` | `false`: 유료 제출 전까지, `true`: 실제 Batch와 저장·알림까지 |

날짜마다 하나의 mapped OpenAI Batch task가 만들어지고, 그 Batch 안에는 선택한
대주제별 요청이 들어간다. 먼저 `submit=false`로 입력 건수와 예상 비용을 확인한 뒤,
실제 분석이 필요할 때만 같은 조건으로 `submit=true`를 실행한다.

## 4. 단일 실행의 10단계

| 순서 | Airflow task | 확인 내용 |
|---:|---|---|
| 1 | `prepare_parameters` | 날짜별 실행 설정 생성 |
| 2 | `prepare_llm_storage` | PostgreSQL LLM migration을 한 번 적용 |
| 3 | `collect_and_merge_sources[]` | Reddit·웹 뉴스 수집과 TextEvent 병합 |
| 4 | `prepare_spark[]` | 날짜별 Spark 경로와 실행 설정 생성 |
| 5 | `run_spark[]` | 계약 검사·품질 분류·중복 제거·Parquet 저장 |
| 6 | `verify_spark[]` | 입력·저장·거부·중복 행 회계 검증 |
| 7 | `store_processed_in_minio[]` | Spark 결과와 report를 MinIO에 게시 |
| 8 | `build_group_daily_batch[]` | 대주제별 v3 요청과 비용 preflight 생성 |
| 9 | `submit_wait_validate_store_notify[]` | 제출·대기·검증·PostgreSQL/Langfuse 저장·Slack 알림 |
| 10 | `read_final_result[]` | 단계별 건수를 serving snapshot으로 읽기 |

## 5. 완료 확인

1. Airflow Grid에서 DAG Run과 모든 mapped task가 초록색인지 확인한다.
2. 실패한 task는 로그의 원인을 수정한 뒤 해당 task부터 Clear하여 재실행한다. 이미
   완료된 OpenAI Batch가 있으면 `batch-state.json`의 ID를 재사용하므로 중복 제출하지
   않는다.
3. MinIO에서 날짜·run ID 경로의 processed, LLM, report 객체를 확인한다.
4. Slack에서 날짜·대주제·완료 상태·시작/종료/소요시간·감정·topic 알림을 확인한다.
5. Streamlit에서 schema 버전을 v3로 두고 분석 결과와 세부 정서를 조회한다.

발표용 저장 결과 화면은 `docs/streamlit_result.png`로 캡처한다. 최신 실제 실행 수치는
[최신 end-to-end 실행 기록](../reports/latest-end-to-end-run.md)에 있다.

## 6. 종료와 재시작

```bash
docker compose -f infra/airflow/docker-compose.airflow.yml down
docker compose --profile serving down
```

`down`은 컨테이너를 중지·제거하지만 named volume과 bind-mounted Airflow 데이터를
삭제하지 않는다. `down -v`는 영속 volume을 삭제하므로 사용하지 않는다.
