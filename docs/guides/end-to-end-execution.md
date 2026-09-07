# 현재 end-to-end 실행 방법

이 문서는 `news_comment_end_to_end_pipeline` 한 번으로 수집 → Kafka 적재 → bounded
Spark 처리 → MinIO 저장 → OpenAI Batch → PostgreSQL 저장 → Slack 완료 알림 → serving
snapshot 읽기를 재현하는 방법을 설명한다.

## 1. 사전 조건

- Docker와 Docker Compose가 실행 중이어야 한다.
- 프로젝트 `.env`에 OpenAI, Langfuse, Slack, PostgreSQL, MinIO 설정이 있어야 한다.
- 원본 Reddit archive와 수집한 웹 뉴스 파일이 프로젝트가 사용하는 로컬/MinIO 경로에
  있어야 한다.
- 자격 증명 값은 화면이나 Git에 올리지 않는다.

## 2. 서비스 시작

프로젝트 루트에서 다음을 실행한다.

```bash
docker compose up -d postgres minio kafka
docker compose --profile serving up -d dashboard
export AIRFLOW_UID="$(id -u)"
docker compose -f infra/airflow/docker-compose.airflow.yml up -d
```

| 서비스 | 주소 | 역할 |
|---|---|---|
| Airflow | `http://localhost:8082` | DAG 설정·실행·로그 확인 |
| Streamlit | `http://localhost:8501` | PostgreSQL 최종 결과 조회 |
| MinIO Console | `http://localhost:9101` | raw·processed·LLM·report 객체 확인 |
| Kafka | `localhost:9092` | 날짜별 `TextEvent v1` batch 적재 |

최종 DAG의 Spark는 Airflow 컨테이너 안에서 `local[2]`로 실행한다. 별도 Spark
Standalone cluster는 Structured Streaming 실험용이며 이 DAG의 필수 서비스가 아니다.

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

## 4. 단일 실행의 14단계

| 순서 | Airflow task | 확인 내용 |
|---:|---|---|
| 1 | `prepare_parameters` | 날짜별 실행 설정 생성 |
| 2 | `prepare_kafka_topics` | `raw-text`·DLQ topic 확인·생성 |
| 3 | `prepare_llm_storage` | PostgreSQL LLM migration을 한 번 적용 |
| 4 | `collect_and_merge_sources[]` | Reddit·웹 뉴스 수집과 TextEvent 병합 |
| 5 | `capture_kafka_start_offsets[]` | 발행 전 partition별 high watermark 기록 |
| 6 | `publish_to_kafka[]` | Run ID·날짜 metadata를 넣어 `raw-text` 발행 |
| 7 | `capture_kafka_end_offsets[]` | 발행 후 offset과 발행 건수 ledger 저장 |
| 8 | `prepare_kafka_spark[]` | 명시적 시작·종료 offset의 Spark 설정 생성 |
| 9 | `run_kafka_spark_batch[]` | 해당 offset 범위를 계약 검사·dedup·Parquet 처리 |
| 10 | `verify_kafka_spark_accounting[]` | 발행·선택·입력·처리 행 회계 검증 |
| 11 | `store_processed_in_minio[]` | Spark 결과와 report를 MinIO에 게시 |
| 12 | `build_group_daily_batch[]` | 대주제별 v3 요청과 비용 preflight 생성 |
| 13 | `submit_wait_validate_store_notify[]` | 제출·대기·검증·PostgreSQL/Langfuse 저장·Slack 알림 |
| 14 | `read_final_result[]` | 단계별 건수를 serving snapshot으로 읽기 |

여러 날짜 task가 같은 Kafka topic을 사용해도 offset 구간 안에서
`pipeline_run_id`와 `analysis_date`를 다시 필터링한다. 따라서 다른 날짜나 Run의 메시지가
동시에 들어오더라도 현재 날짜의 발행 건수와 Spark 입력 건수가 같아야 다음 단계로 간다.

## 5. 완료 확인

1. Airflow Grid에서 DAG Run과 14개 task 종류의 mapped task가 초록색인지 확인한다.
2. 실패한 task는 로그의 원인을 수정한 뒤 해당 task부터 Clear하여 재실행한다. 이미
   완료된 OpenAI Batch가 있으면 `batch-state.json`의 ID를 재사용하므로 중복 제출하지
   않는다. `run_kafka_spark_batch`가 `OffsetOutOfRangeException`으로 실패했다면 이미
   삭제된 offset을 가리키는 상태이므로 그 task만 재시도해서는 복구되지 않는다 —
   해당 날짜의 `capture_kafka_start_offsets`부터 다시 clear해 새 offset으로 재발행해야
   한다. 여러 날짜가 실패했다면 이미 성공한 날짜(map_index)는 건드리지 않도록
   `(task_id, map_index)` 단위로 clear 범위를 좁힌다. 원인과 복구 절차는
   [최신 실행 기록의 Kafka 데이터 손실 장애와 복구](../reports/latest-end-to-end-run.md#kafka-데이터-손실-장애와-복구)에
   정리했다.
3. Kafka ledger에서 시작·종료 offset과 `published_rows`를 확인하고 Spark report의
   `matched_run_rows`, `input_rows`, `accounted_rows`가 같은지 확인한다.
4. MinIO에서 날짜·run ID 경로의 raw, processed, LLM, report 객체를 확인한다.
5. Slack에서 날짜·대주제·완료 상태·시작/종료/소요시간·감정·topic 알림을 확인한다.
6. Streamlit에서 schema 버전을 v3로 두고 분석 결과와 세부 정서를 조회한다.

발표용 저장 결과 화면은 `docs/streamlit_result1.png`와 `docs/streamlit_result2.png`를
상·하단 순서로 연결해 기록했다. 최신 실제 실행 수치는
[최신 end-to-end 실행 기록](../reports/latest-end-to-end-run.md)에 있다.

## 6. Kafka→Spark 단계만 점검

외부 수집과 OpenAI 제출 없이 합성 2건으로 bounded offset과 Spark 행 회계만 확인할 수
있다.

```bash
docker compose -f infra/airflow/docker-compose.airflow.yml run --rm \
  airflow python scripts/smoke_bounded_kafka.py
```

성공 조건은 `published_rows = matched_run_rows = input_rows = accounted_rows = 2`이고
계약 거부와 DLQ는 0건이다.

## 7. 종료와 재시작

```bash
docker compose -f infra/airflow/docker-compose.airflow.yml down
docker compose --profile serving down
```

`down`은 컨테이너를 중지·제거하지만 named volume과 bind-mounted Airflow 데이터를
삭제하지 않는다. `down -v`는 영속 volume을 삭제하므로 사용하지 않는다.
