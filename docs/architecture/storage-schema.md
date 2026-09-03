# PostgreSQL 저장 구조 설계

## 1. 목적

Spark가 처리한 원본·정제 이벤트와 LLM 분석 결과, 파이프라인 실행 상태를 PostgreSQL에 저장하기 위한 목표 Schema입니다.

핵심 수집·정제 저장 영역과 LLM Batch·문서 분석 테이블 migration이 구현됐습니다.
현재 migration은 `sql/migrations/`, Spark 적재 코드는 `storage/postgres.py`에 있습니다.
LLM 결과 JSONL 검증은 구현됐지만 PostgreSQL 적재 adapter 연결은 남아 있습니다.

## 2. 저장 원칙

- Kafka 원본 이벤트와 정제 결과를 분리합니다.
- 분석 결과는 감정, 토픽과 요약 테이블로 나눕니다.
- `event_id`를 원본 문서의 안정적인 식별자로 사용합니다.
- Spark `batch_id`와 LLM Batch 상태를 기록해 재실행을 제어합니다.
- 대량 적재는 행 단위 INSERT 대신 staging과 upsert를 사용합니다.
- 데이터 규모가 커지면 원본 이벤트를 객체 저장소로 분리하는 방안을 검토합니다.

## 3. 목표 테이블

```text
PostgreSQL
├── raw_text_events              # Kafka 원본 이벤트 (구현)
├── text_documents_clean         # 정제·비식별화 문서 (구현)
├── contract_rejected_events     # 계약 위반 원문과 Kafka 위치 (구현)
├── text_data_quality            # 결측·중복·지연·품질 통계
├── document_analyses            # 문서별 감정·토픽·키워드·요약 (migration 구현)
├── sentiment_window_metrics     # 시간대별 감정 집계
├── topic_window_metrics         # 시간대별 토픽 집계
├── topic_summaries              # 주요 토픽 이름과 요약
├── text_alert_events            # 급증·이상 이벤트
├── llm_batch_jobs               # LLM Batch 작업 상태 (migration 구현)
├── llm_batch_requests           # 요청별 결과와 재처리 상태 (migration 구현)
├── stream_batch_commits         # Spark micro-batch 중복 방지 (구현)
└── pipeline_run_history         # 작업 실행 이력
```

토큰·비용 대시보드는 Langfuse가 담당하므로 별도 `llm_usage_ledger`를 만들지 않습니다. PostgreSQL에는 처리 재개와 대조에 필요한 Batch·요청 상태, 실제 token 합계와 재시도 횟수만 유지합니다. Langfuse는 관측 복제본이며 장애나 trace 유실이 기준 상태에 영향을 주지 않습니다. 세부 경계는 [ADR-0001](../adr/0001-langfuse-deployment.md)을 따릅니다.

## 4. 영역별 책임

| 영역 | 주요 테이블 | 저장 내용 |
|---|---|---|
| 원본 | `raw_text_events` | `TextEvent v1`, Kafka 위치, 수신 시각 |
| 정제 | `text_documents_clean` | 정규화 텍스트, 언어, 품질 상태 |
| 품질 | `text_data_quality` | 실행별 결측·중복·지연·길이 통계 |
| 분석 | `document_analyses` | LLM 검증 완료 감정·토픽·키워드·요약 |
| 집계 | `sentiment_window_metrics`, `topic_window_metrics` | 출처·시간 window별 지표 |
| 운영 | `stream_batch_commits`, `pipeline_run_history` | 재시작·멱등성·실행 이력 |
| LLM 작업 | `llm_batch_jobs`, `llm_batch_requests` | Batch ID, custom ID, 상태와 재시도 |

## 5. Spark 적재 흐름

```text
Spark foreachBatch
→ PostgreSQL 트랜잭션과 consumer advisory lock 시작
→ (consumer_name, batch_id) 기존 commit 확인
→ 500행 단위 INSERT ... ON CONFLICT
→ stream_batch_commits에 batch_id·행 회계 기록
→ 트랜잭션 완료
```

같은 consumer의 `batch_id`가 이미 완료된 경우 행 iterator를 읽기 전에 적재를 건너뜁니다. `event_id`의 중복 처리는 Spark와 PostgreSQL 양쪽에서 방어하고, 계약 위반 레코드는 Kafka `topic/partition/offset`을 기본키로 사용합니다.

현재 Driver streaming insert는 1,000건 MVP에 맞춘 방식입니다. 대규모 확장 시 Spark JDBC staging 또는 object storage 기반 bulk load로 교체합니다.

## 6. 구현 전 결정 사항

- [x] 핵심 수집·정제 테이블 컬럼과 데이터 타입 확정
- [x] 원본 JSON text와 검색용 정규 컬럼 범위 결정
- [x] 핵심 기본키·외래키·unique constraint 결정
- [x] 시간·출처 조회 index 결정
- [x] 1,000건 MVP는 staging 없이 transaction upsert 사용
- [ ] 보존 기간과 개인정보 삭제 정책 결정
- [x] Langfuse와 PostgreSQL의 LLM 관측 데이터 책임 구분
- [x] LLM Batch·요청·문서 분석 migration 작성
- [ ] 검증된 LLM 결과의 PostgreSQL upsert adapter 연결
- [ ] migration과 rollback 절차 작성

## 7. 로컬 실행

```bash
docker compose up -d --wait postgres kafka spark-master spark-worker
docker compose --profile tools run --rm spark-runner \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  spark_jobs/streaming_consumer.py \
  --bootstrap-servers kafka:29092 \
  --input-topic raw-text \
  --dlq-topic raw-text-dlq \
  --output data/stream-output/text-events \
  --checkpoint data/stream-checkpoints/text-events \
  --available-now \
  --no-resolve-kafka-package
```

Compose의 `POSTGRES_DSN`으로 DB sink가 활성화됩니다. 호스트 직접 실행에서는 `--postgres-dsn`을 명시하지 않으면 파일 sink만 사용합니다.

## 8. 관련 문서

- [TextEvent v1 데이터 계약](data-contract.md)
- [LLM 분석 설계](llm-analysis-design.md)
- [장애 및 부하 테스트 계획](../planning/failure-and-load-test-plan.md)
- [멘토 피드백 구현 계획](../planning/feedback-implementation-plan.md)
- [PostgreSQL 통합 검증](../../analysis/reports/postgres-integration-validation.md)
