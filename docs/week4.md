# Week 4 — Kafka 이벤트와 Spark 전처리·저장

## 1. 범위와 완료 상태

Week 4에서는 GDELT 뉴스와 Reddit 댓글을 공통 JSON으로 Kafka에 전달하고, 같은 구조를 Spark로 전처리한 뒤 파일과 PostgreSQL에 저장하는 흐름을 확인합니다.

```text
TextEvent v1 JSONL
→ Kafka Producer
→ raw-text Topic
→ Spark Batch 또는 Structured Streaming
→ 계약 검사·텍스트 품질 검사·중복 제거
→ Parquet / PostgreSQL
```

| 요구사항 | 상태 | 검증 근거 |
|---|---|---|
| 데이터·메시지 명세 | 완료 | `TextEvent v1`, JSON Schema와 Kafka mapping |
| Kafka 이벤트 100건 이상 | 완료 | 합성 1,000건 실제 Broker 발행·소비 |
| Spark batch 처리 | 완료 | 동일 코드로 100건·1,000건 처리 |
| Kafka→Spark Streaming | 완료 | 1,000건 입력과 checkpoint 재시작 검증 |
| 파일 저장 | 완료 | 경로별 JSONL과 partitioned Parquet 검증 |
| PostgreSQL 저장 | MVP 완료 | 정상 981건·계약 거부 1건 멱등 적재 |
| 대규모 분산 저장 | 이후 계획 | JDBC staging 또는 object storage·bulk load 전환 |

## 2. 데이터·메시지 명세

### 2.1 TextEvent v1 필드

모든 Kafka 메시지 value는 다음 최상위 필드를 가진 `TextEvent v1` JSON입니다. 뉴스와 댓글에 해당하지 않는 필드도 생략하지 않고 `null`로 보냅니다.

| 필드 | 타입 | 의미 |
|---|---|---|
| `event_id` | string | 출처와 원본 ID로 만든 64자리 SHA-256, Kafka key와 중복 제거 기준 |
| `source_type` | enum | `news` 또는 `comment` |
| `source_name` | enum | `gdelt` 또는 `reddit` |
| `event_time` | ISO-8601 date-time | 원본 이벤트 발생·관측 시각, Kafka timestamp와 Spark watermark 기준 |
| `collected_at` | ISO-8601 date-time | Collector가 이벤트를 만든 UTC 시각 |
| `language` | string | 소문자 언어명 또는 `unknown` |
| `title` | string/null | 뉴스 제목, 댓글은 `null` |
| `text` | string | 비어 있지 않은 분석 대상 텍스트 |
| `url` | URI/null | 뉴스 URL, 댓글은 `null` |
| `community` | string/null | Reddit subreddit, 뉴스는 `null` |
| `engagement` | integer/null | Reddit score, 뉴스는 `null` |
| `schema_version` | integer | 현재 계약 버전 `1` |
| `metadata` | object | 출처별 추가 정보와 `text_scope` |

기계 판독 Schema는 [`sample/schema.json`](../sample/schema.json), 출처별 매핑과 변경 규칙은 [데이터 계약](data-contract.md)에 있습니다.

### 2.2 Kafka JSON 예시

아래 데이터는 실제 기사 원문이 아닌 공개 가능한 합성 뉴스 이벤트입니다.

```json
{
  "event_id": "47407ac86253daca461607abd7b4546dba44c89144e65de4529fee57378eb4c3",
  "source_type": "news",
  "source_name": "gdelt",
  "event_time": "2026-08-20T01:00:00Z",
  "collected_at": "2026-08-20T01:05:00Z",
  "language": "english",
  "title": "Synthetic headline about a new technology policy",
  "text": "Synthetic headline about a new technology policy",
  "url": "https://example.com/synthetic-news",
  "community": null,
  "engagement": null,
  "schema_version": 1,
  "metadata": {
    "domain": "example.com",
    "source_country": "United States",
    "query": "technology policy",
    "text_scope": "title_only"
  }
}
```

### 2.3 Kafka 메시지 mapping

| Kafka 항목 | 값 | 목적 |
|---|---|---|
| Topic | `raw-text` | 뉴스·댓글 공통 원본 이벤트 |
| Key | `event_id` | 동일 이벤트의 안정적인 partitioning과 추적 |
| Value | UTF-8 `TextEvent v1` JSON | Spark가 읽는 공통 입력 |
| Timestamp | `event_time` | event-time watermark와 지연 이벤트 처리 |

사용하는 Topic은 다음 두 개입니다.

| Topic | 용도 | local 설정 |
|---|---|---|
| `raw-text` | 정상 입력 | 3 partitions, 7일 보존 |
| `raw-text-dlq` | JSON·계약 오류 | 3 partitions, 30일 보존 |

로컬 단일 Broker이므로 replication factor는 `1`입니다. 운영 다중 Broker에서는 replication factor와 min ISR을 별도로 정해야 합니다.

## 3. Kafka 이벤트 1,000건 검증

### 3.1 실행 명령

```bash
# Kafka·PostgreSQL·Spark Standalone 시작
docker compose build spark-runner
docker compose up -d --wait postgres kafka spark-master spark-worker

# Topic 초기화
python -m jobs.init_kafka --bootstrap-servers localhost:9092

# 결정적으로 재생성 가능한 합성 이벤트 1,000건 생성
python -m jobs.generate_synthetic_events \
  --count 1000 \
  --output data/spark-input/synthetic-1000.jsonl

# Kafka raw-text에 최대 속도로 발행
python -m jobs.replay_to_kafka \
  --input data/spark-input/synthetic-1000.jsonl \
  --bootstrap-servers localhost:9092 \
  --topic raw-text \
  --speed 0

# 독립 Consumer group으로 1,000건 확인
python -m jobs.inspect_kafka \
  --bootstrap-servers localhost:9092 \
  --topic raw-text \
  --group-id week4-inspector \
  --from-beginning \
  --limit 1000 \
  --idle-timeout 10
```

이미 데이터가 남아 있는 Topic에서 `--from-beginning`을 사용하면 이전 실행도 함께 읽습니다. 정확히 새 1,000건을 비교할 때는 빈 개발 Topic을 사용하거나 offset 범위를 별도로 기록해야 합니다.

### 3.2 실제 검증 결과

| 단계 | 건수 | 설명 |
|---|---:|---|
| Producer 발행 | 1,000 | `TextEvent v1` 합성 이벤트 |
| Kafka→Spark Consumer 입력 | 1,000 | `raw-text`에서 읽은 이벤트 |
| 계약 parsing 성공 | 1,000 | 계약 오류 0건 |
| 중복 `event_id` | 19 | Spark 중복 제거 대상으로 분류 |
| 고유 출력 | 981 | 품질 상태가 결정된 최종 이벤트 |

행 회계는 `1,000 = 19 + 981`로 일치합니다. 이후 별도 malformed JSON 1건을 발행한 검증에서는 `raw-text-dlq`로 1건이 전달됐습니다.

## 4. Spark 전처리·저장

### 4.1 입력 구조

Spark Batch는 `TextEvent v1` JSONL을, Structured Streaming은 Kafka `raw-text` value를 읽습니다. 두 경로 모두 [`spark_jobs/schemas.py`](../spark_jobs/schemas.py)의 명시적 Schema와 [`transform_events()`](../spark_jobs/transformations.py)를 사용합니다.

```text
Batch: JSONL ───────────────────┐
                                ├→ TextEvent v1 Schema → 공통 transformation
Streaming: Kafka value JSON ───┘
```

Schema inference에 의존하지 않으므로 Kafka와 파일 입력의 필드 타입과 의미가 같습니다.

### 4.2 전처리 항목

1. JSON과 필수·추가 필드, enum, `schema_version` 검사
2. `event_time`, `collected_at`을 Spark timestamp로 parsing
3. 분석 전 원문을 `text_original`로 보존
4. Unicode 정규화와 제어·zero-width 문자 검사
5. 문자 수, UTF-8 byte 수, URL·반복 비율과 결합문자 run 측정
6. 이메일·전화번호 등 PII 후보와 과대 입력 검사
7. `accept`, `flag`, `quarantine`, `reject` 품질 상태 결정
8. `event_id` 기반 중복 제거
9. `processed`, `quarantine`, `quality_rejected`, `contract_rejected` 경로 분기

### 4.3 처리 전·후 건수

| 지표 | 100건 Batch | 1,000건 Batch·Streaming |
|---|---:|---:|
| 입력 | 100 | 1,000 |
| 계약 오류 | 0 | 0 |
| 중복 | 1 | 19 |
| 고유 출력 | 99 | 981 |
| `accept` | 90 | 891 |
| `flag` | 5 | 50 |
| `quarantine` | 3 | 30 |
| `reject` | 1 | 10 |

Streaming 저장 경로로 보면 고유 981건은 `processed` 941건, `quarantine` 30건, `quality_rejected` 10건입니다. `processed`는 `accept` 891건과 `flag` 50건을 포함합니다.

### 4.4 Spark 최종 컬럼

원본 계약 필드에 다음 처리 컬럼이 추가됩니다.

| 구분 | 컬럼 | 의미 |
|---|---|---|
| 계약 | `event_timestamp`, `collected_timestamp` | Spark timestamp로 변환한 시각 |
| 계약 | `contract_valid`, `contract_errors` | 계약 통과 여부와 오류 코드 배열 |
| 텍스트 | `text_original`, `text_clean` | 입력 텍스트와 정규화·제한 적용 텍스트 |
| 품질 | `quality_policy_version` | 품질 규칙 버전 |
| 품질 | `quality_status`, `quality_flags`, `exclusion_reason` | 최종 판정, 관측 flag와 제외 사유 |
| 길이 | `character_count`, `utf8_byte_count` | 문자·byte 길이 |
| Unicode | `control_character_count`, `zero_width_count`, `max_combining_mark_run` | 비정상 Unicode 측정값 |
| 내용 | `url_count`, `url_ratio`, `repetition_ratio` | URL·반복 측정값 |
| 처리 | `was_normalized`, `was_truncated` | 정규화·절단 적용 여부 |
| 경로 | `output_route` | 네 저장 경로 중 하나 |
| Kafka | `kafka_key`, `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` | 입력 메시지의 추적 위치 |

Batch 파일 입력에는 Kafka metadata가 없으며 Streaming 입력에서만 Kafka 컬럼이 채워집니다.

### 4.5 Batch 실행과 파일 저장

```bash
docker compose --profile tools run --rm spark-runner \
  spark_jobs/process_sample.py \
  --input data/spark-input/synthetic-1000.jsonl \
  --output data/spark-output/week4-1000 \
  --report data/spark-reports/week4-1000.json \
  --log data/spark-logs/week4-1000.jsonl \
  --master spark://spark-master:7077 \
  --format parquet
```

기본 파일 형식은 `source_type`으로 partition한 Parquet입니다. Windows 로컬 검증에서는 `--format jsonl`도 사용할 수 있습니다.

### 4.6 Kafka Streaming과 PostgreSQL 저장

```bash
docker compose --profile tools run --rm spark-runner \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  spark_jobs/streaming_consumer.py \
  --bootstrap-servers kafka:29092 \
  --input-topic raw-text \
  --dlq-topic raw-text-dlq \
  --output data/stream-output/text-events \
  --checkpoint data/stream-checkpoints/text-events \
  --log data/spark-logs/stream-consumer.jsonl \
  --master spark://spark-master:7077 \
  --available-now \
  --format parquet \
  --no-resolve-kafka-package
```

Compose의 `spark-runner`에는 PostgreSQL DSN이 주입되므로 파일과 DB sink가 함께 동작합니다.

```text
data/stream-output/text-events/
├── processed/          # source_type partitioned Parquet
├── quarantine/         # Parquet
├── quality_rejected/   # Parquet
└── contract_rejected/  # Parquet
```

PostgreSQL 저장 결과는 다음과 같습니다.

| 테이블 | 주요 저장 컬럼 | 검증 행 수 |
|---|---|---:|
| `raw_text_events` | 공통 이벤트, 원문 JSON, Kafka topic·partition·offset | 981 |
| `text_documents_clean` | `text_clean`, 품질·길이·Unicode·경로 컬럼 | 981 |
| `contract_rejected_events` | 원본 Kafka 위치, 오류 코드와 거부 payload | 1 |
| `stream_batch_commits` | `consumer_name`, `batch_id`, 입력·경로별 건수 | 2 commits |

`event_id` upsert와 `(consumer_name, batch_id)` commit으로 재처리 중복을 방지합니다. 같은 checkpoint 재실행에서는 새 micro-batch가 0건이었고, 새 checkpoint로 같은 입력을 읽은 경우에도 기존 commit을 확인해 DB 행 수가 변하지 않았습니다.

## 5. 최종 저장 위치와 형식

| 저장 대상 | 위치 | 형식 | Git 포함 |
|---|---|---|:---:|
| Collector·replay 입력 | `data/raw/`, `data/spark-input/` | JSONL | X |
| Spark Batch 결과 | `data/spark-output/` | Parquet 기본, JSONL 선택 | X |
| Streaming 결과 | `data/stream-output/` | 경로·출처 partitioned Parquet | X |
| Streaming 상태 | `data/stream-checkpoints/` | Spark checkpoint | X |
| 최종 조회·재처리 상태 | PostgreSQL | 관계형 table·JSONB·array | X |
| 공개 검증 결과 | `analysis/reports/` | Markdown·JSON·JSONL 집계 | O |

실제 뉴스·댓글 원문과 실행 산출물은 공개 저장소에 포함하지 않습니다.

## 6. 실제 구현과 이후 계획

### 구현 완료

- `TextEvent v1`과 Kafka JSON mapping
- Kafka `raw-text`, `raw-text-dlq` Topic
- Producer의 1,000건 발행과 Spark Consumer의 1,000건 입력 확인
- Spark Batch 100·1,000건 공통 transformation
- Structured Streaming, watermark, checkpoint와 중복 제거
- 네 출력 경로와 partitioned Parquet
- PostgreSQL transaction upsert와 micro-batch 멱등성
- 계약 오류의 파일·Kafka DLQ 저장

### 이후 계획

- LLM Batch 요청·결과와 PostgreSQL 분석 상태 연결
- Airflow DAG와 retry/backoff
- Broker·Worker·DB 중단과 복구 실험
- Consumer lag와 대규모 처리량 측정
- Driver `executemany`를 JDBC staging 또는 bulk load로 전환
- 다중 노드에서 로컬 bind mount 대신 S3·HDFS 호환 공유 저장소 사용

## 7. 관련 문서

- [데이터 계약](data-contract.md)
- [Ingestion 구현 설명](ingestion-implementation.md)
- [Spark Batch 100·1,000건 검증](../analysis/reports/spark-batch-validation.md)
- [Spark Streaming Consumer](spark-streaming-consumer.md)
- [Spark Streaming 통합 검증](../analysis/reports/spark-streaming-consumer-validation.md)
- [Spark Standalone 실행 구조](spark-standalone.md)
- [PostgreSQL 저장 구조](storage-schema.md)
- [PostgreSQL 통합 검증](../analysis/reports/postgres-integration-validation.md)

