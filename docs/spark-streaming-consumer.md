# Spark Kafka Structured Streaming Consumer

## 목적

`raw-text` Kafka 토픽의 `TextEvent v1` 메시지를 명시적 Schema로 읽고, batch 처리와 같은 계약·텍스트 품질 규칙을 적용한 뒤 처리 경로별로 저장합니다. offset과 중복 상태는 checkpoint로 복구합니다.

구현 진입점은 [`../spark_jobs/streaming_consumer.py`](../spark_jobs/streaming_consumer.py)입니다.

## 처리 흐름

```text
Kafka raw-text
→ value UTF-8 JSON parsing + Kafka topic/partition/offset 보존
→ TextEvent v1 계약 검사
→ 공통 텍스트 품질 transformation
→ event-time watermark
→ event_id 중복 제거
→ foreachBatch 경로 분리
   ├─ processed: accept, flag
   ├─ quarantine: 개인정보·과대 입력 등 격리 대상
   ├─ quality_rejected: tombstone·정규화 후 빈 텍스트
   └─ contract_rejected: 파싱·계약 오류 보관 + raw-text-dlq 발행
```

계약 위반 레코드의 중복 키는 `topic:partition:offset`, 정상 레코드는 `event_id`입니다. watermark 기준 시각은 정상 레코드의 `event_time`, 계약 위반 레코드의 Kafka timestamp입니다. 이 구성으로 정상 이벤트의 재발행 중복을 제한된 state 안에서 제거하면서 서로 다른 잘못된 Kafka 레코드는 유실하지 않습니다.

## 저장과 복구

각 micro-batch는 다음 경로에 `batch_id`별로 기록됩니다.

```text
data/stream-output/
├── processed/batch_id=00000000000000000000/
├── quarantine/batch_id=00000000000000000000/
├── quality_rejected/batch_id=00000000000000000000/
└── contract_rejected/batch_id=00000000000000000000/
```

같은 `batch_id` 경로는 `overwrite`하므로 checkpoint가 같은 micro-batch를 재시도해도 파일 출력이 중복되지 않습니다. 기본 출력은 `source_type`으로 partition한 Parquet입니다. Windows 로컬 검증에서는 native Hadoop helper 없이 실행할 수 있도록 driver-streamed JSONL을 선택할 수 있지만 운영 sink로는 Parquet을 사용합니다.

`raw-text-dlq` Kafka sink는 at-least-once입니다. DLQ 메시지 key가 원본 `topic:partition:offset`으로 안정적이므로 후속 DLQ 저장 단계에서 이 key를 기준으로 멱등 처리해야 합니다. 여러 출력 경로와 Kafka DLQ 사이에 원자적 transaction은 제공하지 않습니다.

checkpoint에는 Kafka offset, watermark와 중복 제거 state가 포함됩니다. 입력 토픽, checkpoint 위치 또는 stateful 연산을 변경할 때 기존 checkpoint를 임의로 재사용하지 않습니다.

## Docker Compose 실행

Kafka는 호스트용 `localhost:9092`와 Compose 내부용 `kafka:29092` listener를 각각 제공합니다. Spark는 `spark-master`, `spark-worker`, 일회성 제출·Driver용 `spark-runner`로 구성됩니다. 자세한 역할과 운영 명령은 [Spark Standalone 실행 구조](spark-standalone.md)를 참고합니다.

```bash
docker compose up -d kafka spark-master spark-worker
python -m jobs.init_kafka
python -m jobs.replay_to_kafka \
  --input data/spark-input/synthetic-1000.jsonl \
  --bootstrap-servers localhost:9092 \
  --topic raw-text \
  --speed 0

docker compose --profile tools run --rm spark-runner \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  spark_jobs/streaming_consumer.py \
  --bootstrap-servers kafka:29092 \
  --input-topic raw-text \
  --dlq-topic raw-text-dlq \
  --output data/stream-output/text-events \
  --checkpoint data/stream-checkpoints/text-events \
  --log data/spark-logs/stream-consumer.jsonl \
  --starting-offsets earliest \
  --master spark://spark-master:7077 \
  --available-now \
  --format parquet \
  --no-resolve-kafka-package
```

`--available-now`는 현재 Kafka backlog를 모두 처리한 후 종료하므로 검증과 backfill에 사용합니다. 지속 실행에서는 이 옵션을 빼고 `--trigger-interval "10 seconds"`를 사용합니다.

## 주요 설정

| 옵션 | 기본값 | 의미 |
|---|---|---|
| `--starting-offsets` | `earliest` | 새 checkpoint의 최초 offset; 기존 checkpoint가 있으면 checkpoint 우선 |
| `--watermark-delay` | `10 minutes` | 늦은 이벤트 허용 범위와 중복 state 보존 기준 |
| `--max-offsets-per-trigger` | `10000` | 한 micro-batch의 최대 Kafka 레코드 수 |
| `--format` | `parquet` | `parquet` 또는 Windows 로컬 검증용 `jsonl` |
| `--no-publish-dlq` | 꺼짐 | 켜면 계약 오류를 파일에만 보관하고 Kafka DLQ 발행 생략 |

## 검증

```bash
python -m pytest -q tests/test_spark_streaming_consumer.py
```

자동 테스트는 설정 오류, Kafka metadata 보존, 계약·품질별 4개 경로 분기, micro-batch별 파일 기록과 watermark 기반 streaming deduplication plan을 검사합니다.

Docker Compose 실제 통합 검증에서는 Kafka 합성 1,000건 중 중복 19건을 제거하고 `processed` 941건, `quarantine` 30건, `quality_rejected` 10건을 기록했습니다. 같은 checkpoint 재시작 시 재처리는 0건이었고, malformed JSON 1건은 `contract_rejected`와 `raw-text-dlq`에 함께 기록됐습니다. 자세한 근거는 [통합 검증 보고서](../analysis/reports/spark-streaming-consumer-validation.md)에 있습니다.

## 운영 한계

- watermark보다 늦은 이벤트는 stateful 중복 제거에서 제외될 수 있으므로 실제 지연 분포로 지연 허용값을 결정해야 합니다.
- 현재 품질 판정은 fixture 일치를 위해 scalar Python UDF를 사용합니다. 처리량 검증 후 native Spark 식 또는 Pandas UDF 전환을 검토합니다.
- 파일 sink와 Kafka DLQ는 하나의 원자적 commit이 아닙니다. PostgreSQL 적재를 추가할 때 `event_id`와 `batch_id` 기반 upsert를 사용합니다.
- 운영 checkpoint는 로컬 임시 디스크가 아닌 장애 후에도 유지되는 공유 저장소에 둡니다.
