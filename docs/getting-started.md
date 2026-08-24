# 로컬 개발과 실행 가이드

## 1. 사전 조건

- Python 3.10 이상
- Java 11 이상
- Docker Desktop과 Docker Compose
- Docker에 할당할 수 있는 최소 4 GiB 메모리

Spark를 호스트에서 직접 실행할 수 있지만 Kafka connector와 Windows Hadoop 의존성 차이를 줄이기 위해 통합 실행은 Docker Compose를 기준으로 합니다.

## 2. Python 환경

```bash
python -m venv .venv
```

Linux·macOS:

```bash
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

의존성을 설치합니다.

```bash
python -m pip install -r requirements.txt
```

## 3. 자동 테스트

```bash
python -m pytest -q
```

현재 테스트 범위는 이벤트 계약, Collector, Kafka Producer, 데이터셋 메타데이터, 텍스트 품질, Spark batch·streaming 변환과 PostgreSQL 멱등 적재입니다.

## 4. 로컬 서비스 시작

Spark 제출 이미지에는 PostgreSQL client가 포함되므로 최초 실행 또는 Dockerfile 변경 후 이미지를 빌드합니다.

```bash
docker compose build spark-runner
docker compose up -d --wait postgres kafka spark-master spark-worker
docker compose ps
```

서비스 주소:

| 서비스 | 주소 |
|---|---|
| Kafka 호스트 listener | `localhost:9092` |
| PostgreSQL | `localhost:5432` |
| Spark Master UI | `http://localhost:8080` |
| Spark Worker UI | `http://localhost:8081` |

Compose 내부에서는 Kafka `kafka:29092`, PostgreSQL `postgres:5432`, Spark Master `spark://spark-master:7077`을 사용합니다.

## 5. Kafka 토픽과 표본 적재

```bash
python -m jobs.init_kafka \
  --bootstrap-servers localhost:9092

python -m jobs.replay_to_kafka \
  --input sample/synthetic-events.jsonl \
  --bootstrap-servers localhost:9092 \
  --topic raw-text \
  --speed 0

python -m jobs.inspect_kafka \
  --bootstrap-servers localhost:9092 \
  --topic raw-text \
  --from-beginning \
  --group-id ingestion-check-1 \
  --limit 10
```

1,000건 검증 입력은 `jobs.generate_synthetic_events`로 `data/` 아래에 생성합니다. 실제 원문과 생성된 실행 데이터는 Git에 포함하지 않습니다.

## 6. Collector 실행

```bash
python -m collectors.gdelt \
  --query "climate change" \
  --max-records 100 \
  --output data/raw/gdelt.jsonl

python -m collectors.reddit \
  --month 2016-01 \
  --subreddit worldnews \
  --limit 100 \
  --output data/raw/reddit.jsonl
```

외부 API·공개 데이터셋의 rate limit과 이용 조건을 먼저 확인합니다. Collector별 옵션과 변환 흐름은 [Ingestion 구현 설명](ingestion-implementation.md)에 있습니다.

## 7. Spark Batch 제출

```bash
docker compose --profile tools run --rm spark-runner \
  spark_jobs/process_sample.py \
  --input data/spark-input/synthetic-1000.jsonl \
  --output data/spark-output/sample-1000 \
  --report data/spark-reports/sample-1000.json \
  --log data/spark-logs/sample-1000.jsonl \
  --master spark://spark-master:7077 \
  --format parquet
```

## 8. Kafka Streaming 제출

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

Compose의 `POSTGRES_DSN`으로 PostgreSQL sink가 자동 활성화됩니다. 지속 실행에서는 `--available-now`를 제거하고 `--trigger-interval`을 지정합니다.

## 9. 종료와 초기화

데이터 volume을 보존하며 서비스를 멈춥니다.

```bash
docker compose stop spark-worker spark-master kafka postgres
```

`docker compose down -v`는 Kafka와 PostgreSQL volume을 삭제하므로 테스트 데이터를 완전히 초기화할 때만 사용합니다.

세부 실행법은 [Spark Standalone](spark-standalone.md), [Streaming Consumer](spark-streaming-consumer.md), [PostgreSQL 저장 구조](storage-schema.md)를 참고합니다.
