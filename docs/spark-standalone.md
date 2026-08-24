# Spark Standalone 실행 구조

## 구성

개발용 Spark 실행 환경은 하나의 `local[2]` 프로세스에서 Master·Worker·Driver 역할을 분리한 Standalone 구조로 전환했습니다.

```text
spark-runner (spark-submit / Driver)
        │
        ▼
spark-master:7077 (자원 관리·Executor 배치)
        │
        ▼
spark-worker (2 cores, 2 GiB)
        └─ Executor (기본 1 GiB, 최대 2 task 동시 실행)

Kafka: kafka:29092
공유 경로: /opt/spark/work-dir → 프로젝트 디렉터리 bind mount
```

`spark-master`는 데이터를 직접 처리하지 않습니다. `spark-runner`에 생성된 Driver가 실행 계획을 만들고, Master가 Worker에 Executor를 배치하며, 실제 Spark task는 Worker의 Executor가 수행합니다.

## 서비스와 UI

| 서비스 | 역할 | 주소 |
|---|---|---|
| `spark-master` | Worker 등록과 자원 스케줄링 | `spark://spark-master:7077` |
| `spark-master` UI | Worker·애플리케이션 상태 확인 | `http://localhost:8080` |
| `spark-worker` | Executor와 task 실행 | 내부 동적 RPC 포트 |
| `spark-worker` UI | Executor·자원 상태 확인 | `http://localhost:8081` |
| `spark-runner` | `spark-submit`과 Driver 실행 | Job 실행 중에만 존재 |

Master와 Worker는 장기 실행 서비스이고 `spark-runner`는 `docker compose run --rm`으로 Job마다 생성·제거합니다. Master·Worker healthcheck가 통과해야 제출 컨테이너가 시작됩니다.

## 시작과 종료

```bash
docker compose up -d kafka spark-master spark-worker
docker compose ps

# 서비스만 중지하고 데이터 volume은 보존
docker compose stop spark-worker spark-master
```

Kafka 데이터와 Maven connector cache를 지우지 않으려면 `docker compose down -v`를 사용하지 않습니다.

## Batch 제출

```bash
docker compose --profile tools run --rm spark-runner \
  spark_jobs/process_sample.py \
  --input data/spark-input/synthetic-100.jsonl \
  --output data/spark-output/standalone-100 \
  --report data/spark-reports/standalone-100.json \
  --log data/spark-logs/standalone-100.jsonl \
  --master spark://spark-master:7077 \
  --format parquet
```

## Kafka Streaming 제출

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

Compose의 `spark-runner`에는 `SPARK_MASTER_URL=spark://spark-master:7077`이 설정되어 있어 애플리케이션의 `--master`를 생략해도 Standalone을 사용합니다. 호스트에서 Python으로 직접 실행할 때는 환경 변수가 없으므로 기존 개발 기본값 `local[2]`를 유지합니다.

## 저장 경로 제약

현재는 한 컴퓨터의 Docker Compose 구성이므로 Driver와 Worker가 같은 프로젝트 bind mount를 `/opt/spark/work-dir`에서 공유합니다. 따라서 파일 sink와 checkpoint가 모든 실행 역할에서 같은 절대 경로로 보입니다.

여러 물리 노드로 Worker를 확장할 때는 이 bind mount를 사용할 수 없습니다. 그 단계에서는 S3, HDFS 또는 호환 object storage처럼 모든 노드가 접근할 수 있는 저장소로 output과 checkpoint를 옮겨야 합니다. Python 프로젝트 코드도 wheel 또는 `--py-files` 배포 방식으로 전환해야 합니다.

## 검증 결과

2026-08-24 실제 Compose 실행에서 다음을 확인했습니다.

- Worker 1개가 Master에 `2 cores, 2.0 GiB`로 등록
- batch 애플리케이션에 Worker Executor 1개·2 cores 할당
- 합성 100건 처리: 고유 99건, 중복 1건
- Kafka streaming 애플리케이션에 별도 Worker Executor 할당
- Kafka 고유 982건 처리: `processed` 941, `quarantine` 30, `quality_rejected` 10, `contract_rejected` 1
- 동일 checkpoint 재제출 시 처리할 새 micro-batch 0건
- partitioned Parquet 출력과 checkpoint 생성 확인

현재 구성은 Master 한 대와 Worker 한 대이므로 Master 자체의 고가용성은 제공하지 않습니다.
