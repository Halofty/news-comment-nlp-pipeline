# MinIO Streaming checkpoint 재시작 검증

## 목적

Spark Structured Streaming의 Kafka offset과 state를 로컬 컨테이너 파일이 아니라
MinIO `news-checkpoints` bucket에 저장하고, Spark 실행 및 MinIO 컨테이너를 다시
시작해도 이미 처리한 이벤트가 중복 저장되지 않는지 확인했다.

검증은 운영 데이터와 분리한 Kafka topic과 object prefix에서 2026-09-03에 수행했다.

```text
Kafka: minio-checkpoint-recovery-20260903-v1
output: s3a://news-processed/streaming/minio-checkpoint-recovery-20260903-v1
checkpoint: s3a://news-checkpoints/spark/minio-checkpoint-recovery-20260903-v1
```

## 실험 순서

1. 합성 이벤트 100건을 Kafka에 게시했다. 생성 데이터에는 같은 `event_id` 1건이
   포함되어 있어 Spark의 유효 입력은 99건이다.
2. `availableNow`로 최초 실행하고 Spark 프로세스를 종료했다.
3. 새 입력 없이 같은 output과 checkpoint로 다시 실행했다.
4. 서로 다른 `event_id`의 추가 이벤트 50건을 게시했다.
5. 같은 checkpoint로 세 번째 실행했다.
6. MinIO의 네 route Parquet와 checkpoint를 Spark로 다시 읽어 행 수와 고유 ID를
   대조했다.
7. MinIO 컨테이너를 재시작한 뒤 report, output과 checkpoint 객체를 다시 조회했다.

## 실행 결과

세 실행 모두 checkpoint에서 같은 query ID
`8d7575b8-e7af-4d0c-b80d-b5cbacb46023`를 복원했다.

| 실행 | 입력 상태 | 처리 행 | micro-batch ID | 완료 시간 |
|---|---|---:|---|---:|
| 1차 | 100건 게시, 중복 1건 포함 | 99 | 0, 1(빈 batch) | 10.923초 |
| 2차 | 새 입력 없음 | 0 | 없음 | 4.050초 |
| 3차 | 고유 이벤트 50건 추가 | 50 | 2, 3(빈 batch) | 13.128초 |

최종 MinIO 출력은 다음과 같다.

| route | 저장 행 |
|---|---:|
| `processed` | 141 |
| `quarantine` | 6 |
| `quality_rejected` | 2 |
| `contract_rejected` | 0 |
| 합계 | **149** |

| 무결성 지표 | 결과 |
|---|---:|
| 예상 고유 행 | 149 |
| 최종 저장 행 | 149 |
| 고유 `event_id` | 149 |
| 누락 | 0 |
| 중복 저장 | 0 |
| checkpoint | 43 objects, 16,555 bytes |
| output prefix | 44 objects, 약 520 KiB |

검증기는 실행별 로그의 합계 `99, 0, 50`, 네 route의 총 행 수, 고유 event ID와
checkpoint의 비어 있지 않음을 한 번에 검사했다. 결과 JSON은
`data/reports/minio-checkpoint-recovery.json`이며 실행 로그는
`data/logs/minio-checkpoint-recovery-run-{1,2,3}.jsonl`이다. `data/`는 Git 제외
대상이므로 이 문서에는 공개 가능한 집계만 기록한다.

## MinIO 컨테이너 재시작

`docker compose restart minio` 후 health check와 bucket 초기화를 다시 실행했다.
재시작 전후에 다음 값이 같았다.

- report: ETag `fec1c50d9a92c966e503e8a000463714`, SHA-256
  `6b782dc4151634b54c7e03b0a39b2c2c7dfa061a89bdc31976512809b2c5650e`
- checkpoint: 43 objects, 약 16 KiB
- output: 44 objects, 약 520 KiB

따라서 컨테이너 프로세스가 재시작돼도 named volume `minio-data`에 저장된 객체는
유지됐다. 단, volume 삭제나 디스크 손실까지 방어하는 백업 실험은 아니다.

## 재현 명령의 핵심

```bash
docker compose --profile tools run --rm --no-deps spark-runner \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  spark_jobs/streaming_consumer.py \
  --bootstrap-servers kafka:29092 \
  --input-topic minio-checkpoint-recovery-20260903-v1 \
  --output s3a://news-processed/streaming/minio-checkpoint-recovery-20260903-v1 \
  --checkpoint s3a://news-checkpoints/spark/minio-checkpoint-recovery-20260903-v1 \
  --available-now --format parquet --no-resolve-kafka-package --no-publish-dlq
```

각 실행은 `--log`만 다른 경로로 지정한다. 검증은
`spark_jobs/verify_minio_checkpoint_recovery.py`로 수행한다.

## 결론과 한계

공유 MinIO checkpoint가 Kafka offset과 batch 순서를 복구했고 무입력 재시작은 0건,
추가 입력 재시작은 새 50건만 처리했다. Spark 프로세스와 MinIO 컨테이너 재시작 후
누락·중복은 모두 0건이다.

이번 실험은 단일 노드 MinIO와 Kafka broker가 살아 있는 조건이다. 이후에는 Kafka
broker 중단, worker 강제 종료, MinIO 네트워크 지연과 volume 백업·복원까지 별도
장애 시나리오로 검증할 수 있다.
