# Spark Streaming Consumer 통합 검증

## 검증 범위

2026-08-23에 Docker Compose의 Kafka 4.3.1과 Spark 3.5.7 환경에서 `raw-text` → Spark Structured Streaming → 경로별 파일 및 `raw-text-dlq` 흐름을 실제로 실행했습니다. 입력은 공개 합성 1,000건이며 출력 파일과 checkpoint는 원문 데이터이므로 `data/` 아래에만 보관합니다.

## 실행 결과

| 항목 | 결과 |
|---|---:|
| Kafka 입력 | 1,000 |
| `event_id` 중복 제거 | 19 |
| 처리된 고유 이벤트 | 981 |
| `processed` | 941 |
| `quarantine` | 30 |
| `quality_rejected` | 10 |
| `contract_rejected` | 0 |

`1,000 = 19 + 941 + 30 + 10`으로 입력 행 회계가 일치합니다. 첫 유효 micro-batch의 처리 시간은 12.549초였고, consumer 시작부터 available-now 종료까지는 25.120초였습니다. 이는 로컬 2-core 검증값이며 운영 처리량 기준으로 일반화하지 않습니다.

## checkpoint 재시작

동일한 output과 checkpoint로 다시 실행했을 때 새 micro-batch가 생성되지 않았고 기존 출력은 981건으로 유지됐습니다. query ID도 최초 실행과 동일하게 복구되어 Kafka offset과 state가 checkpoint에서 이어졌음을 확인했습니다.

## 계약 오류와 DLQ

`not-json` 메시지 1건을 추가 발행한 뒤 같은 checkpoint로 재실행했습니다.

- micro-batch 2 입력 1건, `contract_rejected` 1건
- 파일 출력에 원본 Kafka topic·partition·offset과 계약 오류 코드 보존
- `raw-text-dlq`에 1건 발행
- 파일과 DLQ 모두 원본 위치 `raw-text:0:312` 일치
- DLQ value에 `reason`, `contract_errors`, `raw_event`, 원본 위치와 `rejected_at` 포함

파일 sink는 batch ID 경로 overwrite로 재시도 멱등성을 확보합니다. Kafka DLQ sink는 at-least-once이므로 후속 저장 단계가 `topic:partition:offset` key로 멱등 처리해야 합니다.

기본 운영 포맷인 Parquet도 별도 checkpoint로 전체 토픽을 다시 읽어 검증했습니다. 합성 입력과 malformed JSON을 합친 고유 982건이 같은 경로 분포로 처리됐고, `processed` 출력은 `source_type=news`와 `source_type=comment` partition으로 생성됐습니다.

## Standalone 재검증

2026-08-24에는 `local[2]` 대신 별도 Spark Master와 Worker를 기동하고 같은 Kafka 토픽을 다시 처리했습니다. Master는 Worker를 2 cores·2.0 GiB로 등록했고, streaming 애플리케이션 `app-20260824065102-0001`에 Worker Executor 1개와 2 cores를 할당했습니다.

- Spark master: `spark://spark-master:7077`
- Driver: 일회성 `spark-runner` 컨테이너
- Executor: 별도 `spark-worker` 컨테이너
- 처리 결과: 고유 982건, `941 + 30 + 10 + 1`
- partitioned Parquet와 checkpoint 생성
- 같은 checkpoint 재제출: 새 micro-batch 0건

Standalone 실행의 상세 구조와 재현 명령은 [`docs/guides/spark-standalone.md`](../../docs/guides/spark-standalone.md)에 기록했습니다.

## 근거 로그

원문을 포함하지 않는 구조화 실행 로그는 다음 파일에 보관합니다.

- `spark-streaming-consumer-run-log.jsonl`: 1,000건 최초 실행
- `spark-streaming-consumer-restart-log.jsonl`: 같은 checkpoint 재시작
- `spark-streaming-consumer-dlq-log.jsonl`: 잘못된 JSON 1건 DLQ 검증

운영 출력과 checkpoint는 `.gitignore` 대상인 `data/stream-output/`과 `data/stream-checkpoints/`에 있습니다.
