# Spark PostgreSQL 멱등 적재 검증

## 범위

2026-08-24에 Kafka, Spark Standalone과 PostgreSQL 16을 연결해 Structured Streaming micro-batch의 실제 적재와 재처리 멱등성을 검증했습니다.

## 최초 적재

Kafka 토픽에는 합성 1,000건과 malformed JSON 1건이 있었으며 Spark watermark 중복 제거 후 982건이 micro-batch 0에 전달됐습니다.

| 대상 | 행 수 |
|---|---:|
| `raw_text_events` | 981 |
| `text_documents_clean` | 981 |
| `contract_rejected_events` | 1 |
| micro-batch 0 입력 | 982 |

`stream_batch_commits`에는 `postgres-integration / batch 0 / input 982`와 네 출력 경로의 행 수가 한 트랜잭션으로 기록됐습니다. available-now가 만든 빈 batch 1도 입력 0건으로 기록했습니다.

## 실패와 복구

첫 시도에서는 품질 fixture에 포함된 NUL 문자를 PostgreSQL `text`가 거부했습니다. DB 트랜잭션과 Spark micro-batch가 모두 commit되지 않아 동일 checkpoint로 안전하게 재시도할 수 있었습니다.

저장 경계에서는 NUL만 Unicode replacement character로 바꾸고, 품질 flag와 나머지 텍스트는 유지하도록 수정했습니다. 원본 JSON 문자열은 JSONB 대신 `text`로 저장해 JSON의 escape 표현을 보존합니다.

## 멱등성

checkpoint를 새로 만들어 동일 Kafka 입력을 batch 0·1로 다시 처리했습니다. PostgreSQL sink는 `(consumer_name, batch_id)` commit을 먼저 확인해 두 batch를 모두 `already_committed`로 건너뛰었습니다.

재처리 후 행 수는 다음과 같이 변하지 않았습니다.

```text
raw=981, clean=981, rejected=1, commits=2
```

동일 consumer의 동시 적재는 PostgreSQL transaction advisory lock으로 직렬화됩니다. `event_id`와 Kafka `topic/partition/offset` unique constraint도 중복을 추가로 방어합니다.

## 확장 한계

현재 sink는 `toLocalIterator()`로 Worker 결과를 Driver에 스트리밍하고 500행 단위 `executemany`를 수행합니다. 1,000건 규모에서는 메모리 전체 수집 없이 동작하지만 대규모 처리에서는 Spark JDBC staging 또는 object storage→bulk load 방식으로 교체해야 합니다.
