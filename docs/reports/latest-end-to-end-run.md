# 최신 end-to-end 실행 기록

> 아래 30일 Run은 Kafka bounded batch가 최종 DAG에 들어간 뒤 처음으로 `submit=true`
> 전체 파이프라인을 실행한 기록이다. 이전 92일 Run은 Kafka를 DAG에 편입하기 전
> 성능·LLM 결과이며 **92일 pre-Kafka baseline** 절에 그대로 보존한다.

## 실행 식별자 (Kafka 포함, 30일)

| 항목 | 값 |
|---|---|
| DAG | `news_comment_end_to_end_pipeline` |
| Run ID | `manual__2026-09-07T04:20:54.825421+00:00` |
| 데이터 범위 | 2012-11-01~2012-11-30, 양 끝 포함 30일 |
| 대주제 | `economy` |
| Reddit 제한 | 없음 (`limit=0`) |
| OpenAI 제출 | 실제 제출 (`submit=true`) |
| prompt | `group-daily-v3-emotional-tones-compact-source-balanced` |

이 Run은 날짜별 mapped task 30개를 만들었다. 최초 시도에서 뒤쪽 16개 날짜의
`run_kafka_spark_batch`가 실패했다(원인·복구는 아래 **Kafka 데이터 손실 장애와 복구**
절 참고). 복구 후 최종 상태는 `success`이며 14개 task 종류의 모든 instance(mapped
task 포함 총 188개)가 성공했다.

## Kafka 발행·필터링 상세

여러 날짜 task가 같은 `raw-text` topic을 공유하므로, Spark는 ledger에 기록된
partition별 시작·종료 offset 구간을 읽은 뒤 그 안에서 `pipeline_run_id`와
`analysis_date`가 같은 이벤트만 다시 선택한다.

| 지표 | 결과 |
|---|---:|
| Kafka 발행 / Spark 매칭(`matched_run_rows`) | 84,569 / 84,569건 |
| offset 구간 내 다른 실행분(`ignored_foreign_rows`) | 1,829,429건 |
| offset 구간 총합(`offset_range_rows`) | 1,913,998건 |
| Spark 입력 / 고유 저장 | 84,569 / 84,569건 |
| 계약 거부 / 중복 / DLQ | 0 / 0 / 0건 |

`ignored_foreign_rows`가 매칭 건수의 20배를 넘는 것은 같은 topic을 반복 사용해온
과거 테스트·smoke run들의 메시지가 offset 구간 안에 함께 존재했기 때문이다.
`pipeline_run_id`·날짜 필터링이 이 노이즈 속에서도 정확히 84,569건만 골라냈다는
뜻이며, 발행·선택·Spark 입력·행 회계가 모두 같지 않으면
`verify_kafka_spark_accounting`이 다음 단계 진행을 막는다.

## 단계별 결과

| 단계 | task instance | 처리·저장 결과 |
|---|---:|---:|
| 날짜 설정 | 1 | 30개 날짜 설정 생성 |
| 원본 수집·병합 | 30 | 뉴스 2,624 + Reddit(댓글) 81,945건 |
| Kafka 발행 | 30 | 84,569건 |
| Spark 처리 | 30 | 입력 84,569건 (품질 accept 83,784 / quarantine 708) |
| Spark 고유 저장 | 30 | 84,569건 |
| 계약 거부 / 중복 | 30 | 0 / 0건 |
| MinIO 게시 | 30 | 300개 객체, 62,383,890 bytes |
| LLM 요청 준비 | 30 | 날짜별 economy 요청 1개, `budget_status=ok` 30/30 |
| OpenAI Batch | 30 | 완료 30, 실패 0 |
| PostgreSQL 분석 저장 | 30 | 이 Run 30건 |
| serving snapshot 읽기 | 30 | ready 30개 |

Spark application별 실행 시간의 합은 150.4초다. mapped task가 일부 병렬로
실행되므로 이 값은 DAG wall-clock 시간과 같지 않다.

## LLM 사용량

| 지표 | 값 |
|---|---:|
| 입력 token | 5,430,971 |
| 출력 token | 17,014 |
| 기록된 비용 | $0.5533055 |
| PostgreSQL 전체 누적 분석 | 217건 |

각 Batch는 Reddit과 web news를 먼저 출처별로 해석하고, 두 출처가 모두 있으면
출처 수준 결론에 각각 50% 가중치를 적용한다. 행 수나 token 수가 많은 Reddit이
뉴스보다 더 큰 출처 비중을 갖지 않도록 한 설정이다.

## Kafka 데이터 손실 장애와 복구

### 증상

최초 시도(`manual__2026-09-07T04:20:54.825421+00:00`, 04:20:54 UTC 시작)에서 뒤쪽
16개 날짜(map_index 14~29, 2012-11-15~2012-11-30)의 `run_kafka_spark_batch`가
재시도까지 실패했다. 로그에는 다음과 같은 오류가 남았다.

```
java.lang.IllegalStateException: Cannot fetch offset 53 (... TopicPartition: raw-text-0).
Caused by: org.apache.kafka.clients.consumer.OffsetOutOfRangeException: Fetch position
FetchPosition{offset=53, ...} is out of range for partition raw-text-0
```

첫 실패는 04:26:42 UTC, 시작으로부터 약 6분 뒤였다.

### 원인

`raw-text` topic은 `retention.ms=604800000`(7일)이고 `message.timestamp.type=CreateTime`
이라 브로커는 메시지의 CreateTime을 기준으로 보존 기간을 판정한다. 그런데
`producers/kafka.py`의 `KafkaEventProducer.send()`가 `produce()` 호출 시
`timestamp=event["event_time"]`(기사·댓글의 원본 사건 시각, 이 데이터셋은 2012년)을
그대로 넘기고 있었다. 이 파이프라인은 2012년 아카이브를 replay하므로, 방금 발행한
메시지의 CreateTime이 "지금(2026) - 7일"보다 훨씬 과거로 찍혔다. Kafka의 log
retention 체크 주기(기본 5분)마다 이를 이미 만료된 세그먼트로 판단해 즉시 삭제했고,
그 결과 이후 시도한 Spark bounded read가 이미 사라진 offset을 가리키게 됐다.

실제로 topic을 조회한 결과 `earliest == latest == 28341`(partition 0 기준)로, 앞서
캡처해둔 시작 offset(49, 53, 57 등)을 포함한 메시지가 전부 삭제된 상태였다. 즉
"재시도 대기 후 실패"는 재시도 자체의 문제가 아니라, 첫 시도와 재시도 사이에 이미
Kafka가 해당 offset 구간을 지워버려 어떤 재시도로도 복구할 수 없는 상태였다.

### 수정

[producers/kafka.py](../../producers/kafka.py)에서 `produce()` 호출 시 `timestamp=`
인자를 제거했다. confluent-kafka 클라이언트는 `timestamp`를 지정하지 않으면 실제
발행(ingestion) 시각을 CreateTime으로 사용하므로, retention이 사건 시각이 아닌 실제
적재 시각 기준으로 평가된다. `event_time`은 페이로드 JSON 안에는 그대로 남아 있어
다운스트림의 `event_timestamp` 우선, 없으면 `kafka_timestamp`로 대체하는 로직
([spark_jobs/streaming_consumer.py](../../spark_jobs/streaming_consumer.py))에는
영향이 없다. `tests/test_kafka_producer.py`의 관련 assertion도 이 동작에 맞춰 갱신했다.

### 복구

이미 삭제된 offset을 가리키는 map_index는 `run_kafka_spark_batch`만 재시도해서는
복구되지 않으므로, 실패한 16개 날짜만 `capture_kafka_start_offsets`부터 다시
발행하도록 정확히 clear했다.

1. `airflow tasks states-for-dag-run`으로 실패한 map_index(14~29)를 특정했다.
2. Airflow 3의 `SerializedDAG.clear(task_ids=[(task_id, map_index), ...], run_id=...,
   dry_run=True)`로 영향받는 TaskInstance가 정확히 그 16개 날짜의
   `capture_kafka_start_offsets`~`run_kafka_spark_batch` 5단계(80개)뿐이고 이미 성공한
   0~13번째 날짜는 포함되지 않는지 먼저 확인했다.
3. `dry_run=False`로 실제 clear를 실행해 DagRun을 재개했다. 이 DagRun은 `submit=true`로
   트리거된 상태였으므로, 복구가 끝나면 자동으로 나머지 단계(build_llm, 실제 OpenAI
   Batch 제출)까지 이어진다는 점을 진행 전 확인받았다.
4. 재실행된 16개 날짜 모두 새 CreateTime으로 정상 처리됐고, 전체 30개 날짜가
   `run_kafka_spark_batch`부터 `read_final_result`까지 성공해 DagRun이 `success`로
   종료됐다(06:08:47~06:21:04 UTC, 약 12분).

### 교훈

과거 날짜 데이터를 replay하는 파이프라인에서는 Kafka 레코드의 timestamp를 이벤트
원본 시각으로 설정하면 안 된다. topic의 시간 기반 retention은 항상 실제 ingestion
시각을 기준으로 평가돼야 하며, 사건 시각처럼 별도 의미를 갖는 시각은 페이로드
필드로만 전달해야 한다.

## 최종 결과 확인

- Airflow: 30개 날짜의 14개 task 종류, mapped instance 총 188개가 모두 `success`
- MinIO: 날짜와 Run ID prefix 아래 300개 object와 checksum 기록 확인
- PostgreSQL: 이 Run 분석 30건, 전체 누적 217건 확인
- Langfuse: 날짜별 generation token·비용 trace 확인
- Slack: 날짜별 완료 알림 확인
- Streamlit: v3 결과, 감정 분포, polarization, positive/negative tones, topic 조회

실행 절차는 [현재 end-to-end 실행 방법](../guides/end-to-end-execution.md)에 있다.

---

## 92일 pre-Kafka baseline (과거 기록)

> Kafka를 최종 DAG에 편입하기 전, 별도 Spark batch 단계로 실행한 성능·LLM 결과다.
> 위 30일 Run으로 대체된 현재 기준 수치이며, 규모 비교를 위해 그대로 보존한다.

### 실행 식별자

| 항목 | 값 |
|---|---|
| DAG | `news_comment_end_to_end_pipeline` |
| Run ID | `manual__2026-09-06T17:41:03.997288+00:00` |
| 데이터 범위 | 2012-08-01~2012-10-31, 양 끝 포함 92일 |
| 대주제 | `economy` |
| Reddit 제한 | 없음 (`limit=0`) |
| OpenAI 제출 | 실제 제출 (`submit=true`) |
| prompt | `group-daily-v3-emotional-tones-compact-source-balanced` |

이 Run은 날짜별 mapped task 92개를 만들었다. 최종 상태는 `success`이며 당시 10개 task
종류의 모든 instance가 성공했다.

### 최초 Kafka 편입 smoke 검증 (151건, dry_run)

2026-09-07에 `kafka-bounded-smoke-20260907` Run을 `submit=false`로 실행해 외부 유료
요청 없이 Kafka 편입 직후의 14단계를 처음 검증했다. Reddit은 `limit=2`, 로컬 Google
News는 제한 없이 병합되어 총 151건이 Kafka에 들어갔다. 이 smoke 검증은 위 30일 실제
Run으로 대체됐다.

| 지표 | 결과 |
|---|---:|
| DAG task | 14/14 success |
| 수집·병합 / Kafka 발행 | 151 / 151건 |
| Kafka offset span / Run 필터 일치 | 151 / 151건 |
| Spark 입력 / 행 회계 / 고유 저장 | 151 / 151 / 151건 |
| 계약 거부 / 중복 / DLQ | 0 / 0 / 0건 |
| source | news 149 / comment 2건 |
| Spark 처리 시간 | 3.943초 |
| MinIO processed | 10객체 / 147,138 bytes |
| LLM preflight | economy 1요청, `budget_status=ok` |
| OpenAI 제출 | `dry_run` |
| serving snapshot | `ready` |

### 단계별 결과

| 단계 | task instance | 처리·저장 결과 |
|---|---:|---:|
| 날짜 설정 | 1 | 92개 날짜 설정 생성 |
| 원본 수집·병합 | 92 | 뉴스 7,343 + Reddit 296,053건 |
| Spark 처리 | 92 | 입력 303,396건 |
| Spark 고유 저장 | 92 | 303,396건 |
| 계약 거부 / 중복 | 92 | 0 / 0건 |
| MinIO 게시 | 92 | 920개 객체, 206,879,418 bytes |
| LLM 요청 준비 | 92 | 날짜별 economy 요청 1개 |
| OpenAI Batch | 92 | 완료 92, 실패 0 |
| PostgreSQL 분석 저장 | 92 | 이 Run 92건 |
| serving snapshot 읽기 | 92 | ready 92개 |

Spark application별 실행 시간의 합은 624.603초다. mapped task가 일부 병렬로
실행되므로 이 값은 DAG wall-clock 시간과 같지 않다.

### LLM 사용량

| 지표 | 값 |
|---|---:|
| 입력 token | 20,265,672 |
| 출력 token | 52,166 |
| 기록된 비용 | $2.545403 |
| PostgreSQL 전체 누적 분석(당시) | 187건 |
| 최신 source-balanced v3 누적(당시) | 93건 |

### 실패와 복구 기록

처음 실행에서 세 날짜의 v3 응답이 tone share 합계 검증을 통과하지 못했다.

| 날짜 | 원인 | 복구 |
|---|---|---|
| 2012-08-30 | negative tone 합계 0.65 | 허용 범위 안의 비정규 합계를 1.0으로 정규화 |
| 2012-08-31 | positive 0.72, negative 0.58 | 동일 Batch 결과 재사용 후 정규화·재검증 |
| 2012-10-11 | positive tone 합계 0.40 | 동일 Batch 결과 재사용 후 정규화·재검증 |

빈 tone 배열이 아닌 경우 합계가 `0 < sum <= 1.25`이면 1.0으로 정규화하고, 0 또는
1.25 초과는 계속 거부한다. 복구 시 완료된 OpenAI Batch ID를 재사용해 유료 요청을
새로 만들지 않았고, 해당 task부터 재실행했다. 복구 후 92개 응답 모두 검증·저장됐으며
요청 실패·누락·중복은 0건이다.

Airflow metadata의 최종 복구 구간은 2026-09-06 23:06:00~23:06:51 UTC로 약 51.4초다.
원래 제출부터 수동 원인 분석과 복구까지의 경과 시간에는 Batch 대기와 사람의 개입
시간이 포함되므로 순수 처리 성능으로 사용하지 않는다.
