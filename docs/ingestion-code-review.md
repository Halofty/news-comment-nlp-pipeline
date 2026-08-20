# Ingestion 코드 리뷰

## 1. 리뷰 목적

현재 구현된 뉴스·댓글 수집 파이프라인이 다음 목표를 만족하는지 검토했습니다.

- 서로 다른 데이터 출처를 동일한 이벤트 계약으로 변환하는가
- 수집과 Kafka 전송의 책임이 적절히 분리되어 있는가
- 잘못된 데이터와 외부 서비스 장애에 안전하게 대응하는가
- 재실행과 확장이 가능한 구조인가
- Kafka에 데이터를 안정적으로 저장하고 전달할 수 있는가

리뷰 대상은 Collector, `TextEvent v1`, JSONL staging, Kafka Producer, replay job, 토픽 초기화, 적재 확인 코드와 개발용 Kafka 실행 환경입니다.

## 2. 현재 Ingestion 구조

```text
GDELT API ──→ GDELT Collector ──┐
                                ├─→ TextEvent v1 검증
Reddit ─────→ Reddit Collector ─┘          │
                                           ↓
                                    JSONL Staging
                                           │
                                           ↓
                                      Replay Job
                                           │
                                           ↓
                                     Kafka Producer
                                           │
                                           ↓
                                     raw-text Topic
                                           │
                                           ↓
                                    Inspector / Spark
```

현재 Collector는 Kafka에 직접 연결되지 않습니다. 먼저 공통 스키마 JSONL로 저장한 뒤 replay job이 Kafka로 전달합니다. 이 방식은 실시간성보다 재현성, 디버깅과 반복 테스트를 우선한 MVP 설계입니다.

## 3. 전체 평가

### 잘된 부분

1. **Collector와 Kafka Producer의 책임이 분리되어 있습니다.**
   외부 API·데이터셋 변경과 Kafka 설정 변경을 독립적으로 다룰 수 있습니다.

2. **모든 출처가 `TextEvent v1`을 사용합니다.**
   Kafka 이후 처리기는 GDELT와 Reddit의 원본 필드 차이를 알 필요가 없습니다.

3. **JSONL을 재현 가능한 staging 영역으로 사용합니다.**
   같은 데이터를 반복 발행하거나 다른 속도로 재생할 수 있습니다.

4. **Kafka 메시지 의미가 명확합니다.**
   `event_id`는 key, 전체 이벤트 JSON은 value, `event_time`은 Kafka timestamp로 전달됩니다.

5. **Producer 기본 안정성 설정이 포함되어 있습니다.**
   `acks=all`, idempotent producer, zstd 압축, delivery callback과 flush 검사가 적용되어 있습니다.

6. **외부 서비스 없이 핵심 로직을 검증할 수 있습니다.**
   Collector 변환, 계약 검증, replay, Producer와 Consumer에 대한 단위 테스트가 구성되어 있습니다.

### 종합 판단

현재 구조는 MVP용 Ingestion 기반으로 적절합니다. 다만 운영 가능한 단계로 보기 위해서는 Kafka 데이터 영속성, 출처별 계약 검증, 행 단위 실패 격리, 실제 DLQ와 Broker 통합 테스트가 필요합니다.

## 4. 주요 리뷰 결과

### 4.1 Kafka 데이터 영속성 확인 필요

**중요도: 높음**

현재 `docker-compose.yml`은 named volume을 `/var/lib/kafka/data`에 연결합니다.

```yaml
volumes:
  - kafka-data:/var/lib/kafka/data
```

반면 [Apache Kafka 공식 단일 노드 Compose 예시](https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml)는 Kafka 로그 경로를 `/tmp/kraft-combined-logs`로 설정합니다. 현재 설정에서는 named volume이 실제 Kafka 로그를 보존하지 않을 가능성이 있습니다.

예상 영향:

- 컨테이너 재생성 후 메시지가 사라질 수 있음
- volume은 존재하지만 실제 Broker 데이터가 들어 있지 않을 수 있음
- 문서에 정의한 데이터 보존 동작과 실행 결과가 달라질 수 있음

개선 방향:

```yaml
environment:
  KAFKA_LOG_DIRS: /tmp/kraft-combined-logs

volumes:
  - kafka-data:/tmp/kraft-combined-logs
```

listener, 내부 토픽 replication factor 등 단일 KRaft 노드에 필요한 설정도 공식 예시를 기준으로 명시할 예정입니다.

### 4.2 공통 계약의 의미적 검증 강화 필요

**중요도: 중간 이상**

현재 `validate_event()`는 `source_type`과 `source_name`을 각각 검사하지만 둘의 조합은 확인하지 않습니다.

```python
if event["source_type"] not in SOURCE_TYPES:
    raise ValueError(...)

if event["source_name"] not in SOURCE_NAMES:
    raise ValueError(...)
```

따라서 다음처럼 구조는 맞지만 의미가 잘못된 이벤트도 통과할 수 있습니다.

```json
{
  "source_type": "news",
  "source_name": "reddit",
  "title": null,
  "url": "not a uri"
}
```

추가해야 할 규칙:

| 출처 | 필수 의미 규칙 |
|---|---|
| GDELT | `source_type=news`, 제목과 URL 존재 |
| Reddit | `source_type=comment`, 제목과 URL은 `null` |
| 뉴스 | `community`, `engagement`는 `null` |
| 공통 | URL이 존재한다면 올바른 URI 형식 |

Python 검증과 JSON Schema가 같은 규칙을 적용하도록 함께 변경해야 합니다.

### 4.3 DLQ는 토픽만 있고 발행 로직은 미구현

**중요도: 중간 이상**

현재 `raw-text-dlq` 토픽을 생성하지만 오류 이벤트를 해당 토픽에 발행하는 코드는 없습니다.

현재 동작:

```text
계약 검증 실패 → 예외 발생 → replay 작업 종료
```

목표 동작:

```text
계약 검증 실패
→ 원본 payload와 오류 원인 생성
→ raw-text-dlq 발행
→ 다음 이벤트 처리 계속
```

DLQ 메시지에는 최소한 다음 정보가 필요합니다.

```json
{
  "failed_stage": "schema_validation",
  "error_type": "ValueError",
  "error_message": "text must be a non-empty string",
  "failed_at": "2026-08-20T09:00:00Z",
  "original_topic": "raw-text",
  "original_payload": {}
}
```

토픽 생성, 오류 발행, 오류 조회와 재처리를 하나의 기능으로 완성해야 합니다.

### 4.4 반복 수집 시 JSONL 덮어쓰기

**중요도: 중간**

JSONL Writer는 임시 파일을 완성한 뒤 기존 출력 파일을 교체합니다.

```python
temporary = output.with_suffix(output.suffix + ".tmp")
...
temporary.replace(output)
```

원자적 쓰기이므로 불완전한 파일을 방지한다는 장점이 있습니다. 하지만 Collector를 같은 기본 경로로 다시 실행하면 이전 결과가 사라집니다.

Airflow 주기 실행 전 다음과 같은 실행별 경로가 필요합니다.

```text
data/raw/gdelt/date=2026-08-20/hour=18/events.jsonl
data/raw/reddit/month=2016-01/run_id=.../events.jsonl
```

단순 append보다 날짜·실행 ID 기반 파티션이 재처리와 중복 관리에 유리합니다.

### 4.5 잘못된 원본 한 건이 전체 수집을 중단

**중요도: 중간**

GDELT는 `seendate`를 즉시 파싱하고 Reddit은 여러 숫자 필드를 즉시 정수로 변환합니다.

```python
event_time = datetime.strptime(
    seen_date, "%Y%m%dT%H%M%SZ"
).isoformat() + "Z"
```

```python
"engagement": int(comment.get("score") or 0)
```

원본 한 건의 날짜 또는 숫자 형식이 잘못되면 전체 generator가 중단됩니다. JSONL Writer는 원자적 저장을 위해 임시 파일을 제거하므로 앞서 변환한 정상 이벤트도 결과에 남지 않습니다.

개선 후에는 정상, 필터링, 오류를 구분해야 합니다.

```text
read=1000
emitted=975
filtered=20
invalid=5
```

오류 행은 별도 파일이나 DLQ staging에 저장하여 원인을 확인할 수 있어야 합니다.

### 4.6 Kafka 오류를 문자열로 판별

**중요도: 중간**

토픽 초기화 코드는 다음과 같이 오류 문자열을 검사합니다.

```python
if "TOPIC_ALREADY_EXISTS" in str(error):
    existing.append(name)
    continue
```

오류 메시지는 클라이언트 버전에 따라 달라질 수 있습니다. `KafkaException`과 `KafkaError.TOPIC_ALREADY_EXISTS` 코드를 비교하는 방식이 더 안전합니다.

기존 토픽에 대해서도 존재 여부만 확인하지 말고 파티션 수와 retention 설정이 기대값과 같은지 검증해야 합니다.

### 4.7 결측 score와 실제 0점 구분 필요

**중요도: 낮음**

Reddit 변환은 score가 없을 때도 0을 저장합니다.

```python
"engagement": int(comment.get("score") or 0)
```

이 방식은 실제 0점과 결측값을 구분하지 못합니다. 공통 계약이 `integer/null`을 허용하므로 원본에 값이 없다면 `null`을 유지하는 편이 분석 정확도에 유리합니다.

### 4.8 Inspector 오류에 Kafka 위치 추가 필요

**중요도: 낮음**

Inspector는 JSON 문법 오류에는 topic, partition, offset을 표시하지만 계약 검증 오류에는 해당 위치를 붙이지 않습니다.

운영 단계에서는 다음과 같이 오류 위치를 확인할 수 있어야 합니다.

```text
TextEvent validation failed at raw-text[2] offset 152
```

Consumer에서 key와 `event_id`, Kafka timestamp와 `event_time`이 일치하는지도 함께 검사하면 통합 테스트 품질이 높아집니다.

## 5. 파일별 코드 리뷰

### `collectors/gdelt.py`

| 구분 | 내용 |
|---|---|
| 장점 | HTTP timeout, 429·5xx 재시도, 검색 조건 검증, 명확한 제목 분석 범위 |
| 위험 | 잘못된 날짜 한 건이 전체 수집 중단, 수집 cursor 없음 |
| 다음 작업 | 행 단위 실패 격리, `start <= end` 검사, 수집 통계와 실행별 출력 경로 |
| 평가 | MVP 수집기로 적절하며 주기 실행 전 복구 전략 필요 |

### `collectors/reddit.py`

| 구분 | 내용 |
|---|---|
| 장점 | Parquet streaming, 삭제 댓글·작성자 제외, subreddit 필터 |
| 위험 | 숫자 변환 오류가 전체 수집 중단, 결측 score가 0으로 변환 |
| 다음 작업 | 안전한 타입 변환, 오류 통계, 재시작 위치 저장 |
| 평가 | 개인정보와 메모리 사용 설계는 좋고 이상치 대응 보완 필요 |

### `core/events.py`

| 구분 | 내용 |
|---|---|
| 장점 | 필수·추가 필드 검사, SHA-256 ID, timezone 검사, 공통 검증 재사용 |
| 위험 | 출처별 필드 조합과 URI를 검증하지 않음 |
| 다음 작업 | 의미적 계약 추가, Python과 JSON Schema 동기화 |
| 평가 | 구조적 검증은 안정적이지만 의미적 계약 강화 필요 |

### `storage/jsonl.py`

| 구분 | 내용 |
|---|---|
| 장점 | 원자적 파일 교체, 실패 시 임시 파일 제거, 줄 번호 오류 |
| 위험 | 같은 경로로 실행하면 이전 데이터 덮어쓰기 |
| 다음 작업 | 날짜·실행 ID 기반 출력 경로와 overwrite 정책 정의 |
| 평가 | 구현이 단순하고 안정적이며 파일 관리 정책만 보완하면 됨 |

### `jobs/replay_to_kafka.py`

| 구분 | 내용 |
|---|---|
| 장점 | Producer와 CLI 분리, 시간 정렬, 배속·최대 지연, 종료 시 flush |
| 위험 | 마지막 성공 위치와 부분 실패 재시작 기준 없음 |
| 다음 작업 | checkpoint, DLQ 분기, 대용량 정렬 보호 장치 |
| 평가 | 데모용 replay에는 충분하며 운영용 재시작 설계 필요 |

### `producers/kafka.py`

| 구분 | 내용 |
|---|---|
| 장점 | `acks=all`, 멱등성, event key·timestamp, 압축, delivery 검사 |
| 위험 | 실제 Broker 장애 검증과 작업 전체 timeout 없음 |
| 다음 작업 | timeout·메트릭, DLQ Producer, Broker 장애 테스트 |
| 평가 | 책임 분리와 기본 안정성 설정이 잘 구성됨 |

### `jobs/init_kafka.py`

| 구분 | 내용 |
|---|---|
| 장점 | 토픽·파티션·보존 정책을 코드로 관리, 반복 실행 고려 |
| 위험 | 문자열 기반 오류 분류, 기존 토픽 설정은 검증하지 않음 |
| 다음 작업 | Kafka 오류 코드 비교, 기존 설정 조회·조정 |
| 평가 | 초기 생성 도구로 충분하지만 선언적 관리 기능은 부족 |

### `jobs/inspect_kafka.py`

| 구분 | 내용 |
|---|---|
| 장점 | 유한 소비, idle timeout, 자동 commit 비활성화, 계약 재검증 |
| 위험 | 계약 오류 위치와 key·timestamp 일치 여부를 확인하지 않음 |
| 다음 작업 | topic·partition·offset 문맥, key·timestamp 검증 |
| 평가 | 수동 확인에는 유용하며 통합 테스트 판정 기능 보강 필요 |

### `docker-compose.yml`

| 구분 | 내용 |
|---|---|
| 장점 | 공식 이미지, 버전 고정, health check, named volume |
| 위험 | 실제 Kafka 로그와 volume 경로가 다를 가능성 |
| 다음 작업 | 공식 단일 KRaft 예시에 맞춰 로그·listener·내부 토픽 설정 명시 |
| 평가 | 가장 먼저 수정하고 실제 재시작 검증해야 하는 파일 |

## 6. 개선 우선순위

```text
1. Kafka log volume과 KRaft Compose 설정 수정
                    ↓
2. TextEvent v1 출처별 의미 검증 강화
                    ↓
3. Collector 행 단위 실패 격리와 통계 추가
                    ↓
4. 실제 DLQ 발행·조회·재처리 구현
                    ↓
5. JSONL 실행별 경로와 checkpoint 정의
                    ↓
6. 실제 Broker 통합 및 재시작 테스트
                    ↓
7. 부하·장애·Consumer lag 측정
```

## 7. 테스트 현황과 보강 계획

현재 단위 테스트는 다음 기능을 검증합니다.

- GDELT·Reddit 원본의 공통 이벤트 변환
- 삭제 댓글과 작성자 정보 제외
- Python 계약과 JSON Schema의 공개 합성 데이터 검증
- Kafka key, value와 timestamp 매핑
- replay 배속과 최대 지연
- delivery callback과 flush 오류
- 토픽 초기화의 반복 실행
- Kafka 표본 소비와 계약 재검증

추가해야 할 통합 테스트:

1. 실제 Broker에서 합성 이벤트 발행·소비
2. Broker 재시작 후 메시지 보존 확인
3. 같은 입력 재발행 시 key와 partition 확인
4. 잘못된 이벤트의 DLQ 발행 확인
5. Producer 실행 중 Broker 중단과 복구
6. 보존 기간과 토픽 설정 확인

## 8. 발표 결론

> 현재 Ingestion은 데이터 출처별 Collector, 공통 이벤트 계약, 재현 가능한 JSONL staging과 Kafka Producer가 분리된 구조입니다. 데이터 변환과 메시지 매핑의 기본 동작은 단위 테스트로 검증했습니다. 다음 단계에서는 Kafka 데이터 영속성을 먼저 보장하고, 의미적 계약 검증과 DLQ를 구현한 뒤 실제 Broker 장애·재시작 통합 테스트로 안정성을 확인할 계획입니다.

기능 파일의 존재만 기준으로 하면 Kafka Ingestion 구현은 대부분 준비됐습니다. 하지만 운영 안정성까지 포함하면 현재 완성도는 약 **70~75%**로 평가합니다. 남은 작업의 핵심은 새로운 기능 추가보다 **데이터를 잃지 않고, 잘못된 한 건 때문에 전체 흐름을 멈추지 않으며, 실패 원인을 추적할 수 있게 만드는 것**입니다.
