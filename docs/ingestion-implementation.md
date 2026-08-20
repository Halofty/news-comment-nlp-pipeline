# Ingestion 구현 설명

## 1. 구현 목표

이 프로젝트의 Ingestion 단계에서는 서로 다른 형식의 뉴스와 댓글을 수집해 하나의 공통 이벤트로 변환하고, 재현 가능한 형태로 보관한 뒤 Kafka에 전달하도록 구현했습니다.

구현 범위는 다음과 같습니다.

```text
외부 데이터 수집
→ 공통 이벤트 변환
→ 데이터 계약 검증
→ JSONL staging
→ 과거 데이터 replay
→ Kafka 메시지 발행
→ Kafka 적재 결과 확인
```

MVP에서는 Collector가 Kafka에 직접 연결되지 않습니다. 수집 결과를 먼저 JSONL로 저장하고, 별도의 replay job이 Kafka로 전달합니다. 덕분에 외부 API를 다시 호출하지 않고도 동일한 데이터를 반복해서 테스트할 수 있습니다.

## 2. 전체 구현 구조

```text
GDELT DOC API ──→ collectors/gdelt.py ──┐
                                         │
Reddit Dataset ─→ collectors/reddit.py ─┤
                                         ↓
                                  TextEvent v1 생성
                                   core/events.py
                                         │
                                         ↓
                                  JSONL 원자적 저장
                                  storage/jsonl.py
                                         │
                                         ↓
                                  시간순·배속 replay
                               jobs/replay_to_kafka.py
                                         │
                                         ↓
                                    Kafka Producer
                                  producers/kafka.py
                                         │
                                         ↓
                                    raw-text Topic
                                         │
                                         ↓
                                   적재 결과 검증
                                jobs/inspect_kafka.py
```

## 3. 파일별 구현 내용

| 파일 | 구현한 역할 |
|---|---|
| `collectors/gdelt.py` | GDELT 뉴스 제목·URL·메타데이터 수집 및 변환 |
| `collectors/reddit.py` | Reddit 월별 댓글 스트리밍 수집 및 변환 |
| `collectors/article_fetcher.py` | 기사 전문 수집을 위한 확장 위치 표시 |
| `core/events.py` | `TextEvent v1` 공통 계약과 이벤트 ID·시각 검증 |
| `storage/jsonl.py` | 검증된 이벤트의 JSONL 읽기·원자적 저장 |
| `jobs/replay_to_kafka.py` | JSONL 이벤트의 시간순·배속 Kafka 재생 |
| `producers/kafka.py` | Kafka 연결 설정과 메시지 전송 |
| `jobs/init_kafka.py` | `raw-text`와 `raw-text-dlq` 토픽 초기화 |
| `jobs/inspect_kafka.py` | Kafka 메시지 표본 소비와 계약 재검증 |
| `docker-compose.yml` | 개발용 단일 Kafka Broker 실행 환경 |

## 4. GDELT 뉴스 수집 구현

### 4.1 API 요청

`collectors/gdelt.py`에서 검색어, 기간과 최대 건수를 받아 GDELT DOC API를 호출합니다.

```python
params: dict[str, str | int] = {
    "query": query,
    "mode": "ArtList",
    "maxrecords": max_records,
    "format": "json",
    "sort": "DateDesc",
}

response = client.get(GDELT_DOC_API, params=params, timeout=(5, 30))
response.raise_for_status()
```

구현 내용:

- `mode=ArtList`로 기사 목록을 요청합니다.
- 한 번에 최대 250건까지만 요청하도록 검사합니다.
- `startdatetime`, `enddatetime`으로 UTC 기간을 지정할 수 있습니다.
- 연결 timeout은 5초, 응답 timeout은 30초입니다.
- HTTP 오류를 정상 응답처럼 처리하지 않고 예외로 전달합니다.

HTTP session에는 재시도 정책을 적용했습니다.

```python
retry = Retry(
    total=4,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
)
```

일시적인 rate limit이나 서버 오류가 발생하면 최대 네 번 재시도합니다.

### 4.2 분석 가능한 기사 필터링

```python
url = str(article.get("url") or "").strip()
title = str(article.get("title") or "").strip()
seen_date = str(article.get("seendate") or "").strip()

if not url or not title or not seen_date:
    return None
```

URL, 제목 또는 관측 시각이 없는 기사는 이후 분석과 이벤트 시간 처리에 사용할 수 없으므로 제외합니다.

### 4.3 뉴스 이벤트 변환

```python
return {
    "event_id": stable_event_id("gdelt", url),
    "source_type": "news",
    "source_name": "gdelt",
    "event_time": event_time,
    "collected_at": collected_at,
    "language": str(article.get("language") or "unknown").lower(),
    "title": title,
    "text": title,
    "url": url,
    "community": None,
    "engagement": None,
    "schema_version": 1,
    "metadata": {
        "domain": article.get("domain"),
        "source_country": article.get("sourcecountry"),
        "query": query,
        "text_scope": "title_only",
    },
}
```

MVP에서는 기사 전문이 아닌 제목을 분석합니다. 따라서 `title`과 `text`에 같은 값을 저장하고 `metadata.text_scope`에 `title_only`를 기록했습니다.

뉴스에 존재하지 않는 Reddit 전용 필드도 제거하지 않고 `null`로 유지합니다. 이렇게 해야 모든 이벤트의 최상위 구조가 같아집니다.

## 5. Reddit 댓글 수집 구현

### 5.1 월별 Parquet 스트리밍

`collectors/reddit.py`는 Hugging Face의 월별 Reddit 댓글 파일을 스트리밍으로 읽습니다.

```python
data_file = f"data/RC_{month}.parquet"
return load_dataset(
    DATASET_ID,
    data_files={"train": data_file},
    split="train",
    streaming=True,
)
```

전체 월별 파일을 메모리에 올리지 않고 한 행씩 읽을 수 있도록 `streaming=True`를 사용했습니다.

### 5.2 삭제·결측 댓글 제외

```python
if (
    not comment_id
    or not body
    or body in {"[deleted]", "[removed]"}
    or created_utc is None
):
    return None
```

다음 댓글은 분석 대상에서 제외합니다.

- 댓글 ID가 없는 데이터
- 본문이 비어 있는 데이터
- `[deleted]`, `[removed]` 댓글
- 작성 시각이 없는 데이터

작성자 필드는 읽더라도 공통 이벤트에는 복사하지 않습니다.

### 5.3 댓글 이벤트 변환

```python
return {
    "event_id": stable_event_id("reddit", comment_id),
    "source_type": "comment",
    "source_name": "reddit",
    "event_time": event_time,
    "collected_at": collected_at,
    "language": "unknown",
    "title": None,
    "text": body,
    "url": None,
    "community": comment.get("subreddit"),
    "engagement": int(comment.get("score") or 0),
    "schema_version": 1,
    "metadata": {
        "link_id": comment.get("link_id"),
        "controversiality": int(comment.get("controversiality") or 0),
    },
}
```

원본 Reddit 필드는 다음과 같이 공통 필드에 대응시켰습니다.

| Reddit 원본 | 공통 이벤트 | 설명 |
|---|---|---|
| `id` | `event_id` | 출처와 함께 SHA-256으로 변환 |
| `body` | `text` | NLP·LLM 분석 대상 |
| `created_utc` | `event_time` | UTC ISO-8601로 변환 |
| `subreddit` | `community` | 댓글이 작성된 커뮤니티 |
| `score` | `engagement` | 데이터셋에 기록된 반응 수치 |
| `link_id` | `metadata.link_id` | 연결된 게시물 식별값 |

### 5.4 커뮤니티 필터와 수집 건수 제한

```python
for row in rows:
    subreddit = str(row.get("subreddit") or "").casefold()
    if normalized and subreddit not in normalized:
        continue

    event = comment_to_event(row, collected_at=collected_at)
    if event is None:
        continue

    yield event
    emitted += 1
    if emitted >= limit:
        break
```

`casefold()`로 대소문자 차이를 제거하고, 유효한 이벤트를 지정한 `limit`만큼 만들면 탐색을 중단합니다.

## 6. TextEvent v1 구현

GDELT와 Reddit은 원본 필드가 다르므로 `core/events.py`에 공통 데이터 계약을 구현했습니다.

### 6.1 안정적인 이벤트 ID

```python
def stable_event_id(source_name: str, source_id: str) -> str:
    value = f"{source_name}:{source_id}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()
```

동일한 출처와 원본 ID는 수집을 다시 실행해도 같은 64자리 ID를 만듭니다. 출처 이름까지 해시 입력에 포함해 서로 다른 출처의 ID 충돌을 방지했습니다.

```text
GDELT → sha256("gdelt:" + article_url)
Reddit → sha256("reddit:" + comment_id)
```

### 6.2 이벤트 시각 검증

```python
def parse_event_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("event_time must be an ISO-8601 string")

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)

    if parsed.tzinfo is None:
        raise ValueError("event_time must include a timezone")
    return parsed
```

모든 시각이 timezone을 포함하도록 검사합니다. 이후 Spark가 이벤트 시간 기준 watermark와 window를 적용하려면 timezone이 명확해야 합니다.

### 6.3 필수 필드와 추가 필드 검증

```python
missing = sorted(EVENT_FIELDS.difference(event))
if missing:
    raise ValueError(
        f"event{location} is missing fields: {', '.join(missing)}"
    )

unexpected = sorted(set(event).difference(EVENT_FIELDS))
if unexpected:
    raise ValueError(
        f"event{location} has unexpected fields: {', '.join(unexpected)}"
    )
```

- 정의된 최상위 필드는 모두 존재해야 합니다.
- 출처에 해당하지 않는 필드는 생략하지 않고 `null`로 둡니다.
- 정의되지 않은 출처별 정보는 최상위가 아니라 `metadata`에 넣습니다.

### 6.4 주요 타입과 버전 검증

```python
if not isinstance(event["event_id"], str) or not EVENT_ID_PATTERN.fullmatch(
    event["event_id"]
):
    raise ValueError(...)

if not isinstance(event["text"], str) or not event["text"].strip():
    raise ValueError(...)

if event["schema_version"] != 1:
    raise ValueError(...)
```

빈 분석 텍스트, 잘못된 이벤트 ID와 지원하지 않는 계약 버전을 Kafka에 보내기 전에 차단합니다.

## 7. JSONL Staging 구현

`storage/jsonl.py`는 검증된 이벤트를 줄 단위 JSON으로 저장하고 다시 읽습니다.

### 7.1 줄 단위 읽기와 오류 위치

```python
for line_number, line in enumerate(file, start=1):
    if not line.strip():
        continue
    try:
        event = json.loads(line)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"invalid JSON on line {line_number}: {error.msg}"
        ) from error
    yield validate_event(event, line_number=line_number)
```

JSON 문법과 `TextEvent v1`을 다시 검사하며, 문제가 있으면 JSONL의 줄 번호를 표시합니다.

### 7.2 원자적 파일 저장

```python
temporary = output.with_suffix(output.suffix + ".tmp")
try:
    with temporary.open("w", encoding="utf-8") as file:
        for event in events:
            validate_event(event)
            file.write(json.dumps(event, ensure_ascii=False) + "\n")
    temporary.replace(output)
except Exception:
    temporary.unlink(missing_ok=True)
    raise
```

최종 파일에 직접 쓰지 않고 `.tmp` 파일을 먼저 완성합니다. 모든 이벤트가 정상적으로 저장된 후에만 최종 경로로 교체하므로 중간 실패로 불완전한 JSONL이 남는 것을 방지합니다.

## 8. Kafka Replay Job 구현

`jobs/replay_to_kafka.py`는 JSONL 읽기, 이벤트 정렬, 전송 간격 계산과 Producer 종료를 담당합니다.

### 8.1 이벤트 시간순 정렬

```python
def order_events(events, *, sort_by_event_time):
    if not sort_by_event_time:
        return events
    return sorted(
        events,
        key=lambda event: parse_event_time(event["event_time"]),
    )
```

`--sort-by-event-time` 옵션을 사용하면 과거 이벤트를 실제 발생 순서에 맞춰 재생할 수 있습니다.

### 8.2 배속 재생

```python
event_gap = max(
    0.0,
    (event_time - previous_time).total_seconds(),
)
delay = min(event_gap / speed, max_delay)
if delay > 0:
    sleeper(delay)
```

| 옵션 | 동작 |
|---|---|
| `speed=0` | 기다리지 않고 최대 속도로 전송 |
| `speed=1` | 원본 이벤트 간격대로 전송 |
| `speed=100` | 원본보다 100배 빠르게 전송 |
| `max_delay=5` | 실제 대기 시간을 최대 5초로 제한 |

오래된 Reddit 댓글을 이용해 실시간 스트리밍처럼 보이는 입력을 재현할 수 있습니다.

### 8.3 Producer 종료 보장

```python
try:
    for event in events:
        producer.send(event)
finally:
    producer.close()
```

읽기나 전송 과정에서 오류가 발생하더라도 `close()`를 실행하여 Producer 내부에 남아 있는 메시지의 전달 결과를 확인합니다.

## 9. Kafka Producer 구현

`producers/kafka.py`에는 Kafka 연결과 메시지 전송 책임만 남겼습니다. CLI와 JSONL 처리 코드는 replay job으로 분리했습니다.

### 9.1 Producer 설정

```python
return Producer(
    {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
    }
)
```

| 설정 | 구현 이유 |
|---|---|
| `acks=all` | Broker가 기록을 확인한 뒤 성공 처리 |
| `enable.idempotence=True` | Producer 재시도 중 중복 가능성 감소 |
| `compression.type=zstd` | 텍스트 메시지의 네트워크·저장 사용량 절감 |
| `client.id` | 로그에서 Producer 식별 |

### 9.2 Kafka 메시지 구성

```python
self.client.produce(
    self.topic,
    key=event["event_id"].encode("utf-8"),
    value=payload,
    timestamp=timestamp_ms,
    on_delivery=self._on_delivery,
)
```

| Kafka 요소 | 사용한 값 | 이후 활용 |
|---|---|---|
| Topic | `raw-text` | 정상 원본 이벤트 전달 |
| Key | `event_id` | 파티션 일관성과 중복 제거 기준 |
| Value | `TextEvent v1` JSON | Spark 공통 Schema 입력 |
| Timestamp | `event_time` | watermark와 시간 window 기준 |

### 9.3 Producer 큐 포화 처리

```python
while True:
    try:
        self.client.produce(...)
        break
    except BufferError:
        self.client.poll(0.5)
```

로컬 Producer 큐가 가득 차면 즉시 실패하지 않고 callback을 처리하며 공간이 생길 때까지 기다립니다.

### 9.4 실제 전달 결과 확인

```python
remaining = self.client.flush(self.flush_timeout)
if remaining:
    raise DeliveryError(
        f"{remaining} Kafka messages were not delivered"
    )

if self.delivery_errors:
    raise DeliveryError(...)
```

Producer API 호출 성공만으로 전송 완료라고 판단하지 않습니다. `flush()`와 delivery callback을 확인해 전달되지 않은 메시지가 있으면 성공 메시지를 출력하지 않고 오류로 종료합니다.

## 10. Kafka Broker와 토픽 구현

### 10.1 개발용 Broker

`docker-compose.yml`에 공식 Apache Kafka 이미지 기반의 단일 KRaft Broker를 정의했습니다.

```yaml
services:
  kafka:
    image: apache/kafka:4.3.1
    ports:
      - "9092:9092"
    healthcheck:
      test:
        - CMD-SHELL
        - /opt/kafka/bin/kafka-broker-api-versions.sh \
          --bootstrap-server localhost:9092
```

개발 환경에서 Broker 준비 상태를 확인한 뒤 Producer를 실행할 수 있도록 health check를 추가했습니다.

### 10.2 토픽 초기화

`jobs/init_kafka.py`에 정상 이벤트와 오류 이벤트용 토픽 정책을 정의했습니다.

```python
TOPICS = {
    "raw-text": {
        "retention.ms": str(7 * 24 * 60 * 60 * 1000)
    },
    "raw-text-dlq": {
        "retention.ms": str(30 * 24 * 60 * 60 * 1000)
    },
}
```

| 토픽 | 기본 파티션 | 보존 기간 | 용도 |
|---|---:|---:|---|
| `raw-text` | 3 | 7일 | 정상 뉴스·댓글 이벤트 |
| `raw-text-dlq` | 3 | 30일 | 오류 조사와 재처리 예정 이벤트 |

```python
if "TOPIC_ALREADY_EXISTS" in str(error):
    existing.append(name)
    continue
```

토픽이 이미 존재하면 정상 상태로 취급하므로 초기화 명령을 반복 실행할 수 있습니다.

## 11. Kafka 적재 확인 구현

`jobs/inspect_kafka.py`는 Spark Consumer를 구현하기 전에 Kafka 적재 결과를 확인하기 위한 개발용 도구입니다.

### 11.1 Consumer 설정

```python
return Consumer(
    {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": (
            "earliest" if from_beginning else "latest"
        ),
        "enable.auto.commit": False,
    }
)
```

- 처음부터 확인하거나 최신 메시지부터 확인할 수 있습니다.
- 개발용 확인 작업이 운영 처리 상태에 영향을 주지 않도록 자동 offset commit을 비활성화했습니다.

### 11.2 유한 표본 소비와 재검증

```python
while len(events) < limit:
    message = consumer.poll(idle_timeout)
    if message is None:
        break
    if message.error():
        raise RuntimeError(...)

    event = json.loads(message.value())
    events.append(validate_event(event))
```

지정한 건수만 읽고, 일정 시간 동안 메시지가 없으면 종료합니다. 소비한 JSON을 다시 `TextEvent v1`으로 검증하여 Producer가 올바른 메시지를 적재했는지 확인합니다.

## 12. 실제 실행 과정

### 1단계: Kafka 실행

```bash
docker compose up -d kafka
docker compose ps
```

### 2단계: 토픽 준비

```bash
.venv/bin/python -m jobs.init_kafka
```

### 3단계: 데이터 수집

GDELT 예시:

```bash
.venv/bin/python -m collectors.gdelt \
  --query "artificial intelligence" \
  --max-records 100 \
  --output data/raw/gdelt.jsonl
```

Reddit 예시:

```bash
.venv/bin/python -m collectors.reddit \
  --month 2016-01 \
  --subreddit worldnews \
  --limit 100 \
  --output data/raw/reddit.jsonl
```

### 4단계: Kafka 발행

```bash
.venv/bin/python -m jobs.replay_to_kafka \
  --input data/raw/gdelt.jsonl \
  --topic raw-text
```

합성 이벤트로 확인할 수도 있습니다.

```bash
.venv/bin/python -m jobs.replay_to_kafka \
  --input sample/synthetic-events.jsonl
```

### 5단계: 적재 결과 확인

```bash
.venv/bin/python -m jobs.inspect_kafka \
  --topic raw-text \
  --from-beginning \
  --group-id ingestion-check-1 \
  --limit 10 \
  --idle-timeout 5
```

## 13. 구현 검증

| 테스트 파일 | 검증 내용 |
|---|---|
| `tests/test_collectors.py` | GDELT·Reddit 변환, 필터와 개인정보 제외 |
| `tests/test_event_schema.py` | JSON Schema와 Python 계약 일치 |
| `tests/test_kafka_producer.py` | key·timestamp·전송·replay·오류 처리 |
| `tests/test_kafka_ingestion.py` | 토픽 초기화와 Consumer 표본 검증 |

현재 외부 서비스 없이 실행하는 단위 테스트는 총 16개이며 모두 통과합니다.

```text
16 passed
```

## 14. 현재 구현 범위

### 구현 완료

- GDELT 뉴스 제목·메타데이터 수집
- Reddit 댓글 스트리밍 표본 수집
- 뉴스와 댓글의 `TextEvent v1` 변환
- 안정적인 SHA-256 이벤트 ID
- 공통 필드·타입·시각·버전 검증
- JSONL 원자적 저장과 재검증
- 이벤트 시간 정렬과 배속 replay
- Kafka Producer의 멱등성·압축·전달 결과 확인
- `raw-text`, `raw-text-dlq` 토픽 초기화
- Kafka 메시지 표본 소비와 계약 재검증
- 개발용 Kafka Compose 정의

### 이후 구현 예정

- 기사 전문 수집
- 오류 이벤트의 실제 DLQ 발행과 재처리
- 실행별 JSONL 저장 경로와 수집 checkpoint
- Collector에서 Kafka로 직접 보내는 선택적 Sink
- Airflow 기반 주기 실행과 retry/backoff
- Spark Consumer의 정제·중복 제거·watermark 처리
- Kafka Broker 재시작과 장애 상황 통합 테스트

`raw-text-dlq`는 현재 토픽 생성까지만 구현되어 있습니다. 계약 오류를 DLQ에 발행하고 다시 처리하는 기능은 다음 구현 범위입니다.

## 15. 발표 요약

> 서로 다른 GDELT 뉴스와 Reddit 댓글을 각 Collector에서 수집한 뒤 `TextEvent v1`이라는 공통 스키마로 변환했습니다. 변환된 데이터는 바로 Kafka에 보내지 않고 JSONL staging에 원자적으로 저장하여 재현성과 재처리 가능성을 확보했습니다. 별도의 replay job이 과거 이벤트를 시간순 또는 배속으로 재생하고, Kafka Producer는 `event_id`를 key, `event_time`을 timestamp로 사용해 `raw-text` 토픽에 전달합니다. 마지막으로 개발용 Inspector가 Kafka 메시지를 다시 읽어 동일한 데이터 계약으로 검증하도록 구현했습니다.

이 구현에서 중요하게 생각한 부분은 **출처별 수집 책임과 Kafka 전송 책임을 분리한 것**, **Kafka 이후 단계가 출처 차이를 알 필요 없도록 공통 계약을 둔 것**, 그리고 **외부 API를 반복 호출하지 않고 같은 데이터를 재생할 수 있도록 JSONL staging을 둔 것**입니다.
