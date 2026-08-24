# 커뮤니티 텍스트 품질·안전 규칙

- 정책 버전: 1
- 확정일: 2026-08-23
- 기준 구현: [`../../core/text_quality.py`](../../core/text_quality.py)
- 검증 fixture: [`text-quality-fixtures.jsonl`](text-quality-fixtures.jsonl)
- 적용 대상: `TextEvent v1.text`, 우선 Reddit 댓글

## 1. 목적과 비목표

이 규칙은 비정상 입력 한 건이 Kafka, Spark, PostgreSQL 또는 LLM 비용에 과도한 영향을 주지 않도록 측정값과 처리 상태를 정의합니다. 품질 신호를 근거로 사용자의 의도, 악성 여부, 정치적 성향이나 유해성을 단정하지 않습니다.

원문은 `text_original`에 보존하고 정제 결과는 `text_clean`에 파생합니다. 공개 저장소에는 실제 원문을 커밋하지 않습니다.

## 2. 처리 상태

| 상태 | 의미 | 후속 처리 |
|---|---|---|
| `accept` | 정의된 품질 신호 없음 | 정제·분석 대상으로 사용 |
| `flag` | 형식은 유효하지만 검토할 품질 신호가 있음 | 저장하고 분석하되 flag 집계 |
| `quarantine` | 비용·개인정보·Unicode 위험이 커 일반 분석에서 격리 | 원문 접근을 제한하고 자동 LLM 분석 제외 |
| `reject` | 내용이 없거나 원천 삭제 표시임 | 정제 출력과 분석 대상에서 제외 |

`reject`와 `quarantine`은 Kafka DLQ를 의미하지 않습니다. JSON 파싱 실패, 계약 위반과 지원하지 않는 schema version처럼 메시지 자체를 처리할 수 없는 경우에만 DLQ를 사용합니다.

## 3. 확정 임계값

| 측정값 | 기준 | flag | 상태 |
|---|---:|---|---|
| 문자 수 | 5,000자 초과 | `EXCESSIVE_LENGTH` | `flag` |
| 문자 수 | 20,000자 초과 | `EXCESSIVE_LENGTH` | `quarantine` |
| UTF-8 크기 | 65,536 byte 초과 | `EXCESSIVE_UTF8_BYTES` | `quarantine` |
| 허용하지 않는 제어 문자 | 1개 이상 | `CONTROL_CHARACTERS` | `flag`, 정제본에서 제거 |
| zero-width 문자 | 1개 이상 | `ZERO_WIDTH_CHARACTERS` | `flag`, 정제본에서 제거 |
| 연속 결합문자 | 8개 초과 | `EXCESSIVE_COMBINING_MARKS` | `quarantine` |
| 반복 비율 | 100자 이상이며 0.80 이상 | `HIGH_REPETITION` | `flag` |
| URL 비율 | URL 2개 이상이며 문자 점유율 0.50 이상 | `URL_HEAVY` | `flag` |
| 이메일·전화번호 후보 | 1개 이상 | `POSSIBLE_PII` | `quarantine` |

초기값은 시스템 보호를 위한 보수적 기준입니다. Spark 1,000건 profile에서 정상 댓글이 과도하게 flag되면 정책 버전을 올리고 근거 통계를 함께 기록합니다.

## 4. 측정 정의

### 문자와 byte

`character_count`는 Python/Spark 문자열의 Unicode code point 수, `utf8_byte_count`는 UTF-8 인코딩 결과의 byte 수입니다. 한글과 이모지는 문자 수가 같아도 ASCII보다 byte가 크므로 두 값을 모두 유지합니다.

### 반복 비율

100자 미만은 반복 탐지 대상에서 제외합니다. 그 이상은 다음 두 값 중 큰 값을 `repetition_ratio`로 사용합니다.

1. 공백을 제외한 문자 중 가장 많이 등장한 문자의 비율
2. UTF-8 byte를 zlib으로 압축했을 때 감소한 비율

이는 도배 가능성을 나타내는 휴리스틱이며 악성 행위 판정값이 아닙니다. Spark 구현에서는 같은 fixture 결과를 만족하는 동등한 알고리즘을 사용하거나 Python UDF의 비용을 비교합니다.

### 개인정보 후보

이메일과 전화번호 형태를 정규식으로 탐지하지만 확정적인 개인정보 판정은 아닙니다. 오탐 가능성이 있으므로 원문 삭제 대신 격리하고, LLM 전달 전 별도 마스킹 단계에서 다시 처리합니다.

### token 상한

문자·byte 상한은 Kafka 이전과 Spark에서 적용합니다. LLM token은 모델 tokenizer 결과여야 하므로 현재 품질 함수에서 문자 수로 추정하지 않습니다. LLM Batch 구현 시 문서별 정확한 `token_count`와 정책 버전을 기록하고, 상한 초과 문서만 LLM 요청에서 truncate 또는 제외합니다. 저장된 `text_original`과 `text_clean`은 자르지 않습니다.

## 5. Unicode 정책

- UTF-8 decoding은 수집기 경계에서 엄격하게 처리하며 복구 불가능한 byte 입력은 계약 오류로 기록합니다.
- 정제본에는 의미 변화가 비교적 작은 NFC 정규화를 적용합니다.
- NFKC는 호환 문자와 기호를 바꿀 수 있어 기본 적용하지 않습니다.
- 탭, LF, CR은 허용하고 그 외 Unicode `Cc` 제어 문자는 정제본에서 제거합니다.
- U+200B, U+2060과 U+FEFF는 개수를 기록하고 정제본에서 제거합니다.
- U+200C(ZWNJ)와 U+200D(ZWJ)는 일부 언어와 이모지 조합에서 의미가 있으므로 일괄 제거하지 않습니다.
- 이모지와 정상 범위의 결합문자는 보존합니다.
- 변경 여부를 `was_normalized`로 기록하며 원문을 덮어쓰지 않습니다.

## 6. Spark 출력 컬럼

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `quality_policy_version` | integer | 판정에 사용한 정책 버전, 현재 `1` |
| `text_original` | string | `TextEvent v1.text` 원문, 접근 제한 대상 |
| `text_clean` | string | 제어·zero-width 제거와 NFC 적용 결과 |
| `character_count` | long | 정제 전 Unicode 문자 수 |
| `utf8_byte_count` | long | 정제 전 UTF-8 byte 수 |
| `control_character_count` | long | 허용하지 않는 `Cc` 문자 수 |
| `zero_width_count` | long | 정책에서 지정한 zero-width 문자 수 |
| `max_combining_mark_run` | integer | NFC 후 연속 결합문자 최댓값 |
| `url_count` | integer | URL 정규식 일치 수 |
| `url_ratio` | double | 전체 정제 문자열 중 URL이 차지하는 비율 |
| `repetition_ratio` | double | 반복 휴리스틱 값 |
| `quality_status` | string | `accept`, `flag`, `quarantine`, `reject` |
| `quality_flags` | array<string> | 안정적인 품질 코드 목록 |
| `exclusion_reason` | string/null | reject·quarantine의 대표 원인 |
| `was_normalized` | boolean | 원문과 정제본이 다른지 여부 |
| `was_truncated` | boolean | 품질 단계에서는 항상 false, LLM 입력 단계에서 별도 기록 |

## 7. 단계별 책임

| 단계 | 책임 |
|---|---|
| Collector·JSONL | 엄격한 decoding, 필수 필드, 빈 값과 원천 tombstone의 1차 제외, hard byte 방어 |
| Kafka Producer | `TextEvent v1` 계약 검증과 broker 전송, 품질이 낮다는 이유로 DLQ에 보내지 않음 |
| Spark | 전체 측정값 생성, NFC 정제, flag·상태 결정, 원문과 정제본 분리 |
| PostgreSQL | 원문 접근 제한, 품질 컬럼과 정책 버전 저장, 격리 데이터 조회 분리 |
| LLM worker | `accept`·허용된 `flag`만 선택, PII 마스킹, 모델 tokenizer 기반 상한과 truncation 기록 |

정확한 `event_id` 중복은 Spark의 이벤트 deduplication에서 처리합니다. 동일 내용이 다른 ID로 반복되는 경우는 이번 단계에서 삭제하지 않고 후속 content hash·시간 window 정책의 입력으로 남깁니다.

## 8. Flag 사전

| flag | 의미 |
|---|---|
| `SOURCE_TOMBSTONE` | `[deleted]` 또는 `[removed]` 원천 표시 |
| `EMPTY_AFTER_NORMALIZATION` | 정제·trim 후 분석할 문자가 없음 |
| `EXCESSIVE_LENGTH` | soft 문자 상한 초과 |
| `EXCESSIVE_UTF8_BYTES` | hard UTF-8 byte 상한 초과 |
| `CONTROL_CHARACTERS` | 허용하지 않는 제어 문자 포함 |
| `ZERO_WIDTH_CHARACTERS` | 지정한 보이지 않는 문자 포함 |
| `EXCESSIVE_COMBINING_MARKS` | 허용 범위를 넘는 연속 결합문자 |
| `HIGH_REPETITION` | 반복 휴리스틱 임계값 이상 |
| `URL_HEAVY` | URL 개수와 점유율이 모두 임계값 이상 |
| `POSSIBLE_PII` | 이메일 또는 전화번호 형태 포함 |

## 9. 변경 규칙

- 임계값, 정규화 방식, flag 의미 또는 상태 우선순위를 바꾸면 정책 버전을 증가시킵니다.
- 기준 구현, fixture, Spark transformation과 문서를 함께 변경합니다.
- 기존 profile과 새 정책 결과를 비교해 정상 다국어 텍스트의 오탐 증가 여부를 기록합니다.
