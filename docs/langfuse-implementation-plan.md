# Langfuse 구현과 토큰 관리 계획

## 1. 목적과 범위

OpenAI Batch API로 처리하는 뉴스·댓글 분석의 token, 비용, 지연, 오류와 재시도를 Langfuse에서 관측하기 위한 구현 계획입니다.

MVP는 [ADR-0001](adr/0001-langfuse-deployment.md)에 따라 관리형 Langfuse Cloud 일본 리전을 사용합니다. 기사·댓글·prompt·응답 원문은 Langfuse에 보내지 않고 허용된 metadata와 usage만 기록합니다.

이 문서의 범위는 다음과 같습니다.

- Langfuse adapter와 fallback 구현
- 비동기 OpenAI Batch trace 구성
- token·비용 수집, 대조와 예산 제어
- 개인정보·원문 전송 방지
- 단위·통합·장애 테스트와 완료 조건

OpenAI Batch 요청 생성, 제출, polling과 분석 결과 적재의 전체 설계는 [LLM 분석 설계](llm-analysis-design.md)를 따릅니다.

## 2. 책임 분리

| 구성요소 | 기준 책임 | 장애 시 동작 |
|---|---|---|
| PostgreSQL | Batch·요청 상태, 재시도, 결과, 실제 token 합계 | 처리 재개와 중복 방지의 기준으로 계속 사용 |
| Langfuse | trace 조회, token·비용·지연 집계와 대시보드 | 기록을 건너뛰고 경고 로그 생성 |
| 구조화 로그 | Langfuse 전송 성공·실패와 usage 대조 결과 | 운영자가 재전송·원인 확인에 사용 |
| OpenAI 응답 | 실제 문서별·Batch별 token usage 기준값 | 응답이 없으면 비용 확정 대신 미확정 상태 기록 |

Langfuse는 관측 복제본이며 핵심 처리의 기준 저장소가 아닙니다. trace가 누락되어도 Batch polling, 결과 검증과 PostgreSQL 적재는 계속되어야 합니다.

## 3. 목표 구현 구조

```text
observability/
├── __init__.py
├── models.py              # vendor 독립 trace·usage 자료형
├── sink.py                # ObservabilitySink protocol
├── langfuse_sink.py       # Langfuse Python SDK v4 adapter
├── structured_log_sink.py # metadata-only JSON 로그 fallback
└── noop_sink.py           # 관측 비활성화 구현

jobs/
└── verify_langfuse.py     # 합성 trace 전송·조회 검증 CLI

tests/
├── test_observability_sink.py
├── test_langfuse_sink.py
├── test_token_reconciliation.py
└── test_observability_failure.py
```

Batch workflow는 Langfuse SDK 객체를 직접 import하지 않고 다음 경계만 호출합니다.

```text
Batch workflow ──> ObservabilitySink
                    ├── LangfuseSink
                    ├── StructuredLogSink
                    └── NoOpSink
```

제안하는 interface 책임은 다음과 같습니다.

- `start_batch`: 내부 Batch trace 식별자와 제출 metadata 등록
- `record_stage`: JSONL 생성·제출·polling·결과 적재 단계 기록
- `record_generation`: 문서별 결과, usage, 비용과 검증 상태 기록
- `record_reconciliation`: 문서 합계와 Batch 합계의 대조 결과 기록
- `flush`: 짧게 실행되는 CLI 종료 전 전송 완료 시도

모든 method는 허용된 자료형만 입력받고 Langfuse SDK 예외를 외부로 전달하지 않습니다.

## 4. Trace와 문서 연결

```text
Trace: 내부 llm_batch_id 1건
├── Span: build-request-file
├── Span: submit-openai-batch
├── Span: poll-openai-batch
├── Span: load-and-validate-results
└── Generation: custom_id + attempt 1건
    ├── event_id
    ├── prompt_version / schema_version / model
    ├── status / error_code / validation_result
    ├── input_tokens / cached_input_tokens
    ├── output_tokens / reasoning_output_tokens
    ├── total_tokens / cost
    └── submitted_at / completed_at
```

OpenAI Batch는 최대 수 시간 동안 비동기로 실행될 수 있으므로 Python context manager를 제출부터 완료까지 열어 두지 않습니다.

1. PostgreSQL에 먼저 내부 `llm_batch_id`를 생성합니다.
2. `llm_batch_id`를 seed로 안정적인 Langfuse trace ID를 생성합니다.
3. 요청 생성·제출·polling은 같은 trace에 독립 span으로 기록합니다.
4. 결과 JSONL을 적재할 때 `custom_id`와 `attempt`별 generation을 생성합니다.
5. `event_id + prompt_version`으로 논리 요청을, `custom_id + attempt`로 실제 과금 시도를 구분합니다.

동일 문서를 재시도하면 기존 generation을 덮어쓰지 않습니다. 각 시도의 token과 비용을 별도 기록하고 논리 요청 단위 누적 비용은 Langfuse 또는 PostgreSQL 조회에서 합산합니다.

## 5. Token 관리 계획

### 5.1 Token 값의 종류

| 값 | 출처 | 용도 | 기준값 여부 |
|---|---|---|:---:|
| `estimated_input_tokens` | 제출 전 tokenizer 추정 | 입력 제한과 예상 예산 검사 | 아니요 |
| `input_tokens` | OpenAI 문서별 결과 usage | 실제 입력 사용량 | 예 |
| `cached_input_tokens` | OpenAI usage 상세 | cache 비용 구분 | 예 |
| `output_tokens` | OpenAI 문서별 결과 usage | 실제 출력 사용량 | 예 |
| `reasoning_output_tokens` | 모델 응답 usage 상세 | reasoning 사용량 구분 | 예 |
| `total_tokens` | OpenAI usage | 문서별 총사용량 대조 | 예 |
| Batch token 합계 | OpenAI Batch 객체 usage | 문서별 합계 대조 | 예 |

추정 token은 제출 차단과 예산 예측에만 사용합니다. 과금·보고에는 API가 반환한 실제 usage를 사용합니다.

### 5.2 수집 시점

```text
요청 생성
→ tokenizer로 입력 token 추정
→ 문서 상한·Batch 예상 예산 확인
→ OpenAI Batch 제출
→ 결과 JSONL 다운로드
→ custom_id별 실제 usage 파싱
→ 문서별 generation 기록
→ 문서별 합계와 Batch usage 대조
→ PostgreSQL 최소 usage와 Langfuse flush
```

실패·만료되어 usage를 받지 못한 요청은 token을 `0`으로 단정하지 않고 `usage_status=unavailable`로 기록합니다.

### 5.3 대조 규칙

Batch 완료 후 다음 항목을 자동 검사합니다.

- 성공 결과의 `input_tokens` 합계와 Batch `input_tokens`
- 성공 결과의 `output_tokens` 합계와 Batch `output_tokens`
- 문서별 `input + output`과 `total_tokens`
- 전체 문서 token 합계와 Batch `total_tokens`
- 완료·실패·누락 요청 수와 Batch `request_counts`
- PostgreSQL 요청 수와 Langfuse generation 기록 대상 수

정확히 일치하면 `reconciliation_status=matched`, Batch API가 세부 usage를 제공하지 않으면 `not_available`, 값이 다르면 `mismatched`로 기록합니다. 불일치가 있어도 결과 적재는 완료하고 운영 경고를 남깁니다.

## 6. 비용 관리 계획

### 6.1 비용 계산

- token 수는 OpenAI 응답을 기준으로 합니다.
- 가격은 모델, usage 유형, Batch 할인과 가격 기준일을 포함한 versioned price 설정으로 관리합니다.
- Langfuse가 해당 Batch 가격을 정확히 계산하지 못하면 `cost_details`를 명시적으로 전달합니다.
- 일반 API 단가와 Batch 단가를 혼용하지 않습니다.
- 가격표가 없거나 모델명이 매칭되지 않으면 비용을 `0`으로 만들지 않고 `cost_status=unresolved`로 기록합니다.

각 generation에는 `pricing_version`, `pricing_effective_date`, `currency=USD`를 metadata로 남깁니다. 가격 변경 후 과거 비용이 조용히 바뀌지 않도록 계산 당시의 버전을 보존합니다.

### 6.2 재시도 비용

최초 요청과 재시도는 다음처럼 구분합니다.

```text
logical_request_key = event_id + prompt_version
attempt = 1, 2, 3 ...
custom_id = logical_request_key + attempt
```

- `attempt=1`은 최초 비용입니다.
- `attempt>1`은 재시도 비용입니다.
- 같은 `event_id + prompt_version`의 완료 결과가 있으면 신규 제출을 차단합니다.
- 재시도 누적 비용과 재시도 때문에 증가한 token을 별도 집계합니다.

### 6.3 예산 제어

MVP에서는 다음 세 단계로 예산을 제어합니다.

1. 제출 전 예상 token과 단가로 Batch 예상 비용을 계산합니다.
2. PostgreSQL에 저장된 당일 확정 비용과 미완료 Batch 예상 비용을 합산합니다.
3. 일별 한도를 넘으면 신규 제출만 중단하고 polling·결과 적재·Langfuse 기록은 계속합니다.

경고 기준은 기본적으로 일별 예산의 70%, 90%, 100%로 두되 설정값으로 변경할 수 있게 합니다. 100% 도달 시 상태를 `budget_blocked`로 기록하고 운영자가 한도 또는 입력 범위를 조정한 뒤 재개합니다.

## 7. 전송 데이터와 보안

### 허용 metadata

- 내부 `llm_batch_id`, `event_id`, `custom_id`
- model, prompt version, schema version, 실행 환경
- token, 비용, 상태, 오류 코드, attempt
- 단계별 시각, 지연 시간과 Schema 검증 결과
- 원문을 포함하지 않는 품질 flag 집계

### 금지 데이터

- 기사·댓글·제목 원문
- prompt·LLM 응답 원문
- URL, 커뮤니티, 작성자와 사용자 식별값
- API key, DSN, 파일 내용과 자유 형식 예외 메시지

SDK의 `input`과 `output`은 비워 둡니다. metadata는 allowlist builder가 생성하고 전송 직전에 masking hook으로 한 번 더 검사합니다. 금지 key나 허용되지 않은 자유 문자열이 발견되면 trace를 폐기하고 보안 경고만 기록합니다.

## 8. 환경 설정

7단계 구현 시 `.env.example`에 값이 아닌 변수명과 설명만 추가합니다.

```dotenv
LANGFUSE_ENABLED=false
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
LANGFUSE_BASE_URL=https://jp.cloud.langfuse.com
LANGFUSE_TRACING_ENVIRONMENT=development
LANGFUSE_TIMEOUT_SECONDS=5
LLM_DAILY_BUDGET_USD=
LLM_BUDGET_WARNING_RATIOS=0.70,0.90,1.00
```

- secret이 없으면 `StructuredLogSink` 또는 `NoOpSink`를 선택합니다.
- 실제 `.env`와 key는 Git에 포함하지 않습니다.
- `LANGFUSE_ENABLED=true`인데 필수 설정이 없으면 시작 시 명확한 경고를 남기되 LLM 처리는 중단하지 않습니다.

## 9. 구현 단계

### 7-1. Vendor 독립 계약

- `UsageRecord`, `CostRecord`, `BatchTrace`, `GenerationTrace` 자료형 작성
- allowlist metadata builder 작성
- `ObservabilitySink` protocol과 `NoOpSink` 구현
- 실제 SDK 없이 단위 테스트

### 7-2. Langfuse adapter

- `langfuse>=4,<5` 의존성 추가
- 환경 변수 기반 client factory 구현
- 안정적인 trace ID와 수동 generation 기록
- 명시적 `usage_details`, `cost_details`와 flush 구현
- SDK 오류·timeout 격리

### 7-3. Batch usage 대조

- OpenAI 결과 fixture에서 usage 추출
- 문서별 합계와 Batch usage 대조
- 재시도·누락·실패 usage 처리
- PostgreSQL 최소 usage 필드 또는 운영 기록 연결

### 7-4. 합성 데이터 통합 검증

- 원문 없는 합성 Batch trace 전송
- Langfuse UI/API에서 trace·generation·token·비용 확인
- metadata에 금지 데이터가 없는지 검사
- Langfuse endpoint 차단과 잘못된 key로 fallback 검증

### 7-5. LLM Batch workflow 연결

- 요청 생성·제출·polling·결과 적재 단계에 sink 연결
- 동일 결과 재적재 시 중복 generation 방지
- CLI 종료 전 제한 시간 flush
- 실행 가이드와 검증 보고서 작성

## 10. 테스트 계획

| 구분 | 시나리오 | 기대 결과 |
|---|---|---|
| 단위 | 허용 metadata만 입력 | sink 호출 성공 |
| 단위 | 원문·URL·자유 형식 오류 포함 | 전송 차단과 보안 경고 |
| 단위 | 문서·Batch usage 일치 | `matched` |
| 단위 | token 합계 불일치 | `mismatched`, 결과 적재 유지 |
| 단위 | pricing 정보 없음 | `unresolved`, 비용 0으로 왜곡하지 않음 |
| 단위 | 동일 문서 재시도 | attempt별 비용 분리 |
| 단위 | 동일 generation 재적재 | 중복 기록 방지 |
| 장애 | Langfuse timeout·인증 실패 | fallback 전환, 본 처리 성공 |
| 장애 | flush 실패 | 경고 기록, 종료 코드와 결과 저장 유지 |
| 통합 | 합성 Batch 5~10건 | trace 1건과 문서별 generation 확인 |
| 통합 | 일별 예산 초과 | 신규 제출만 `budget_blocked` |

## 11. 완료 조건과 산출물

다음을 모두 만족하면 피드백 7단계를 완료로 표시합니다.

- 합성 LLM Batch 한 건이 하나의 trace로 조회됩니다.
- `custom_id`별 generation과 실제 입력·출력 token이 확인됩니다.
- 문서별 합계와 Batch usage 대조 결과가 남습니다.
- 최초 요청과 재시도 비용이 분리됩니다.
- prompt·응답·기사·댓글 원문이 trace에 존재하지 않습니다.
- Langfuse 중단 상태에서도 Batch 결과가 PostgreSQL에 저장됩니다.
- 실행 명령, 환경 변수, 검증 결과가 문서화됩니다.

예상 산출물은 다음과 같습니다.

- `observability/` adapter 구현
- token·비용 대조 단위 테스트
- 합성 trace 통합 테스트 또는 검증 CLI
- `analysis/reports/langfuse-token-validation.md`
- `.env.example`의 Langfuse 설정 항목

현재 구현·검증 상태는 [Langfuse 샘플 token·비용 추적 검증](../analysis/reports/langfuse-token-validation.md)에 기록합니다.

## 12. 관련 문서

- [Langfuse 도입 ADR](adr/0001-langfuse-deployment.md)
- [LLM 분석 설계](llm-analysis-design.md)
- [PostgreSQL 저장 구조](storage-schema.md)
- [데이터와 보안 원칙](data-security.md)
- [피드백 구현 계획](feedback-implementation-plan.md)
- [장애·부하 테스트 계획](failure-and-load-test-plan.md)

## 13. 공식 참고 자료

- [Langfuse token·cost 추적](https://langfuse.com/docs/observability/features/token-and-cost-tracking)
- [Langfuse Python SDK v4 변경 사항](https://langfuse.com/docs/observability/sdk/upgrade-path/python-v3-to-v4)
- [Langfuse 민감 데이터 masking](https://langfuse.com/docs/observability/features/masking)
- [Langfuse event batching과 flush](https://langfuse.com/docs/observability/features/queuing-batching)
- [OpenAI Batch 객체와 usage](https://platform.openai.com/docs/api-reference/batch/object?api-mode=responses)
