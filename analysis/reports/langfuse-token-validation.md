# Langfuse 샘플 token·비용 추적 검증

## 1. 검증 목적

OpenAI Batch 형식의 합성 결과를 사용해 문서별 token·비용을 계산하고, Batch 합계와 대조한 뒤 metadata-only 관측 payload로 기록할 수 있는지 확인했습니다.

실제 OpenAI API와 Langfuse Cloud에는 요청하지 않았습니다. 현재 환경에 Langfuse 자격 증명이 없어 Cloud UI 확인은 후속 검증으로 남겼습니다.

## 2. 환경

| 항목 | 값 |
|---|---|
| 검증일 | 2026-08-24 |
| Python | 3.11.9 |
| Langfuse SDK | 4.14.4 |
| 입력 | 공개 가능한 합성 Batch 객체·manifest·결과 JSONL |
| 관측 sink | `StructuredLogSink` |
| SDK smoke test | 실제 SDK client, `tracing_enabled=False` |

## 3. 합성 데이터

| 항목 | 값 |
|---|---:|
| Batch | 1건 |
| Generation | 3건 |
| 고유 event | 2건 |
| 최초 요청 | 2건 |
| 재시도 | 1건 |
| 완료 | 3건 |
| 실패 | 0건 |

샘플에는 기사·댓글·prompt·응답 원문이 없으며 식별용 합성 ID와 usage만 포함합니다.

## 4. Token 대조 결과

| 구분 | 문서별 합계 | Batch usage | 결과 |
|---|---:|---:|---|
| 입력 token | 300 | 300 | 일치 |
| 출력 token | 60 | 60 | 일치 |
| 전체 token | 360 | 360 | 일치 |
| cached 입력 token | 20 | 20 | 일치 |
| reasoning 출력 token | 5 | 5 | 일치 |

최종 `reconciliation_status`는 `matched`입니다.

## 5. 비용 계산 결과

아래 단가는 계산 로직 검증용 **합성 가격**이며 실제 OpenAI 모델 가격이 아닙니다.

| usage | 합성 단가 / 1M token |
|---|---:|
| 일반 입력 | $0.50 |
| cached 입력 | $0.25 |
| 출력 | $2.00 |

| custom ID | attempt | 입력 | cached 입력 | 출력 | 비용 |
|---|---:|---:|---:|---:|---:|
| `sample-a-p1-a1` | 1 | 100 | 0 | 20 | $0.000090 |
| `sample-b-p1-a1` | 1 | 120 | 20 | 30 | $0.000115 |
| `sample-a-p1-a2` | 2 | 80 | 0 | 10 | $0.000060 |
| 합계 |  | 300 | 20 | 60 | **$0.000265** |

재시도는 동일 event의 기존 generation을 덮어쓰지 않고 `attempt=2`로 별도 기록했습니다.

## 6. Trace 산출물

[합성 trace JSONL](langfuse-sample-trace.jsonl)은 총 9개 event로 구성됩니다.

| event | 건수 |
|---|---:|
| `llm_batch_trace` | 1 |
| `llm_batch_stage` | 4 |
| `llm_generation` | 3 |
| `llm_usage_reconciliation` | 1 |

전체 key를 자동 검사한 결과 `input`, `output`, `text`, `title`, `url`, `community`, `author` 원문 후보 필드는 0개였습니다. Token 값은 구분이 명확한 `input_tokens`, `output_tokens` 이름으로 기록됩니다.

## 7. 자동 테스트

Langfuse 관측 테스트 9개와 전체 테스트 52개가 통과했습니다.

- Responses API와 Chat Completions usage 필드명 호환
- 잘못된 token 합계와 cached·reasoning 세부값 거부
- cached 입력을 분리한 비용 계산
- 문서별 합계와 Batch usage 일치·불일치 판정
- 실제 Langfuse SDK v4 method 호출 호환
- Langfuse payload에서 `input`·`output` 미설정
- primary sink 실패 시 구조화 로그 fallback
- 예외 메시지 대신 예외 유형만 기록해 secret 노출 방지
- 재시도 attempt별 비용 분리와 metadata-only 로그 검사

## 8. 재현 방법

```bash
python -m pytest -q tests/test_langfuse_observability.py
python -m jobs.verify_langfuse
```

관리형 Langfuse key를 환경 변수로 주입한 뒤 실제 Cloud trace를 전송할 때는 다음 명령을 사용합니다.

```bash
python -m jobs.verify_langfuse --sink langfuse
```

필요한 환경 변수는 `.env.example`에 기록되어 있습니다. 실제 key는 Git에 포함하지 않습니다.

## 9. 남은 검증

- 관리형 일본 리전 프로젝트에서 trace 1건과 generation 3건 UI 확인
- Langfuse API에서 token·비용 합계 조회
- 실제 LLM Batch workflow와 PostgreSQL 결과 적재에 adapter 연결
- Cloud endpoint 차단 상태에서 결과 적재가 계속되는 end-to-end 검증

