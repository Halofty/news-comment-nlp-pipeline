# 6차시 과제 — 부하·복구 보완과 전체 흐름 점검

## 1. 제출 결론

기존 수집·Spark·PostgreSQL 부하 및 복구 실험은 다시 실행하지 않고 공개 가능한
결과 파일을 한 문서에서 추적할 수 있게 정리했다. 이번 보완에서는 다음을 추가했다.

- GPT-5.6 Luna Responses Batch 요청 JSONL과 metadata-only manifest 생성
- Batch 파일 업로드·제출·상태 조회·결과 다운로드 CLI
- 감정·토픽·키워드·요약 응답 JSON Schema와 결과 검증
- 제출 전 예상 token·최대 비용 경고와 예산 초과 차단
- Langfuse 장애 시 구조화 로그 fallback 실제 실행
- Airflow `llm_batch_pipeline` 수동 DAG와 기본 `submit=false` 보호
- 최신 구성도와 LLM 데이터 모델 migration

2026-09-03에 `OPENAI_API_KEY`, `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`를 로컬
`.env`에 주입했다. 값은 출력하지 않고 존재 여부와 실제 인증만 확인했다. Langfuse Cloud
Japan의 metadata-only sample trace와 OpenAI 합성 Batch 검증을 거쳐, 경제·사회 그룹의
2012년 1월 일별 Batch 31개를 제출했다. 이 중 1~21일은 결과 다운로드·Schema 검증·실제
usage 및 비용 대조까지 완료했고, 22~31일은 별도 Batch 완료 후 같은 절차로 회수한다.

### 1.1 과제 요구사항 충족 현황

| 번호 | 과제 요구사항 | 상태 | 완료 근거 또는 남은 내용 |
|---:|---|---|---|
| 1 | 기준·부하 실행의 입력, 시간·처리량, 최종 저장, 오류·미처리 비교 | 완료 | 2장의 Google News·Spark 비교표에 입력·시간·처리량·저장·경고를 기록 |
| 2 | 실패 단계, 재실행 위치와 재실행 후 저장 결과 | 완료 | Spark write 직전 실패와 PostgreSQL 연결 실패를 각각 복구하고 누락·중복을 검증 |
| 3 | fallback 또는 alert의 실제 동작 결과 | 완료 | Langfuse primary 장애를 안전하게 재현해 fallback event 9개를 저장하고, LLM 예산 `critical`·`blocked`를 실행으로 확인 |
| 4 | 최신 구성도와 데이터 모델 | 완료 | MinIO·LLM·Langfuse·Airflow가 반영된 HTML/PNG 구성도, TextEvent v1, PostgreSQL·LLM migration 연결 |
| 5 | Kafka·Spark·저장·Airflow 로그, 단계별 건수와 최종 확인법 | 완료 | 6장의 공개 검증 보고서 링크와 단계별 처리 건수, 행 회계·고유 ID·usage 대조 방법 제시 |
| 6 | 아직 실행되지 않는 단계와 남은 작업 | 완료 | 9장에 실제 OpenAI·Langfuse Cloud 실행, MinIO 연동, 전체 DAG 연결 등을 미완료로 명시 |
| 7 | 현재 실행 방법과 확인 결과를 반영한 README | 완료 | 메인 README에 최신 흐름, 구현 상태, 실행·검증 문서 링크 반영 |
| 선택 | BI·대시보드·API·inference 결과 예시 | 해당 없음 | 해당 기능을 추가하지 않았으므로 제출 의무 없음 |

따라서 **6차시 과제의 필수 문서 항목 7개는 모두 갖춘 상태**다. 다만 이번에 추가 목표로
선택한 OpenAI·Langfuse의 외부 서비스 검증까지 모두 끝난 것은 아니다. 아래 항목은 key와
외부 계정 설정이 있어야 완료할 수 있다.

| 추가 구현 항목 | 현재 상태 | 최종 완료 조건 |
|---|---|---|
| GPT-5.6 Luna Batch | 1~21일 결과 검증 완료 | 22~31일 회수 후 31일 통합·월간 요약 실행 |
| Langfuse Cloud | 실제 usage 전송 완료·UI 확인 대기 | 1~21일 실제 token·cost trace 육안 확인 필요 |
| LLM PostgreSQL 적재 | 부분 완료 | 검증 결과 upsert adapter 구현과 동일 결과 재실행 멱등성 확인 |
| 전체 end-to-end Airflow | 부분 완료 | 수집·Spark·LLM DAG를 dataset dependency로 연결해 한 흐름으로 실행 |

## 2. 기준 실행과 부하 실행 비교

### 2.1 Google News 수집 부하

| 지표 | 기준 실행: 2012년 1월 | 부하 실행: 2012년 2~12월 | 변화 |
|---|---:|---:|---:|
| 날짜 | 31일 | 335일 | 10.81배 |
| RSS 요청 | 124 | 1,340 | 10.81배 |
| 응답 항목 | 4,823 | 51,793 | 10.74배 |
| 중복 제거 처리 | 2,558 | 26,639 | 10.41배 |
| 최종 저장 | 2,410 | 26,584 | 11.03배 |
| 실행 시간 | 177.563초 | 1,915.851초 | 10.79배 |
| 최종 저장 처리량 | 13.57건/초 | 13.88건/초 | 유사 |
| 치명적 오류 | 0 | 0 | 변화 없음 |
| 미처리 가능 경고 | 0 | 3 | RSS 100건 상한 |

입력량과 시간이 거의 비례했고 처리량 저하는 관찰되지 않았다. 다만 3개 검색 요청이
100건 상한에 도달해 해당 날짜·검색어에는 결과 누락 가능성이 있다. 이는 프로세스
오류가 아니라 외부 검색 인터페이스의 완전성 한계이며, 검색어 세분화와 날짜별
재개가 남은 보완 작업이다.

### 2.2 Spark 처리 부하

| 지표 | 기준 | 부하 | 확인 |
|---|---:|---:|---|
| 입력 | 100 | 1,000 | 동일 transformation |
| 실행 시간 | 11.005초 | 11.954초 | JVM 시작 비용 비중이 큼 |
| 입력 기준 처리량 | 9.09건/초 | 83.65건/초 | 소규모 측정치 |
| 중복 | 1 | 19 | 모두 설명됨 |
| 고유 저장 | 99 | 981 | 행 회계 일치 |
| 계약 오류 | 0 | 0 | 없음 |
| 미처리 | 0 | 0 | `입력=고유+중복+계약 오류` |

1,000건 결과는 실제 대규모 성능 한계를 뜻하지 않는다. Spark JVM 시작 시간이 포함된
소규모 비교이며, 전체 2012년 처리 전에는 partition·shuffle·sink 처리량을 별도로
측정해야 한다.

## 3. 실패 단계·재실행 위치·복구 결과

### 3.1 Spark 저장 직전 강제 실패

```text
15,063,050건 입력
→ Reddit 21개 subreddit 필터 + Google News 결합
→ 2,935,785건 처리 완료
→ --inject-failure-before-write
→ InjectedProcessingFailure
→ 최종 출력 0건
```

실패 위치는 처리 완료 후 Parquet write 이전이다. 동일 입력에서 장애 옵션만 제거해
처리 task부터 다시 실행했다.

| 검증 | 복구 결과 |
|---|---:|
| 처리 | 2,935,785 |
| 저장 | 2,935,785 |
| 고유 `event_id` | 2,935,785 |
| 누락 | 0 |
| 중복 | 0 |
| 실행 시간 | 28.846초 |

근거: [processing-failure.json](../date6/results/processing-failure.json),
[processing-recovery.json](../date6/results/processing-recovery.json)

### 3.2 PostgreSQL 연결 실패와 중복 재실행

Reddit 100건과 Google News 100건을 사용했다. `127.0.0.1:55432`로 연결해
`OperationalError: Connection refused`를 재현한 뒤 정상 `5432`로 바꾸어 DB 적재
task부터 재실행했다.

| 시점 | 저장 행 | 고유 ID | 누락 | 중복 |
|---|---:|---:|---:|---:|
| 연결 실패 직후 | 0 | 0 | 200 | 0 |
| 정상 연결 복구 | 200 | 200 | 0 | 0 |
| 동일 배치 재실행 | 200 | 200 | 0 | 0 |

`event_id` 기본키와 `ON CONFLICT ... DO UPDATE`로 동일 입력을 다시 실행해도 400건으로
늘지 않았다. 근거: [postgres-recovery.json](../date6/results/postgres-recovery.json)

## 4. fallback과 alert 실제 동작

### 4.1 Langfuse fallback

관측 primary가 모든 method에서 `RuntimeError`를 발생시키도록 안전하게 모의했다.
`FailSafeObservabilitySink`는 예외 본문을 노출하지 않고 `error_type=RuntimeError`만
경고한 뒤 `StructuredLogSink`로 같은 metadata를 저장했다.

| 항목 | 결과 |
|---|---:|
| primary 실패 | batch 1 + stage 4 + generation 3 + reconciliation 1 + flush 1 |
| fallback 관측 event | 9 |
| token 대조 | 입력 300, 출력 60, 합계 360 — 일치 |
| fallback 기준 비용 | $0.0000642 |
| 원문·prompt·응답 저장 | 0 |

실행 명령:

```bash
python -m jobs.verify_langfuse \
  --sink langfuse \
  --simulate-primary-failure \
  --output analysis/reports/langfuse-fallback-trace.jsonl \
  --input-price-per-million 0.10 \
  --cached-input-price-per-million 0.01 \
  --output-price-per-million 0.60
```

근거: [langfuse-fallback-trace.jsonl](../../../analysis/reports/langfuse-fallback-trace.jsonl)

### 4.2 Langfuse Cloud 실제 연결

2026-09-03에 Langfuse Cloud Japan에서 인증을 확인한 뒤 같은 metadata-only sample을
실제로 전송했다. `generation_count=3`, 입력 300·출력 60·전체 360 token이 일치했고
fallback 경고 없이 종료됐다. 기사·댓글 원문, prompt와 응답 본문은 전송하지 않았다.
Cloud UI에서 trace와 비용이 보이는지는 사용자가 마지막으로 확인한다.

근거: [OpenAI·Langfuse 소량 검증](../../../analysis/reports/openai-langfuse-cloud-smoke-validation.md)

### 4.3 LLM 예산 경고

공개 합성 이벤트 2건으로 GPT-5.6 Luna Batch 요청을 만들었다. 최대 출력 300 token/건,
일별 예산 `$0.00045`를 지정했을 때 예상 최대 비용 `$0.0004189`로 예산의 93.09%가 되어
`budget_status=critical`이 실제 기록됐다. 100% 이상이면 `blocked`로 판정하며 Airflow
submit task는 API를 호출하기 전에 실패한다.

| 입력 | 요청 생성 | skip | 예상 입력 token | 최대 출력 token | 상태 |
|---:|---:|---:|---:|---:|---|
| 2 | 2 | 0 | 589 | 600 | `critical` |

근거: [llm-batch-dry-run.json](../../../analysis/reports/llm-batch-dry-run.json)

Airflow에서도 동일 입력을 `$0.00001` 예산으로 실행해 `submit_or_dry_run` task가
실제로 실패하는 것을 확인했다. 예산을 `$0.01`로 바꿔 새 Run을 실행하자 4개 task가
모두 성공했다. 두 실행 모두 `submit=false`라 외부 API 호출은 없었다. 근거는
[Airflow LLM dry-run 검증](../../../analysis/reports/airflow-llm-dry-run-validation.md)에
있다.

## 5. 최신 구성도와 데이터 모델

- 구성도: [HTML](../../architecture/system-architecture.html) ·
  [PNG](../../architecture/system-architecture.png)
- 공통 입력: [TextEvent v1](../../architecture/data-contract.md)
- 저장 모델: [PostgreSQL 저장 구조](../../architecture/storage-schema.md)
- LLM 결과 계약: `llm_analysis/contract.py`
- LLM migration: `sql/migrations/004_llm_analysis.sql`
- migration 검증: [LLM 저장 migration](../../../analysis/reports/llm-storage-migration-validation.md)

LLM 영역의 핵심 키는 다음과 같다.

```text
llm_batch_jobs.llm_batch_id
    1 ── N llm_batch_requests.custom_id
              event_id + attempt
              1 ── 1 document_analyses
                       sentiment · topics · keywords · summary
```

PostgreSQL은 재개·멱등성의 기준이고 Langfuse는 token·비용·지연을 조회하는 관측
복제본이다. Langfuse 장애가 분석 결과를 유실시키지 않도록 책임을 분리했다.

## 6. Kafka·Spark·저장·Airflow 실행 근거

| 단계 | 확인 결과 | 공개 로그·보고서 |
|---|---|---|
| Kafka | 1,000건 발행·소비, malformed 1건 DLQ | [Date 4](../date4.md) |
| Spark batch | 1,000 입력, 19 중복, 981 고유 출력 | [batch 검증](../../../analysis/reports/spark-batch-validation.md) |
| Spark streaming | checkpoint 재시작 0건, 4개 경로 분기 | [streaming 검증](../../../analysis/reports/spark-streaming-consumer-validation.md) |
| PostgreSQL | raw 981, clean 981, rejected 1, 재처리 불변 | [DB 검증](../../../analysis/reports/postgres-integration-validation.md) |
| Airflow | 서로 다른 두 날짜 각각 수집 1,000·Spark 1,000 | [Airflow 검증](../../../analysis/reports/airflow-assignment-validation.md) |
| Airflow LLM | 4개 task 성공, 요청 2건 dry-run | [LLM DAG 검증](../../../analysis/reports/airflow-llm-dry-run-validation.md) |
| LLM dry-run | 요청 2, 비용 critical 경고 | [preflight JSON](../../../analysis/reports/llm-batch-dry-run.json) |
| Langfuse fallback | primary 실패 후 관측 event 9개 보존 | [fallback JSONL](../../../analysis/reports/langfuse-fallback-trace.jsonl) |
| Langfuse Cloud | 일본 리전 인증·metadata-only trace 전송 성공 | [Cloud 소량 검증](../../../analysis/reports/openai-langfuse-cloud-smoke-validation.md) |
| OpenAI Batch | 경제·사회 1~21일 결과 21건 검증, 누락·중복·실패 0건 | [중간 결과](economy-social-results-01-21.md) |
| 2012년 1월 기간 요약 Batch | 기존 32건 표본안은 미제출·대체됨 | `data/llm/period-summary-2012-01/preflight.json` |
| 대주제별 계층형 Batch | 4개 대주제 전체 + AskReddit 비교 표본 비용 산정 완료 | [비용 예측](llm-cost-estimate.md) |
| 경제·사회 일별 Batch | 전체 71,209건 기반 날짜별 독립 Batch 31개 제출 완료·처리 중 | [실행 기록](economy-social-batch.md) |
| 경제·사회 1~21일 결과 | 21/21 검증, 실제 비용 $0.3488443, 주요 주제·품질 문제 정리 | [중간 결과](economy-social-results-01-21.md) |

최종 행은 각 Spark report에서 `input = contract_rejected + duplicate + unique`를 확인하고,
PostgreSQL에서는 `count(*)`, `count(distinct event_id)` 및
`stream_batch_commits`를 확인한다. LLM은 manifest 수, Batch `request_counts`, 검증 결과
수와 usage 합계를 대조한다.

## 7. LLM Batch 실행 방법

### 7.1 비용 없는 사전 검사

```bash
python -m jobs.openai_batch prepare \
  --input sample/synthetic-events.jsonl \
  --request-output data/llm/requests.jsonl \
  --manifest-output data/llm/manifest.jsonl \
  --report data/llm/requests.report.json \
  --model gpt-5.6-luna \
  --limit 100 \
  --daily-budget-usd 1.00
```

### 7.2 실제 제출·상태·다운로드·검증

```bash
export OPENAI_API_KEY='환경에서만 설정'

python -m jobs.openai_batch submit \
  --request-file data/llm/requests.jsonl \
  --preflight-report data/llm/requests.report.json \
  --state-output data/llm/batch-state.json \
  --internal-batch-id manual-001

python -m jobs.openai_batch status \
  --batch-id batch_xxx \
  --state-output data/llm/batch-state.json

python -m jobs.openai_batch download \
  --batch-id batch_xxx \
  --result-output data/llm/results.jsonl \
  --state-output data/llm/batch-state.json

python -m jobs.openai_batch validate \
  --results data/llm/results.jsonl \
  --manifest data/llm/manifest.jsonl \
  --output data/llm/validated-analysis.jsonl \
  --report data/llm/validation-report.json
```

공식 기준: [GPT-5.6 Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna),
[Batch API](https://developers.openai.com/api/reference/resources/batches)

### 7.3 대체된 32건 표본안

`data/experiments/week5/january-recovery`의 2012년 1월 통합 Parquet를 한 번
순회하여 1월 1일부터 31일까지의 일별 요청 31건과 월 통합 요청 1건, 총 32건을
생성했다. 전체 2,935,785행의 건수와 소스 분포는 모두 집계하되 원문 전체를 한
요청에 넣지 않는다. 일별 요청에는 Reddit 댓글과 Web News 제목을 각각 최대 20건,
월 요청에는 각각 최대 100건을 고정 seed reservoir sampling으로 포함한다. 작성자와
URL은 전송 대상에서 제외하며, 삭제 글·빈 글·제어 문자를 제거하고 항목당 1,000자로
제한한다.

| 항목 | 사전 검증 결과 |
|---|---:|
| 전체 입력 행 | 2,935,785 |
| Reddit 행 | 2,933,375 |
| Web News 행 | 2,410 |
| Batch 요청 | 32 |
| 예상 입력 토큰 | 63,061 |
| 최대 출력 토큰 | 9,600 |
| 최대 예상 비용 | $0.0120661 |
| 로컬 비용 한도 | $0.10 |
| 판정 | `ok` |

> 이 요청 파일은 OpenAI에 제출하지 않았다. 이후 확정한 `4개 대주제 + AskReddit`
> 계층형 분석안으로 대체하며, 새 비용 예측은 [별도 문서](llm-cost-estimate.md)를
> 기준으로 한다.

재현 명령:

```bash
python -m jobs.period_summary_batch \
  --input data/experiments/week5/january-recovery \
  --request-output data/llm/period-summary-2012-01/requests.jsonl \
  --manifest-output data/llm/period-summary-2012-01/manifest.jsonl \
  --report data/llm/period-summary-2012-01/preflight.json \
  --year 2012 --month 1 --daily-budget-usd 0.10
```

## 8. Airflow 실행과 확인

`llm_batch_pipeline`은 다음 4개 task로 구성된다.

```text
prepare_parameters
→ build_and_budget_check
→ submit_or_dry_run
→ verify_preflight_and_submission
```

기본 `submit=false`이므로 OpenAI key가 없어도 요청 형식·건수·예산을 확인한다.
실제 제출은 UI의 DAG Run configuration에서 명시적으로 `submit=true`로 바꾸고
Airflow 컨테이너에 `OPENAI_API_KEY`를 전달한 경우에만 수행한다. 의도적 장애 재현은
낮은 `daily_budget_usd`로 제출 전 차단을 확인할 수 있으며 외부 API에는 요청하지 않는다.

## 9. 아직 실행되지 않는 단계와 남은 작업

| 단계 | 현재 상태 | 남은 작업 |
|---|---|---|
| OpenAI Batch 실제 실행 | 경제·사회 1~21일 완료·검증 | 22~31일 회수, 품질 필터와 월간 요약 실행 |
| Langfuse Cloud | 인증·실제 usage 전송 완료 | Cloud UI에서 1~21일 trace·token·cost 육안 확인 |
| LLM PostgreSQL 저장 | migration·결과 검증 완료 | upsert adapter와 재실행 검증 |
| MinIO 데이터 연결 | 서비스·bucket 완료 | fixture upload와 Spark `s3a://` 검증 |
| 전체 Airflow 연결 | 수집·Spark DAG, LLM DAG 각각 구현 | dataset dependency로 end-to-end 연결 |
| Google News 상한 | 경고 3건 기록 | 검색 조건 세분화·resume 수집 |
| Reddit 2012 가공 | 12개월 원본 다운로드 완료 | 21개 subreddit 일별 Parquet 변환 |
| BI·API·inference 화면 | 추가하지 않음 | 이번 제출 대상 아님 |

## 10. 사용자가 직접 해야 하는 외부 서비스 설정

실제 외부 실행에는 코드로 대신할 수 없는 계정 설정이 남아 있다. 상세한 화면 경로,
환경변수, 검증 명령과 secret 점검은
[OpenAI API와 Langfuse Cloud 설정 가이드](openai-langfuse-setup.md)에
정리했다.

| 순서 | 사용자가 할 일 | 확인 상태 | 완료 기준 |
|---:|---|---|---|
| 1 | OpenAI API 전용 프로젝트 생성 | 완료 | `news-comment-nlp-pipeline` 프로젝트 선택 가능 |
| 2 | API Billing·GPT-5.6 Luna 사용 가능 여부 확인 | 완료 | 실제 Batch 접수로 model·결제 상태 확인 |
| 3 | 프로젝트 budget·usage notification 설정 | 사용자 확인 필요 | 대시보드 경고 + 로컬 예산 차단 이중화 |
| 4 | 프로젝트 범위 OpenAI key 발급 | 완료 | `.env` 존재 확인, 값 미출력 |
| 5 | Langfuse Cloud Japan 프로젝트 생성 | 완료 | 일본 리전 인증 성공 |
| 6 | Langfuse 프로젝트 key pair 발급 | 완료 | `.env` 존재·Cloud 인증 확인, 값 미출력 |
| 7 | Airflow 컨테이너 다시 생성 | 완료 | `credentials-configured` 확인 |
| 8 | Langfuse sample trace 전송 | 전송 완료·UI 확인 필요 | 3 generations, 300/60/360 token 확인 |
| 9 | Airflow 2건 dry-run | 완료 | `submit=false`, 4개 task 성공 |
| 10 | OpenAI 실제 Batch 제출 | 1~21일 완료·22~31일 회수 대기 | 31일 결과·usage·날짜 연속성 최종 대조 |

최초 실제 제출 권장 configuration:

```json
{
  "input_path": "sample/synthetic-events.jsonl",
  "output_root": "data/airflow-output/llm-batch",
  "model": "gpt-5.6-luna",
  "limit": 2,
  "daily_budget_usd": "0.01",
  "submit": false
}
```

실제 2건은 CLI에서 이미 한 번 제출했으므로 Airflow에서 같은 요청을 `submit=true`로
다시 실행하지 않는다. 남은 사용자 확인은 OpenAI dashboard budget·notification,
Langfuse UI trace·token·cost, Batch 완료 후 결과·usage다.

## 11. 자동 검증

```bash
python -m pytest -q
docker compose config --quiet
docker compose -f infra/airflow/docker-compose.airflow.yml config --quiet
```

API key가 없는 테스트에서는 외부 OpenAI·Langfuse 호출을 수행하지 않는다. HTTP client는
명시적인 `submit` 명령에서만 생성되고, Airflow DAG도 기본 dry-run으로 보호된다.
