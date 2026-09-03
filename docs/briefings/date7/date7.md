# 6차시 과제 — 부하·복구 보완과 전체 흐름 점검

## 1. 제출 결론

기존 수집·Spark·PostgreSQL 부하 및 복구 실험은 다시 실행하지 않고 공개 가능한
결과 파일을 한 문서에서 추적할 수 있게 정리했다. 이번 보완에서는 다음을 추가했다.

- GPT-5.6 Luna Responses Batch 요청 JSONL과 metadata-only manifest 생성
- Batch 파일 업로드·제출·상태 조회·결과 다운로드 CLI
- 감정·토픽·키워드·요약 응답 JSON Schema와 결과 검증
- LLM label quality gate와 일별 31건 기반 월간 통합 분석
- 검증 결과의 PostgreSQL transaction upsert와 재실행 멱등성 검증
- 제출 전 예상 token·최대 비용 경고와 예산 초과 차단
- Langfuse 장애 시 구조화 로그 fallback 실제 실행
- Airflow 수집→Spark→LLM 통합 DAG와 기본 `submit=false` 보호
- 최신 구성도와 LLM 데이터 모델 migration

2026-09-03에 `OPENAI_API_KEY`, `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`를 로컬
`.env`에 주입했다. 값은 출력하지 않고 존재 여부와 실제 인증만 확인했다. Langfuse Cloud
Japan의 metadata-only sample trace와 OpenAI 합성 Batch 검증을 거쳐, 경제·사회 그룹의
2012년 1월 일별 Batch 31개를 제출했다. 31개 모두 결과 다운로드·Schema 검증·실제 usage
및 비용 대조를 완료했고, 누락·중복·실패 없이 31개 일별 응답을 확보했다. 비정상 label
10개를 quality gate로 제외한 뒤 월간 통합 Batch 1건도 완료·검증했다.
검증 결과 32건은 PostgreSQL에 적재했고 같은 입력을 재실행한 뒤에도 세 LLM 테이블이
각각 32행으로 유지됐다. Airflow에서는 Reddit 100건 수집, Spark 100건 처리, LLM 요청
10건 생성을 하나의 통합 DAG Run으로 실행했다.

### 1.1 과제 요구사항 충족 현황

| 번호 | 과제 요구사항 | 상태 | 완료 근거 또는 남은 내용 |
|---:|---|---|---|
| 1 | 기준·부하 실행의 입력, 시간·처리량, 최종 저장, 오류·미처리 비교 | 완료 | 2장의 Google News·Spark 비교표에 입력·시간·처리량·저장·경고를 기록 |
| 2 | 실패 단계, 재실행 위치와 재실행 후 저장 결과 | 완료 | Spark write 직전 실패와 PostgreSQL 연결 실패를 각각 복구하고 누락·중복을 검증 |
| 3 | fallback 또는 alert의 실제 동작 결과 | 완료 | Langfuse primary 장애를 안전하게 재현해 fallback event 9개를 저장하고, LLM 예산 `critical`·`blocked`를 실행으로 확인 |
| 4 | 최신 구성도와 데이터 모델 | 완료 | MinIO·LLM·Langfuse·Airflow가 반영된 HTML/PNG 구성도, TextEvent v1, PostgreSQL·LLM migration 연결 |
| 5 | Kafka·Spark·저장·Airflow 로그, 단계별 건수와 최종 확인법 | 완료 | 공개 검증 보고서와 단계별 처리 건수, 행 회계·고유 ID·usage 대조 방법 제시 |
| 6 | 아직 실행되지 않는 단계와 남은 작업 | 완료 | 9장에 Google News 검색 세분화, Reddit 추가 가공과 MinIO Streaming checkpoint 전환을 장기 확장으로 명시 |
| 7 | 현재 실행 방법과 확인 결과를 반영한 README | 완료 | 메인 README에 최신 흐름, 구현 상태, 실행·검증 문서 링크 반영 |

따라서 **6차시 과제의 필수 문서 항목 7개와 이번에 선택한 OpenAI·Langfuse 실행 검증은
모두 완료했다.** LLM 결과 32건의 PostgreSQL 멱등 적재와 Airflow의
수집→Spark→LLM 요청 준비 흐름도 실제 로컬 환경에서 검증했다.

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

2026-09-03에 Langfuse Cloud Japan에서 metadata-only sample로 인증과 SDK 연결을 먼저
확인한 뒤, 실제 경제·사회 일별 31건과 월간 1건의 usage를 전송했다. 기사·댓글 원문,
prompt와 응답 본문은 전송하지 않았다.

| 실제 분석 범위 | generation | 입력 token | 출력 token | 비용 | usage 대조 |
|---|---:|---:|---:|---:|---|
| 일별 분석 | 31 | 5,432,661 | 7,423 | $0.5477199 | 31건 `matched` |
| 월간 통합 | 1 | 4,213 | 172 | $0.0005245 | 1건 `matched` |
| 합계 | 32 | 5,436,874 | 7,595 | **$0.5482444** | 32건 일치 |

실제 전송에서 구조화 로그 fallback 파일은 비어 있어 primary 전송 실패가 없었다.

근거: [경제·사회 최종 결과](economy-social-results-01-31.md),
[OpenAI·Langfuse 사전 소량 검증](../../../analysis/reports/openai-langfuse-cloud-smoke-validation.md)

### 4.3 제출 전 LLM 예산 경고 안전장치

실데이터 제출 전 공개 합성 이벤트 2건으로 예산 안전장치만 검증했다. 최대 출력 300 token/건,
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
- 실제 적재·통합 검증: [LLM PostgreSQL·Airflow 검증](../../../analysis/reports/llm-postgres-airflow-integration-validation.md)

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
| Langfuse Cloud | 실제 일별 31건·월간 1건 usage 대조 일치 | [최종 결과](economy-social-results-01-31.md) |
| OpenAI 일별 Batch | Reddit 71,209건·News 633건을 무표본 분석, 31/31 완료 | [실행 기록](economy-social-batch.md) |
| LLM quality gate | 31행 보존, 403개 label 중 비정상 10개 제외 | [최종 결과](economy-social-results-01-31.md#6-출력-품질-점검) |
| OpenAI 월간 Batch | 정제된 일별 31건으로 1건 생성·검증, 실패·누락 0건 | [최종 결과](economy-social-results-01-31.md#7-1월-통합-요약-결과) |
| LLM PostgreSQL | 실제 32건 적재·동일 입력 재실행 후 세 테이블 각 32행 유지 | [통합 검증](../../../analysis/reports/llm-postgres-airflow-integration-validation.md) |
| Airflow 통합 DAG | Reddit 100→Spark 100→LLM 요청 10건 dry-run, 최종 성공 | [통합 검증](../../../analysis/reports/llm-postgres-airflow-integration-validation.md) |
| 경제·사회 1월 결과 | 일별·월간 총비용 $0.5482444, 주요 감성·주제 정리 | [최종 결과](economy-social-results-01-31.md) |

최종 행은 각 Spark report에서 `input = contract_rejected + duplicate + unique`를 확인하고,
PostgreSQL에서는 `count(*)`, `count(distinct event_id)` 및
`stream_batch_commits`를 확인한다. LLM은 manifest 수, Batch `request_counts`, 검증 결과
수와 usage 합계를 대조한다.

## 7. 실제 LLM Batch 실행

### 7.1 확정한 분석 범위

실제로 보낸 분석은 표본 기반 32건 요청안이 아니다. 2012년 1월 경제·사회 범위의 원문을
날짜별로 모두 넣은 일별 요청 31건과, 검증·정제된 일별 응답을 합친 월간 요청 1건이다.

| 항목 | 실제 실행 값 |
|---|---|
| Reddit 범위 | `Economics`, `business`, `news`, `TrueReddit`, `changemyview` |
| Reddit 입력 | 71,209건 |
| Google News 입력 | `economy` 주제 633건 |
| 표본 추출 | 없음 |
| 일별 요청·Batch | 31 requests / 31 independent Batches |
| 월간 요청·Batch | 1 request / 1 Batch |
| 성공한 분석 Batch | 총 32개 |
| 모델·endpoint | `gpt-5.6-luna`, `/v1/responses` |

처음에는 일별 요청 31건을 한 Batch로 묶었으나 조직 queued token 한도 5,000,000을
초과해 validation에서 실패했다. 완료 request와 사용 token은 0이므로 비용도 발생하지
않았다. 이후 날짜별 독립 Batch로 분리하고 queue가 비워지는 순서에 따라 1~15일,
16~21일, 22~31일을 제출했다.

### 7.2 일별 결과 회수와 검증

```bash
python -m jobs.collect_economy_daily_results \
  --artifact-root data/llm/economy-social-2012-01/days \
  --response-root data/llm_response/economy-social/2012/01/days \
  --year 2012 --month 1 --start-day 1 --end-day 31 \
  --combined-output data/llm_response/economy-social/2012/01/daily-results-01-31.validated.jsonl \
  --report data/llm_response/economy-social/2012/01/daily-results-01-31.report.json
```

결과는 출력 순서가 아니라 `custom_id`로 날짜별 manifest와 대조했다. 일별 31건 모두
Schema 검증과 usage reconciliation을 통과했고 누락·중복·실패는 0건이다.

### 7.3 quality gate와 월간 통합

```bash
python -m jobs.quality_gate_llm_results \
  --input data/llm_response/economy-social/2012/01/daily-results-01-31.validated.jsonl \
  --output data/llm_response/economy-social/2012/01/daily-results-01-31.cleaned.jsonl \
  --report data/llm_response/economy-social/2012/01/quality-gate-report.json

python -m jobs.economy_period_batch prepare-monthly \
  --daily-results data/llm_response/economy-social/2012/01/daily-results-01-31.cleaned.jsonl \
  --request-output data/llm/economy-social-2012-01/monthly/requests.jsonl \
  --manifest-output data/llm/economy-social-2012-01/monthly/manifest.jsonl \
  --report data/llm/economy-social-2012-01/monthly/preflight.json \
  --year 2012 --month 1 --budget-usd 0.05
```

quality gate는 31행을 모두 보존하면서 403개 label 중 비정상 10개를 제외했다. 월간
Batch는 1건 완료됐고 Schema·`custom_id`·usage 검증을 모두 통과했다. 실제 일별 비용은
`$0.5477199`, 월간 비용은 `$0.0005245`, 합계는 `$0.5482444`다.

초기의 표본 기반 32 requests/1 Batch 계획은 실제 제출하지 않았다. 해당 사전검사 파일은
의사결정 이력으로만 남기며 현재 실행 결과나 비용에 포함하지 않는다. 상세 실행 명령과
결과는 [Batch 실행 기록](economy-social-batch.md)과
[최종 결과](economy-social-results-01-31.md)를 기준으로 한다.

공식 기준: [GPT-5.6 Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna),
[Batch API](https://developers.openai.com/api/docs/guides/batch)

## 8. Airflow 수집·처리·MinIO·LLM 요청 준비 흐름

`reddit_spark_llm_pipeline`은 다음 9개 task로 수집부터 LLM 요청 검증까지 연결한다.

```text
prepare_parameters
→ collect_reddit_day
→ run_spark
→ verify_spark
→ store_spark_output_in_minio
→ prepare_llm_parameters
→ build_and_budget_check
→ submit_or_dry_run
→ verify_pipeline
```

기존 Run은 Reddit 2016-01-01 100건을 수집해 Spark에서 고유 100건으로 처리하고 그
출력을 MinIO에 2개 객체·128,906 bytes로 저장하고 LLM 요청 10건을 생성했다. 예상 최대
비용은 `$0.0021044`, 예산 상태는 `ok`,
최종 DAG 상태는 `success`였다. 이미 완료된 경제·사회 Batch를 중복 제출하지 않도록
`submit=false`로 검증했으며 외부 API 비용은 발생하지 않았다.

근거: [LLM PostgreSQL·Airflow 통합 검증](../../../analysis/reports/llm-postgres-airflow-integration-validation.md)

이후 MinIO를 기본 저장 backend로 켜고 2016-01-02를 재실행해 raw 100건,
Spark 고유 100건과 LLM 요청 10건이 각각 `news-raw`, `news-processed`, `news-llm`에
자동 게시됨을 확인했다. 최초 `data/` 정식 파일 862개·40,617,977,648 bytes를 전체
복사한 뒤 새 실행 결과까지 포함한 현재 정식 파일은 869개다. 실패는 0건이었고 같은
데이터를 다시 실행했을 때 869개 모두 `unchanged`, 실제 전송량은 0 bytes였다.
실행 report와 log는 `news-reports`로 분리했다. MinIO 전체에는 fixture와 이전 key를
포함해 952개 객체·약 37.83 GiB가 저장되어 있다.

근거: [MinIO 전체 데이터 이전 검증](../../../analysis/reports/minio-data-migration-validation.md)

## 9. 과제 범위 밖 기술 확장

MinIO의 Python·Spark·Airflow 통합과 현재 raw부터 LLM 응답까지의 전체 복사는 완료했다.
장기 확장 범위는 Google News 100건 상한 검색 세분화, Reddit 2012년 2~12월 일별
Parquet 추가 변환과 Structured Streaming checkpoint 전환이다. 제출 현황 표에서는
이 항목들을 제외했다.

## 10. 자동 검증

```bash
python -m pytest -q
docker compose config --quiet
docker compose -f infra/airflow/docker-compose.airflow.yml config --quiet
```

API key가 없는 테스트에서는 외부 OpenAI·Langfuse 호출을 수행하지 않는다. HTTP client는
명시적인 `submit` 명령에서만 생성되고, Airflow DAG도 기본 dry-run으로 보호된다.
