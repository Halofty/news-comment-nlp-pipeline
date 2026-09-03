# LLM PostgreSQL 적재와 Airflow 통합 검증

- 검증일: 2026-09-03 KST
- LLM 데이터: 경제·사회 2012년 1월 일별 31건과 월간 1건
- Airflow DAG: `reddit_spark_llm_pipeline`

## 1. PostgreSQL LLM 결과 적재

`storage/llm_postgres.py`의 transaction upsert adapter와
`jobs/store_llm_results.py`를 사용해 검증·정제된 실제 결과를 저장했다.

```text
llm_batch_jobs       32행 / 고유 batch 32개
llm_batch_requests   32행 / 고유 custom_id 32개
document_analyses    32행 / 고유 event_id·prompt_version 32개
total_cost_usd       $0.54824440
```

동일 입력으로 CLI를 한 번 더 실행한 뒤에도 세 테이블의 행 수와 고유 키 수가 모두
32로 유지됐다. `llm_batch_id`, `custom_id`, `(event_id, prompt_version)`의 upsert 기준이
각 단계의 중복 저장을 방지했다.

```bash
python -m jobs.store_llm_results \
  --artifact-root data/llm/economy-social-2012-01 \
  --response-root data/llm_response/economy-social/2012/01 \
  --daily-results data/llm_response/economy-social/2012/01/daily-results-01-31.cleaned.jsonl \
  --monthly-results data/llm_response/economy-social/2012/01/monthly/result.cleaned.jsonl \
  --report data/llm_response/economy-social/2012/01/postgres-import.json
```

## 2. Airflow 수집·처리·LLM 요청 준비 연결

새 DAG는 다음 단계를 하나의 의존 관계로 연결한다.

```text
prepare_parameters
→ collect_reddit_day
→ run_spark
→ verify_spark
→ prepare_llm_parameters
→ build_and_budget_check
→ submit_or_dry_run
→ verify_pipeline
```

안전한 실제 검증에서는 `submit=false`를 사용해 외부 API 재요청과 중복 비용을
발생시키지 않았다.

| 항목 | 결과 |
|---|---:|
| DAG import 오류 | 0 |
| 수집 입력 | Reddit 2016-01-01, 100건 |
| Spark 처리·저장 | 100건 / 고유 100건 / 오류 0건 |
| Spark 처리 시간 | 5.836초 |
| LLM 요청 생성 | 10건 |
| 예상 입력 token | 3,044 |
| 최대 출력 token | 3,000 |
| 예상 최대 비용 | $0.0021044 |
| 예산 상태 | `ok` |
| 제출 상태 | `dry_run` |
| 최종 DAG 상태 | `success` |

실제 경제·사회 LLM 결과 32건은 이미 OpenAI Batch로 완료했기 때문에 이 통합 검증에서
같은 데이터를 다시 제출하지 않았다. 통합 DAG의 `submit=true` 경로는 기존 Batch
제출 helper를 그대로 사용하며 기본값은 비용 안전을 위해 `false`다.

## 3. 자동 테스트

- LLM PostgreSQL record·upsert 테스트 2개
- Spark 출력→LLM 입력 연결 테스트 1개
- 기존 LLM Airflow dry-run·예산 차단 테스트 2개
- Airflow DAG 목록에서 `reddit_spark_llm_pipeline` 확인
- DAG import 오류 0건
