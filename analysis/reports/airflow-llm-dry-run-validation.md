# Airflow GPT-5.6 Luna Batch dry-run 검증

- 실행일: 2026-09-02 KST
- DAG: `llm_batch_pipeline`
- Run ID: `llm-final-dry-run-2026-09-02`
- 실제 OpenAI 제출: `false`
- 최종 상태: `success`
- 실행 시간: 4.137초

## Task 상태

| Task | 상태 | 시작(UTC) | 종료(UTC) |
|---|---|---|---|
| `prepare_parameters` | success | 17:46:51.847 | 17:46:51.985 |
| `build_and_budget_check` | success | 17:46:52.876 | 17:46:53.031 |
| `submit_or_dry_run` | success | 17:46:53.914 | 17:46:54.025 |
| `verify_preflight_and_submission` | success | 17:46:54.950 | 17:46:55.073 |

## 사전 검사 결과

| 지표 | 결과 |
|---|---:|
| 입력 이벤트 | 2 |
| Batch 요청 | 2 |
| 제외 | 0 |
| 예상 입력 token | 589 |
| 최대 출력 token | 600 |
| 예상 최대 비용 | $0.0003778 |
| 일별 예산 | $0.01 |
| 예산 상태 | `ok` |

생성된 비공개 실행 산출물은 `data/airflow-output/llm-batch/llm-final-dry-run-2026-09-02/`의
`requests.jsonl`, `manifest.jsonl`, `preflight.json`입니다. `data/`는 Git에서 제외되므로
공개 저장소에는 이 집계 보고서와 원문이 합성 데이터인 `sample/` 요청 예시만 둡니다.

Airflow DAG import 오류는 0건이었고 기존 `gdelt_daily_spark_batch`,
`reddit_daily_spark_batch`와 새 `llm_batch_pipeline`이 함께 조회됐습니다.

## 예산 차단과 복구

동일한 합성 입력 2건에서 일별 예산만 변경해 두 번 더 실행했습니다. 두 실행 모두
`submit=false`이므로 외부 OpenAI 요청은 없었습니다.

| Run ID | 일별 예산 | preflight | submit task | DAG 결과 |
|---|---:|---|---|---|
| `llm-budget-blocked-2026-09-02` | $0.00001 | `blocked` | failed | failed |
| `llm-budget-recovered-2026-09-02` | $0.01 | `ok` | success(dry-run) | success |

차단 실행은 요청 파일을 만든 뒤 `submit_or_dry_run`에서 중단됐고 검증 task는
`upstream_failed`였습니다. 예산을 정상값으로 변경해 같은 위치부터 새 DAG Run으로
재실행하자 전체 task가 성공했습니다. 따라서 alert가 단순 문서가 아니라 API 호출 전
실제 제어 조건으로 동작함을 확인했습니다.
