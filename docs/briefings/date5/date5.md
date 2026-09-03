# Week 5 — 날짜별 Reddit·GDELT Spark 자동화

## 목표

Airflow 실행 시 하루를 입력받아 Reddit 댓글 또는 GDELT 뉴스 제목을 수집하고 기존 Spark 처리까지 자동 실행합니다. 각 소스에서 날짜만 바꾼 실행 결과를 남깁니다.

```text
날짜 Param
├─ Reddit 월별 데이터 스트리밍·UTC 날짜 필터링
└─ GDELT HTTP API 뉴스 제목 수집
→ TextEvent v1 JSONL
→ 기존 Spark batch
→ 행 회계 검증
```

## 비교 날짜

| 구분 | 날짜(UTC) | 원본 월 파일 | 수집 목표 |
|---|---|---|---:|
| 변경 전 | 2016-01-01 | `RC_2016-01.parquet` | 1,000건 |
| 변경 후 | 2016-02-01 | `RC_2016-02.parquet` | 1,000건 |

각 월 파일의 첫날을 선택해 월 전체를 내려받지 않고 streaming으로 필요한 수량만 읽습니다. 두 실행은 날짜 이외의 설정이 같습니다.

| 구분 | 날짜(UTC) | 검색어 | 유효 수집 |
|---|---|---|---:|
| GDELT 변경 전 | 2026-08-14 | `artificial intelligence` | 110건 |
| GDELT 변경 후 | 2026-08-15 | `artificial intelligence` | 109건 |

GDELT도 날짜 이외의 설정은 동일합니다. 두 데이터 소스의 제공 기간이 달라 Reddit 날짜와 GDELT 날짜를 직접 비교하지는 않습니다.

## DAG

DAG ID는 `reddit_daily_spark_batch`와 `gdelt_daily_spark_batch`입니다. 두 DAG 모두 준비·수집·Spark·검증의 같은 네 단계로 구성됩니다.

```text
prepare_parameters
→ collect_reddit_day 또는 collect_gdelt_day
→ run_existing_spark_job
→ verify_row_accounting
```

| task | 역할 |
|---|---|
| `prepare_parameters` | 날짜 형식과 단일 날짜 범위 검증 |
| `collect_reddit_day` | Reddit 댓글을 날짜로 필터링해 저장 |
| `collect_gdelt_day` | GDELT 제목을 HTTP로 수집해 최소 100건 확인 |
| `run_existing_spark_job` | 기존 Spark batch 실행 |
| `verify_row_accounting` | 입력 행 전체가 처리 결과로 설명되는지 확인 |

## 수행 결과

- 날짜를 입력받는 DAG 구현
- 하루 범위 및 최소 수집량 검증
- Reddit Collector와 Spark 연결
- HTTP GDELT Collector와 Spark 연결
- 두 날짜 실제 실행
- 실행 로그와 결과 집계

## 관련 문서

- [결과 비교](result.md)
- [2016-01-01 실행 로그](log_date_2016-01-01.txt)
- [2016-02-01 실행 로그](log_date_2016-02-01.txt)
- [GDELT 2026-08-14 실행 로그](log_gdelt_2026-08-14.txt)
- [GDELT 2026-08-15 실행 로그](log_gdelt_2026-08-15.txt)
- [실행 가이드](../../guides/airflow-automation.md)
- [검증 보고서](../../../analysis/reports/airflow-assignment-validation.md)
- [`dags/reddit_daily_spark_batch.py`](../../../dags/reddit_daily_spark_batch.py)
- [`dags/gdelt_daily_spark_batch.py`](../../../dags/gdelt_daily_spark_batch.py)
