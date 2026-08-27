# Week 5 — 날짜별 Reddit·Spark 자동화

## 목표

Airflow 실행 시 하루를 입력받아 공개 Reddit 댓글 데이터에서 해당 날짜의 댓글을 수집하고 기존 Spark 처리까지 자동 실행합니다. 두 실행은 날짜만 변경합니다.

```text
날짜 Param
→ 월별 Reddit 데이터 스트리밍
→ 해당 UTC 날짜 필터링
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

## DAG

DAG ID는 `reddit_daily_spark_batch`입니다.

```text
prepare_parameters
→ collect_reddit_day
→ run_existing_spark_job
→ verify_row_accounting
```

| task | 역할 |
|---|---|
| `prepare_parameters` | 날짜 형식·단일 날짜 범위를 검증하고 월 파일 결정 |
| `collect_reddit_day` | Reddit 댓글을 날짜로 필터링해 최소 100건 저장 |
| `run_existing_spark_job` | 기존 Spark batch 실행 |
| `verify_row_accounting` | 입력 행 전체가 처리 결과로 설명되는지 확인 |

## 제출 체크리스트

- [x] 날짜를 입력받는 DAG
- [x] 하루 범위 및 최소 수집량 검증
- [x] Reddit Collector와 Spark 연결
- [x] 두 날짜 실제 실행
- [x] 실행 로그와 결과 집계
- [ ] 성공 화면 캡처와 GitHub 링크 제출

## 관련 문서

- [결과 비교](result.md)
- [2016-01-01 실행 로그](log_date_2016-01-01.txt)
- [2016-02-01 실행 로그](log_date_2016-02-01.txt)
- [실행 가이드](../../guides/airflow-automation.md)
- [검증 보고서](../../../analysis/reports/airflow-assignment-validation.md)
- [`dags/reddit_daily_spark_batch.py`](../../../dags/reddit_daily_spark_batch.py)
