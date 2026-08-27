# Airflow 날짜형 과제 검증

- DAG: `reddit_daily_spark_batch`
- 날짜: 2016-01-01, 2016-02-01
- 고정 조건: 유효 댓글 최대 1,000건, 2 partitions
- 흐름: Reddit 일별 수집 → 기존 Spark batch → 행 회계 검증

## 자동 검사

| 항목 | 결과 |
|---|---|
| DAG import | 오류 0건 |
| Collector·Airflow helper 테스트 | 15개 통과 |
| 단일 날짜 범위 검사 | 구현 완료 |
| 최소 100건 검사 | 구현 완료 |
| Docker Compose | healthy |

## 실제 실행

| run ID | 날짜 | 상태 | 수집 | Spark |
|---|---|---|---:|---|
| `reddit-date-2016-01-01-v2` | 2016-01-01 | 성공 | 1,000 | 성공·1,000행 회계 |
| `reddit-date-2016-02-01` | 2016-02-01 | 성공 | 1,000 | 성공·1,000행 회계 |

GDELT HTTPS API 연결 실패는 DAG나 날짜 검증 문제가 아니었지만 외부 상태에 의존하므로, 이번 제출 실행은 기존 Reddit Collector를 날짜형으로 확장해 재현합니다.

재현 설정은 [Airflow 실행 가이드](../../docs/guides/airflow-automation.md)를 참고합니다.
