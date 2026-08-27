# Airflow 날짜형 과제 검증

- DAG: `reddit_daily_spark_batch`
- 날짜: 2016-01-01, 2016-02-01
- 고정 조건: 유효 댓글 최대 1,000건, 2 partitions
- 흐름: Reddit 일별 수집 → 기존 Spark batch → 행 회계 검증
- 추가 검증: GDELT HTTP 일별 수집 → 동일 Spark batch → 행 회계 검증

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
| `gdelt-http-2026-08-14-v2` | 2026-08-14 | 성공 | 110 | 성공·110행 회계 |
| `gdelt-http-2026-08-15` | 2026-08-15 | 성공 | 109 | 성공·109행 회계 |

GDELT HTTPS API는 이 환경에서 TLS handshake가 실패해 Collector endpoint를 기능이 동일한 HTTP로 변경했습니다. 다만 이번 제출 실행 결과는 외부 API 상태와 무관하게 재현할 수 있도록 Reddit Collector를 날짜형으로 확장해 확보했습니다.

재현 설정은 [Airflow 실행 가이드](../../docs/guides/airflow-automation.md)를 참고합니다.
