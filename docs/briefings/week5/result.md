# Week 5 일별 실행 결과

## 비교 조건

| 항목 | 변경 전 | 변경 후 |
|---|---|---|
| 날짜 | 2016-01-01 | 2016-02-01 |
| 데이터 | Pushshift Reddit comments | 동일 |
| 수집 목표 | 1,000건 | 1,000건 |
| output partitions | 2 | 2 |
| Spark master | `local[2]` | 동일 |

날짜만 변경합니다. GDELT는 HTTPS API 연결 상태를 추가 확인할 때까지 이번 과제 실행 경로에서 제외했습니다.

## 실행 결과

| 날짜 | Param | Reddit 수집 | Spark | 행 회계 | 최종 상태 |
|---|:---:|---:|:---:|---:|:---:|
| 2016-01-01 | 성공 | 1,000건 | 성공 | 1,000 = 1,000 | 성공 |
| 2016-02-01 | 성공 | 1,000건 | 성공 | 1,000 = 1,000 | 성공 |

두 실행 모두 계약 거부와 중복 `event_id`가 0건이었습니다. 품질 규칙 결과는 1월 1일 `accept` 989건·`quarantine` 11건, 2월 1일 `accept` 985건·`flag` 1건·`quarantine` 14건입니다. Spark 실행 시간은 각각 5.790초와 5.644초였습니다.

## 결과 경로

```text
data/airflow-input/
├── reddit-2016-01-01.jsonl
└── reddit-2016-02-01.jsonl

data/airflow-output/
├── reddit-2016-01-01/<run_id>/
└── reddit-2016-02-01/<run_id>/
```

원문 JSONL과 Parquet는 `data/` 규칙에 따라 Git에서 제외하고 집계와 실행 로그만 공개합니다.

## 완료 조건

- 각 날짜에서 사용할 수 있는 이벤트 100건 이상
- 네 task 모두 성공
- `input_rows == accounted_rows`
- 두 실행 사이에 날짜 외 설정이 동일

위 네 조건을 모두 충족했습니다.
