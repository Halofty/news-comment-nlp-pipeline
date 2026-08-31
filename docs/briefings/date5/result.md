# Week 5 Reddit·GDELT 일별 실행 결과

## Reddit 일일 전체 수집 추가 검증

기존 과제 실행은 날짜별 1,000건으로 제한했지만, 이후 `limit=0`을 무제한으로 정의하고 같은 두 날짜를 전체 수집했습니다. 아래 결과는 **수집 완료 기준**이며 전체 데이터를 Spark로 처리한 결과는 아직 아닙니다.

| 날짜 | 원본 날짜 필터 행 | 최종 `TextEvent v1` | JSONL 크기 | 수집 상태 |
|---|---:|---:|---:|:---:|
| 2016-01-01 | 1,452,563 | 1,452,563 | 약 812MB | 완료 |
| 2016-02-01 | 1,915,934 | 1,915,934 | 약 1.2GB | 완료 |
| 합계 | 3,368,497 | 3,368,497 | 약 2.0GB | 완료 |

월별 원본 Parquet는 각각 약 9.80GB와 9.46GB이며 5천만 행을 넘습니다. footer 통계를 검사한 결과 `created_utc` row-group 최소·최대가 두 파일 모두 단조 증가했습니다. 필요한 날짜의 row group 앞부분과 footer만 가진 sparse Parquet를 구성하고 PyArrow predicate pushdown으로 정확한 하루 범위를 읽었습니다. 이벤트는 JSONL에 기록되기 전에 Python `TextEvent v1` 계약 검증을 통과합니다.

```bash
.venv/bin/python -m collectors.reddit \
  --month 2016-01 \
  --input-parquet data/raw/reddit-parquet/RC_2016-01.sparse.parquet \
  --start-date 2016-01-01 \
  --end-date 2016-01-01 \
  --limit 0 \
  --output data/airflow-input/reddit-2016-01-01.jsonl
```

`limit=0`은 선택 날짜의 모든 유효 행을 뜻합니다. 양수 제한의 기존 동작은 유지합니다.

## 기존 1,000건 Airflow·Spark 과제 비교 조건

| 항목 | 변경 전 | 변경 후 |
|---|---|---|
| 날짜 | 2016-01-01 | 2016-02-01 |
| 데이터 | Pushshift Reddit comments | 동일 |
| 수집 목표 | 1,000건 | 1,000건 |
| output partitions | 2 | 2 |
| Spark master | `local[2]` | 동일 |

Reddit의 두 실행에서는 날짜만 변경합니다. GDELT도 별도 절에서 같은 방식으로 날짜만 변경해 검증합니다.

## Reddit 실행 결과

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

## GDELT 실행 결과

GDELT는 HTTPS TLS 오류를 피하기 위해 기능이 같은 HTTP endpoint를 사용했습니다. 필수 필드가 없는 API 항목이 정규화 과정에서 제외될 수 있어 목표 100건보다 10건 더 요청했습니다.

| 날짜 | 검색어 | API 요청 | 유효 입력 | Spark 유효 행 | 행 회계 | 최종 상태 |
|---|---|---:|---:|---:|---:|:---:|
| 2026-08-14 | `artificial intelligence` | 110 | 110 | 110 | 110 = 110 | 성공 |
| 2026-08-15 | `artificial intelligence` | 110 | 109 | 109 | 109 = 109 | 성공 |

두 실행 모두 계약 거부와 중복 `event_id`가 0건입니다. 각 날짜에서 zero-width 문자 제목 1건씩이 `flag` 처리됐으며, 나머지 109건과 108건은 `accept` 처리됐습니다. Spark 실행 시간은 각각 5.321초와 5.331초였습니다.

## 전체 결과 요약

| 소스 | 실행 횟수 | 총 입력 | 계약 거부 | 중복 | 행 회계 | 결과 |
|---|---:|---:|---:|---:|:---:|:---:|
| Reddit 댓글 | 2 | 2,000 | 0 | 0 | 일치 | 성공 |
| GDELT 뉴스 제목 | 2 | 219 | 0 | 0 | 일치 | 성공 |
| 합계 | 4 | 2,219 | 0 | 0 | 일치 | 성공 |

Reddit과 GDELT의 제공 기간이 다르므로 두 소스의 날짜를 직접 비교하지 않습니다. 이번 검증의 목적은 각 소스에서 날짜 parameter를 바꿔 동일한 `TextEvent v1 → Spark → 행 회계` 흐름을 재실행할 수 있는지 확인하는 것입니다.
