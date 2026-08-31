# Global Voices Scrapy 소량 검증

- 실행일: 2026-08-29
- 대상: Global Voices `2016-02` 영어 월별 아카이브
- 실행 범위: `2016-02-01`~`2016-02-29`, 월 최대 1페이지, 출력 최대 5건
- 목적: 전체 수집 전 robots 준수, selector, 키워드 필터와 `TextEvent v1` 변환 확인

## 결과

| 항목 | 결과 |
|---|---:|
| 실제 HTTP 요청 | 2건 (`robots.txt` 1, 아카이브 1) |
| HTTP 200 응답 | 2건 |
| 생성 이벤트 | 5건 |
| `TextEvent v1` Python 검증 | 5건 통과 |
| 이벤트 날짜 | 2016-02-26, 2016-02-28, 2016-02-29 |
| 페이지 추가 순회 | 없음 (`max_pages_per_month=1`) |

실행 시 `ROBOTSTXT_OBEY=True`, 도메인 동시 요청 1개, 기본 요청 간격 10초, AutoThrottle와 HTTP cache가 활성화됐습니다. 실제 제목과 원문 JSONL은 `data/validation/`에 저장되며 Git에는 포함하지 않습니다.

## 재현 명령

```bash
.venv/bin/scrapy crawl global_voices_archive \
  -a start_date=2016-02-01 \
  -a end_date=2016-02-29 \
  -a max_pages_per_month=1 \
  -a max_items=5 \
  -O data/validation/global-voices-smoke.jsonl
```

fixture·계약 단위 테스트 6개도 별도로 통과했습니다. 전체 저장소 테스트는 현재 호스트 가상환경에 기존 `pyspark` 패키지가 없어 Spark 테스트 수집 단계에서 실행되지 않았으며, 웹 뉴스 관련 실패는 아닙니다.
