# Scrapy 웹 뉴스 제목 수집 가이드

## 구현 범위

`collectors/web_news/spiders/global_voices_archive.py`가 Global Voices 영어 월별 아카이브에서 제목·URL·게시일을 읽고 `TextEvent v1` JSONL로 내보냅니다. 기본 지원 기간은 Reddit 원본과 맞춘 `2012-01-01`~`2016-02-29`입니다.

```bash
.venv/bin/scrapy crawl global_voices_archive \
  -a start_date=2016-02-01 \
  -a end_date=2016-02-01 \
  -a max_pages_per_month=1 \
  -a max_items=20 \
  -O data/raw/global-voices-2016-02-01.jsonl
```

`keywords`는 쉼표로 구분해 바꿀 수 있습니다. 빈 문자열이면 날짜 범위의 모든 제목을 대상으로 합니다.

```bash
-a keywords=government,election,internet,climate
```

## 코드 책임

| 파일 | 책임 |
|---|---|
| `spiders/global_voices_archive.py` | 월 URL 생성, 카드 추출, 날짜·키워드 필터, 페이지 이동과 1차 URL 중복 제거 |
| `normalization.py` | 원문과 분리된 제목 정규화, 추적 parameter를 제거한 canonical URL 생성 |
| `event_mapper.py` | 수집 값을 공통 `TextEvent v1` 이벤트로 변환하고 안정적인 ID 생성 |
| `settings.py` | robots 준수, 요청 간격·동시성·AutoThrottle·HTTP cache 설정 |

`max_pages_per_month=0`, `max_items=0`은 각각 제한 없음입니다. 개발 검증에서는 반드시 작은 제한을 주고, 전체 기간 수집은 예상 페이지 수와 요청 시간을 확인한 다음 실행합니다.

## 검증과 변경 대응

```bash
PYTHONPATH=. .venv/bin/pytest -q tests/test_web_news_collector.py tests/test_web_news_contract.py
```

테스트는 저장된 축약 HTML fixture만 사용하므로 실제 사이트를 반복 호출하지 않습니다. 실제 소량 검증은 selector나 사이트 정책이 바뀌었을 때 한 페이지로 제한해 수행합니다.

- 제목이나 날짜가 0건이면 먼저 아카이브 HTML selector 변경을 확인합니다.
- `robots.txt`가 차단하면 우회하지 않고 수집을 중단합니다.
- 상세 기사 페이지가 필요해지기 전에는 본문 요청을 추가하지 않습니다.
- 동적 렌더링 문제를 실제로 확인한 뒤에만 Firecrawl 도입을 별도 결정합니다.
