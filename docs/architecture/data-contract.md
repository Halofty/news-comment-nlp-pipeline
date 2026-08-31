# 텍스트 이벤트 데이터 Contract

- version: 1
- 적용 범위: 웹 뉴스 제목, GDELT 뉴스 제목(기존), Reddit 댓글
- 기계 판독 스키마: [`../sample/schema.json`](../../sample/schema.json)

## 1. 목적

웹 뉴스, 기존 GDELT 뉴스와 Reddit 댓글을 동일한 이벤트 형식으로 Kafka에 전달하고 Spark, PostgreSQL과 LLM Batch 처리 단계가 같은 필드 의미를 사용하도록 데이터 계약을 정의합니다.

## 2. 데이터 출처

### 2.1 GDELT 뉴스

- API: GDELT DOC 2.0 API
- MVP 분석 범위: 기사 제목
- 수집 필드: `url`, `title`, `seendate`, `domain`, `language`, `sourcecountry`
- 제외 필드: `url_mobile`, `socialimage`

GDELT DOC API는 검색 결과의 기사 URL과 메타데이터를 제공합니다. MVP에서는 원문 URL에 추가 요청을 보내지 않으며 `title`을 분석 텍스트로 사용합니다. 향후 기사 전문을 수집할 경우 `collectors/article_fetcher.py`에 구현하고 `metadata.text_scope`로 제목과 전문을 구분합니다.

### 2.2 Reddit 댓글

- 데이터셋: `fddemarco/pushshift-reddit-comments`
- 형식: 월별 Parquet
- 수집 필드: `id`, `body`, `created_utc`, `subreddit`, `score`, `link_id`, `controversiality`
- 제외 필드: `author`, `subreddit_id`

작성자 정보와 사용자 식별값은 공통 이벤트에 포함하지 않습니다.

### 2.3 웹 뉴스

- 최초 대상: Global Voices 영어 월별 아카이브
- 공통 기간: 기존 Reddit 데이터와 맞춘 `2012-01-01`~`2016-02-29`
- MVP 분석 범위: 기사 제목과 게시일, 원문 URL, 언론사 메타데이터
- 수집 방식: 공식 아카이브를 Scrapy로 저속 순회하고 제목을 로컬 키워드로 필터링

검색 결과 페이지를 대량 수집하지 않습니다. 사이트의 `robots.txt`, 이용 조건과 요청 간격을 지키며 기사 본문에는 추가 요청하지 않습니다.

## 3. 공통 이벤트 스키마

모든 최상위 필드는 항상 존재합니다. 출처에 해당하지 않는 필드는 `null`을 사용하고, 출처별 추가 정보는 `metadata`에 저장합니다.

| 필드 | 타입 | 필수 | 설명 |
|---|---|---:|---|
| `event_id` | string | O | 출처와 원본 ID로 생성한 64자리 SHA-256 |
| `source_type` | enum | O | `news` 또는 `comment` |
| `source_name` | enum | O | `web_news`, `gdelt` 또는 `reddit` |
| `event_time` | date-time | O | 윈도우 처리에 사용할 timezone 포함 시각 |
| `collected_at` | date-time | O | Collector가 이벤트를 생성한 UTC 시각 |
| `language` | string | O | 소문자 언어명 또는 `unknown` |
| `title` | string/null | O | 뉴스 제목, 댓글은 `null` |
| `text` | string | O | 비어 있지 않은 분석 대상 텍스트 |
| `url` | URI/null | O | 뉴스 원문 URL, 댓글은 `null` |
| `community` | string/null | O | Reddit 커뮤니티, 뉴스는 `null` |
| `engagement` | integer/null | O | Reddit score, 뉴스는 `null` |
| `schema_version` | integer | O | 현재 계약에서는 `1` |
| `metadata` | object | O | 출처별 추가 필드 |

정의되지 않은 최상위 필드는 허용하지 않습니다.

## 4. GDELT 필드 매핑

| GDELT 필드 | 공통 이벤트 필드 | 변환 규칙 |
|---|---|---|
| `url` | `event_id` | `sha256("gdelt:" + url)` |
| `url` | `url` | 원본 URL |
| `title` | `title` | 앞뒤 공백 제거 |
| `title` | `text` | MVP의 LLM 분석 입력 |
| `seendate` | `event_time` | ISO-8601 UTC 문자열로 변환 |
| `language` | `language` | 소문자로 변환, 없으면 `unknown` |
| `domain` | `metadata.domain` | 원본 값 |
| `sourcecountry` | `metadata.source_country` | 원본 값 |
| 수집 검색어 | `metadata.query` | Collector 실행 시 사용한 검색어 |
| 고정값 | `metadata.text_scope` | MVP에서는 `title_only` |
| 해당 없음 | `community` | `null` |
| 해당 없음 | `engagement` | 측정값이 없으므로 `null` |

`seendate`는 GDELT가 제공한 시각으로 사용하며 원문 사이트의 정확한 게시 시각이라고 단정하지 않습니다.

## 5. Reddit 필드 매핑

| Reddit 필드 | 공통 이벤트 필드 | 변환 규칙 |
|---|---|---|
| `id` | `event_id` | `sha256("reddit:" + id)` |
| `body` | `text` | 앞뒤 공백 제거 |
| `created_utc` | `event_time` | Unix timestamp를 ISO-8601 UTC로 변환 |
| `subreddit` | `community` | 원본 커뮤니티 이름 |
| `score` | `engagement` | 데이터셋에 기록된 정수 값 |
| `link_id` | `metadata.link_id` | 원본 값 |
| `controversiality` | `metadata.controversiality` | 정수 값 |
| 해당 없음 | `title` | `null` |
| 해당 없음 | `url` | `null` |
| 미확정 | `language` | MVP에서는 `unknown` |

Reddit의 `score`는 댓글 작성 시점의 고정값이 아니라 데이터셋 생성 시 기록된 값으로 해석합니다.

## 5.1 웹 뉴스 필드 매핑

| 아카이브 값 | 공통 이벤트 필드 | 변환 규칙 |
|---|---|---|
| canonical URL | `event_id` | `sha256("web_news:" + canonical_url)` |
| 기사 URL | `url` | fragment·추적 query를 제거한 canonical URL |
| 표시 제목 | `title`, `text` | 원문 표기를 보존한 제목 전용 분석 입력 |
| 게시일 | `event_time` | 해당 날짜 UTC 00:00:00; 게시 시각으로 오해하지 않음 |
| 고정값 | `language` | 영어 아카이브 출처 근거로 `en` |
| 언론사 | `metadata.publisher` | 최초 구현은 `Global Voices` |
| 정규화 제목 | `metadata.normalized_title` | HTML entity 해제, NFC, 공백 정규화 |
| 키워드 | `metadata.matched_keywords` | 제목과 일치한 실행 키워드 |
| 수집 페이지 | `metadata.source_page_url` | 발견한 아카이브 페이지 |
| 고정값 | `metadata.text_scope` | `title_only` |

원문 제목과 정규화 제목을 분리합니다. URL이 없는 예외적 확장에 대비한 `publisher|date|normalized_title` 키도 metadata에 남기지만, 현재 이벤트 ID는 canonical URL만 사용합니다.

## 6. 제외 및 품질 규칙

텍스트 길이, Unicode, 반복, URL과 개인정보 후보의 측정·판정 규격은 [`../analysis/quality/text-quality-rules.md`](../../analysis/quality/text-quality-rules.md)에서 관리합니다. 이 문서는 공통 이벤트 계약을, 품질 정책은 계약을 통과한 텍스트의 Spark 정제·격리 상태를 정의합니다.

- GDELT의 `url`, `title`, `seendate` 중 하나라도 없으면 제외합니다.
- Reddit의 `id`, `body`, `created_utc` 중 하나라도 없으면 제외합니다.
- 웹 뉴스의 URL, 제목, 게시일 중 하나라도 없으면 제외합니다.
- 빈 댓글과 `[deleted]`, `[removed]` 댓글은 제외합니다.
- 같은 `event_id`는 동일 이벤트로 취급합니다.
- 같은 제목이라도 URL이 다르면 우선 별도 뉴스 이벤트로 유지합니다.
- 작성자 정보는 읽더라도 출력 이벤트에 복사하지 않습니다.
- 실제 원문 데이터와 생성된 `data/` 파일은 공개 저장소에 커밋하지 않습니다.

## 7. 시간 필드 의미

| 출처 | `event_time` | `collected_at` |
|---|---|---|
| GDELT | API의 `seendate` | Collector 실행 시각 |
| Reddit | 댓글의 `created_utc` | 표본 이벤트 생성 시각 |
| 웹 뉴스 | 아카이브 게시일의 UTC 자정 | Spider 실행 시각 |

모든 시각에는 timezone을 포함하며 출력 시 UTC의 `Z` 표기를 우선 사용합니다.

## 8. 스키마 변경 규칙

- 필드 제거, 타입 변경 또는 기존 의미 변경 시 `schema_version`을 증가시킵니다.
- 새로운 출처별 정보는 우선 `metadata`에 추가합니다.
- 최상위 필드 추가가 필요하면 JSON Schema, Python 검증, Spark schema와 문서를 함께 변경합니다.
- Producer는 알 수 없는 스키마 버전이나 정의되지 않은 최상위 필드를 거부합니다.

## 9. 기사 전문 확장 규칙

기사 전문은 MVP 범위에 포함하지 않습니다. 확장 시 본문 추출 성공 이벤트는 `text`에 정제 본문을 저장하고 `metadata.text_scope=full_text`로 표시합니다. 접근 제한이나 추출 실패 시 제목을 유지하고 `title_only`로 처리합니다.

Firecrawl은 동적 페이지나 추출 실패 사이트를 위한 향후 선택지로만 기록합니다. 현재 의존성에 설치하지 않고 외부 서비스도 호출하지 않습니다.
