# GDELT 뉴스 데이터셋 명세

- 카탈로그 ID: `gdelt-doc-news`
- 제공자: The GDELT Project
- 접근 방식: GDELT DOC 2.0 API
- 공식 API 설명: <https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/>
- 이용 조건: <https://www.gdeltproject.org/about.html#termsofuse>
- 확인 기준일: 2026-08-23
- 공통 계약: [`../../docs/data-contract.md`](../../docs/data-contract.md)
- 표본 profile: [`../reports/gdelt-sample-profile.json`](../reports/gdelt-sample-profile.json)

## 프로젝트 사용 범위

GDELT DOC API의 `ArtList` JSON 응답에서 뉴스 URL, 제목과 관측 메타데이터를 수집합니다. MVP에서는 기사 원문에 추가 요청을 보내지 않고 제목만 분석 텍스트로 사용합니다.

| 항목 | 값 |
|---|---|
| 검색 단위 | Collector 실행 시 지정한 검색어와 선택적 UTC 시작·종료 시각 |
| 요청 상한 | 현재 Collector 기준 요청당 250건 |
| MVP 텍스트 | `title` |
| `text_scope` | `title_only` |
| 원문 저장 | Git 제외 경로인 `data/`에만 허용 |
| 기사 전문 | 현재 범위에서 제외 |

API는 검색 결과의 일부이며 전체 뉴스 모집단이나 특정 국가·언어의 완전한 표본으로 해석하지 않습니다. `seendate`도 원문 사이트의 정확한 최초 게시 시각이 아니라 GDELT가 제공한 관측 시각으로 취급합니다.

## 이용 조건과 저작권 경계

GDELT 공식 Terms of Use는 GDELT가 공개한 데이터셋의 학술·상업·정부 목적 사용과 재배포를 허용하며, 사용 또는 재배포 시 GDELT Project 인용과 공식 사이트 링크를 요구합니다.

이 허용 범위를 각 뉴스 발행사의 기사 본문 저작권까지 확장해 해석하지 않습니다. 현재 프로젝트가 기사 제목과 메타데이터만 사용하는 이유이며, 기사 전문 수집은 사이트별 이용약관·robots 정책·저작권을 별도로 검토한 뒤 구현합니다.

## 원본 필드와 매핑

| 원본 필드 | 사용 | `TextEvent v1` | 처리 |
|---|:---:|---|---|
| `url` | O | `url`, `event_id` | URL을 보존하고 `sha256("gdelt:" + url)`로 ID 생성 |
| `title` | O | `title`, `text` | 앞뒤 공백 제거, MVP 분석 텍스트로 사용 |
| `seendate` | O | `event_time` | timezone이 있는 UTC ISO-8601로 변환 |
| `language` | O | `language` | 소문자 변환, 결측 시 `unknown` |
| `domain` | O | `metadata.domain` | 응답 값을 보존 |
| `sourcecountry` | O | `metadata.source_country` | 응답 값을 보존 |
| `url_mobile` | X | 없음 | MVP에서 제외 |
| `socialimage` | X | 없음 | 이미지 수집·분석 범위가 아니므로 제외 |

`url`, `title`, `seendate` 중 하나라도 비어 있으면 이벤트를 만들지 않습니다.

## 수집 명령

```bash
python3 -m collectors.gdelt \
  --query "climate change" \
  --max-records 100 \
  --output data/validation/gdelt-climate-change-100.jsonl
```

정확한 기간을 재현해야 할 때는 `--start YYYYMMDDHHMMSS`와 `--end YYYYMMDDHHMMSS`를 함께 기록합니다. 기간을 생략한 동적 API 결과는 이후 같은 검색어로도 동일하게 재현되지 않을 수 있습니다.

## 알려진 품질과 운영 문제

- 공유 IP rate limit으로 2026-08-20의 100건 검증을 완료하지 못했습니다.
- HTTP 성공 코드와 함께 JSON이 아닌 rate-limit 안내문이 반환될 수 있습니다.
- URL이 다른 동일·유사 기사가 중복으로 남을 수 있습니다.
- 언어와 출처 국가 값은 GDELT 분류 결과이며 별도 정답 검증 전에는 추정값으로 취급합니다.
- 검색어, 시간 범위와 API 정렬 방식에 따라 선택 편향이 생깁니다.

현재 검증 상태는 `blocked`입니다. 성공 표본이 없으므로 건수·언어·도메인 분포를 임의로 작성하지 않습니다.

