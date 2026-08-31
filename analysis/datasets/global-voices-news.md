# Global Voices 영어 뉴스 제목 데이터 명세

## 출처와 목적

- 제공자: Global Voices
- 접근 경로: `https://globalvoices.org/YYYY/MM/` 월별 영어 아카이브
- 공통 분석 기간: `2012-01-01`~`2016-02-29`
- 수집 단위: 기사 제목, 게시일, canonical URL과 언론사 메타데이터
- 분석 목적: 같은 기간의 Reddit 댓글과 영어 토픽의 시간 변화를 비교

GDELT DOC API의 조회 기간이 Reddit 데이터와 맞지 않아 과거 기사 제목 수집 경로를 언론사 공식 아카이브 기반으로 바꿨습니다. Google News나 일반 검색은 후보 발견에만 사용할 수 있으며, 본 수집기는 검색 결과를 크롤링하지 않습니다.

## 수집·선별 정책

초기 수집기는 Global Voices 영어 월별 아카이브를 사용합니다. `robots.txt`를 따르고 도메인 동시 요청을 1개로 제한하며 기본 요청 간격은 10초입니다. HTTP cache를 사용해 개발·재실행 중 같은 페이지에 반복 요청하는 일을 줄입니다.

기본 키워드는 다음 네 묶음의 범용어입니다.

- 정치: `government`, `election`, `president`, `parliament`
- 경제: `economy`, `economic`, `market`, `business`
- 기술: `technology`, `internet`, `digital`
- 환경: `climate`, `energy`, `environment`

키워드는 확정 토픽 라벨이 아니라 수집 후보를 넓게 제한하는 조건입니다. 실제 분석 토픽은 후속 데이터 탐색·모델링 과정에서 정합니다.

## `TextEvent v1` 매핑과 품질

- `source_name=web_news`, `source_type=news`, `language=en`
- 원문 제목은 `title`과 `text`에 보존합니다.
- entity·Unicode·공백을 정리한 값은 `metadata.normalized_title`에 별도로 둡니다.
- canonical URL로 안정적인 `event_id`를 만들고 같은 페이지 내 반복 카드를 제거합니다.
- 게시일만 제공되므로 `event_time`의 시각은 UTC 자정이며 실제 게시 시각을 뜻하지 않습니다.
- URL·제목·게시일이 없거나 기간 밖이거나 키워드가 맞지 않는 항목은 출력하지 않습니다.

## 권리와 확장 제한

현재는 제목과 최소 메타데이터만 수집하며 기사 본문, 이미지, 작성자 프로필을 저장하지 않습니다. 공개·배포 전에는 Global Voices의 현재 이용 조건과 Creative Commons 표시 요건을 다시 확인해야 합니다. 사이트 구조나 `robots.txt`가 바뀌면 Spider를 중단하고 selector·허용 범위를 재검토합니다.

Firecrawl은 JavaScript 렌더링이나 추출 실패가 실제로 확인될 때 검토할 미래 대안입니다. 현재 프로젝트에는 설치하거나 호출하지 않습니다.
