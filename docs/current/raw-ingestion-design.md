# Current raw ingestion 설계

## 1. 결정 사항

2026 current 파이프라인의 MVP는 Google News `economy` 검색과 경제·사회 Reddit
4개(`Economics`, `business`, `TrueReddit`, `changemyview`)를 당일 여러 번 함께
수집한다.
날짜가 바뀐 직후 마지막 catch-up 수집을 수행하고, 전날 `event_time` partition을 확정한
다음 기존 Kafka → Spark → MinIO → LLM → PostgreSQL 경로를 실행한다.

초기 기준 timezone은 `Asia/Seoul`이다. 원본의 시각은 timezone을 보존해 UTC로
정규화하고, 일별 분석 partition만 KST 달력 날짜로 계산한다.

## 2. Airflow 실행 구성

### Raw ingestion DAG

| 항목 | 값 |
|---|---|
| DAG ID | `current_raw_ingestion` |
| schedule | `5,35 * * * *` |
| timezone | `Asia/Seoul` |
| 대상 | Google News economy와 경제·사회 subreddit 4개 |
| 처리 범위 | source별 마지막 성공 cursor 이후부터 현재 실행 경계 전까지 |
| 결과 | immutable raw snapshot, TextEvent staging, collection manifest, cursor |
| LLM 실행 | 하지 않음 |

30분마다 실행하되 실패한 interval은 cursor를 전진시키지 않는다. 다음 성공 실행이 같은
구간부터 다시 수집하므로 중간 실패가 데이터 공백으로 바뀌지 않게 한다.

### Daily finalization DAG

| 항목 | 값 |
|---|---|
| DAG ID | `current_daily_pipeline` |
| schedule | `10 0 * * *` |
| timezone | `Asia/Seoul` |
| 자동 실행 대상 | `data_interval_end` 기준 전날 KST 날짜 |
| 수동 실행 대상 | 사용자가 입력한 `start_date`~`end_date` |
| 처리 방식 | 범위를 날짜별 mapped task로 확장 |

00:10 실행은 다음 순서를 따른다.

1. 전날 00:00부터 당일 00:00 KST까지 source별 cursor가 도달했는지 확인한다.
2. 도달하지 않았다면 00:00 경계까지만 마지막 catch-up 수집을 실행한다.
3. 전날 partition의 manifest를 `finalized`로 기록한다.
4. finalized partition을 기존 Kafka bounded batch 이후 경로로 처리한다.

00:10은 “날짜 변경 직후” 처리하면서 00:00에 raw ingestion과 finalization이 동시에
경쟁하지 않게 하기 위한 초기값이다. 실제 수집 지연 지표를 확인한 뒤 조정한다.

## 3. 자동·수동 날짜 결정

날짜 결정은 다음 우선순위를 사용한다.

| Run 종류 | start/end가 입력됨 | 대상 |
|---|:---:|---|
| Scheduled | 무관 | Airflow interval 기준 전날 1일 |
| Manual | 예 | 입력한 inclusive 범위 |
| Manual | 아니오 | 실행 시각 기준 전날 1일 |

Scheduled Run에서 wall clock의 `date.today()`를 직접 사용하지 않는다. Airflow의
`data_interval_end`를 KST로 변환해 재실행과 backfill에서도 같은 날짜가 계산되게 한다.

## 4. raw 저장 구조

로컬 경로와 MinIO object key는 같은 partition 의미를 사용한다.

```text
raw/current/
├── google-news/
│   └── collected_date=2026-09-23/
│       └── window_start=2026-09-23T143500+0900/
│           ├── response.xml
│           ├── events.jsonl
│           └── manifest.json
└── community/
    └── provider=reddit/
        └── collected_date=2026-09-23/
            └── window_start=2026-09-23T143500+0900/
                ├── response.jsonl
                ├── events.jsonl
                └── manifest.json
```

`collected_date`는 수집 작업의 날짜이고 분석 partition은 `event_time`을 KST로 변환해
별도로 만든다. 자정 경계를 지난 API 응답 한 건에 전날과 당일 이벤트가 함께 있어도
올바른 분석 날짜로 나뉘어야 한다.

## 5. manifest와 cursor

각 수집 window는 최소한 다음 정보를 남긴다.

```json
{
  "source": "google_news",
  "window_start": "2026-09-23T14:35:00+09:00",
  "window_end": "2026-09-23T15:05:00+09:00",
  "requested": 4,
  "received": 187,
  "normalized": 181,
  "duplicate_in_window": 6,
  "rejected": 0,
  "cursor_before": "...",
  "cursor_after": "...",
  "status": "complete"
}
```

- cursor는 source별로 분리한다.
- raw와 manifest 업로드가 모두 성공한 뒤에만 cursor를 갱신한다.
- 같은 window 재실행은 안정적인 event ID와 object key로 멱등 처리한다.
- API가 cursor를 제공하지 않으면 시간 overlap을 두고 재조회한 뒤 event ID로 dedup한다.

## 6. Google News 수집 규칙

- MVP에서는 `economy` 대주제만 같은 30분 ingestion Run에서 수집한다.
- URL을 기본 식별자로 사용하고 제목·publisher·게시 시각도 함께 보존한다.
- 요청별 반환 건수와 100건 상한 도달 여부를 manifest에 기록한다.
- RSS pubDate가 없거나 파싱되지 않는 항목은 원본에 남기고 정규화 실패로 집계한다.
- RSS 검색은 완전한 뉴스 원장이 아니므로 `complete`는 검색 결과 전체 확보가 아니라
  정의한 수집 window가 오류 없이 실행되었다는 뜻이다.

## 7. Reddit current 수집 규칙

기존 `config/analysis-groups.yaml`의 경제·사회 그룹 중 `Economics`, `business`,
`TrueReddit`, `changemyview`를 대상으로 한다. 범용 뉴스 커뮤니티인 `r/news`는
Google News와의 역할 중복을 줄이기 위해 MVP에서 제외한다. archive 파일이 아니라
Reddit이 허용한 공식 API 경로와 인증을 사용한다.

- pagination을 끝까지 수행하거나 rate limit으로 중단된 위치를 cursor로 남긴다.
- 작성자 식별자는 TextEvent와 LLM 입력에 포함하지 않는다.
- 삭제·제거 표시, 빈 본문, PII, 과대 입력과 도배성 텍스트를 품질 규칙으로 처리한다.
- API response 원본의 보존 기간과 삭제 동기화 정책을 provider 약관에 맞춰 정한다.
- rate limit header, retry 횟수와 최종 상태를 manifest에 기록한다.

API client ID·secret·user agent는 환경변수로만 주입하며 raw response나 manifest에
기록하지 않는다. API 접근 승인과 데이터 보존·삭제 조건을 확인하기 전에는 실제
장기 수집을 시작하지 않는다.

## 8. 일별 확정 조건

다음 조건을 모두 만족해야 전날 partition을 `finalized`로 기록한다.

- Google News와 커뮤니티의 cursor가 날짜 종료 경계 이상에 도달
- 예정된 ingestion window의 성공 또는 catch-up 대체 기록 존재
- raw snapshot과 manifest가 MinIO에 존재하고 checksum 검증 통과
- TextEvent staging의 입력·정규화·거부 건수 회계 일치
- source별 건수가 0이면 정상적인 0건인지 수집 실패인지 구분됨

조건을 만족하지 못하면 downstream task를 실행하지 않고 실패 알림을 보낸다. 일부
source만 조용히 제외한 채 일별 분석을 성공 처리하지 않는다.

## 9. 색인 지연과 수정 데이터

00:10 결과는 초기 확정본이다. Google News 색인 지연이나 커뮤니티 수정·삭제를 반영할
필요가 있으면 후속 단계에서 같은 날짜를 재수집하고 `revised` revision으로 저장한다.
초기 구현에서는 자동 재분석까지 포함하지 않지만 manifest에 revision과 finalization
시각을 두어 확장 가능하게 한다.

## 10. 구현 순서

1. 공통 `CollectionWindow`, `CollectionManifest`, cursor store 계약 — 완료
2. Google News window collector와 fixture 테스트 — 완료
3. Reddit API 인증 adapter와 공개 fixture — 완료
4. Reddit window collector와 pagination·부분 수집 테스트 — 완료
5. MinIO raw publisher와 cursor 원자적 갱신 — 구현 완료, 실제 MinIO 검증 대기
6. `current_raw_ingestion` DAG — schedule·Airflow import 완료, 실제 Run 대기
7. daily finalization의 raw completeness gate — 완료
8. 기존 end-to-end DAG의 Kafka 이후 경로 연결
9. scheduled·manual 날짜 결정 테스트 — 완료
10. 실제 하루 shadow 수집 후 전날 1일 finalization 검증

현재 `current_daily_pipeline`은 매일 00:10 KST 또는 수동 날짜 범위로 실행되어 두
source cursor, complete manifest, events 파일과 checksum을 검사한 뒤
`data/finalized/current/date=YYYY-MM-DD/`에 고유 `TextEvent v1`과 확정 manifest를
원자적으로 기록한다. Kafka 이후 단계는 아직 이 DAG에 연결하지 않았으므로, 이
확정 산출물을 downstream의 입력으로 바꾸는 작업이 다음 우선순위다.
