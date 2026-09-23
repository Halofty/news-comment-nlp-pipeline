# Current 2026 뉴스·커뮤니티 동향 파이프라인

## 목표

2026년 Google News와 Reddit 커뮤니티 데이터를 당일 반복 수집하고, 날짜가 바뀐 직후
전날 데이터를 확정해 경제·사회 분야의 감정·양극화·세부 정서·토픽을 분석한다.

현재 단계는 2012 archive 기반 v1의 처리 구조를 재사용하는 v2 전환 작업이다. collector,
반복 raw 수집 DAG와 D-1 확정 DAG는 구현했지만 실제 API shadow run과 Kafka 이후 연결은
남아 있다. 아래 상태는 공통 README의 2012 실행 결과와 구분한다.

## MVP 수집 범위

초기 버전은 이미 LLM 분석 경험이 있는 경제·사회 범위에 집중한다.

### Google News

- 검색 대주제: `economy`
- 현재 keyword 묶음: `economy`, `economic`, `market`, `business`
- 분석 텍스트: 영어 기사 제목
- 결과 해석: 전체 뉴스 원장이 아니라 Google News RSS 검색 결과 표본

### Reddit

| subreddit | MVP 포함 이유 |
|---|---|
| `Economics` | 경제 현상·정책 중심 논의 |
| `business` | 기업·산업·시장 중심 논의 |
| `TrueReddit` | 장문 기사와 사회 현안 토론 |
| `changemyview` | 사회 쟁점에 대한 논증형 의견 |

2012년 경제·사회 검증에는 위 네 곳과 `r/news`까지 총 5개가 사용됐다. current MVP에서는
Google News와 역할이 겹치고 주제 범위가 넓은 `r/news`를 제외한다. 따라서 “검증 결과를
그대로 재현”하는 것이 아니라 검증된 경제·사회 범위를 더 작은 운영 표본으로 시작하는
것이다.

작성자 식별자는 저장·분석하지 않는다. 실제 장기 수집은 Reddit의 공식 API 접근과
보존·삭제 조건을 확인한 뒤 시작한다.

## 실행 주기

```text
매시 05분·35분
  Google News economy + Reddit 4개 subreddit raw 수집
        ↓
다음 날 00:10 KST
  00:00 경계까지 catch-up → 전날 partition 확정
        ↓
  Kafka → Spark → MinIO → OpenAI Batch
        ↓
  PostgreSQL → Streamlit → Slack
```

- 자동 실행은 Airflow interval을 기준으로 전날 하루를 처리한다.
- 수동 실행은 `start_date`~`end_date`를 받아 기존처럼 날짜별 mapped task로 처리한다.
- raw 수집이 불완전하면 downstream을 열지 않고 실패 알림을 보낸다.
- 당일 데이터 분석은 추후 `provisional` 상태로 분리한다.

상세 window, cursor, manifest와 일별 확정 조건은
[Raw ingestion 설계](raw-ingestion-design.md)를 따른다.

## 재사용하는 기존 구조

- `TextEvent v1`
- Kafka bounded batch와 DLQ
- Spark 계약 검사·품질 판정·중복 제거
- MinIO raw/processed/LLM/report 저장
- OpenAI Batch와 Langfuse
- PostgreSQL 멱등 upsert
- Streamlit 조회
- Slack 완료·최종 실패 알림

archive 전용 Reddit collector는 historical 경로로 유지하고, current Reddit API
collector를 별도 adapter로 추가한다.

## 구현 단계

| 단계 | 구현 내용 | 상태 |
|---:|---|:---:|
| 0 | historical/current 브랜치·문서 경계 | 완료 |
| 1 | KST·30분 수집·00:10 D-1 확정 정책 | 완료 |
| 2 | collection window·manifest·cursor 계약 | 코드·단위 검증 완료 |
| 3 | Google News economy window collector | fixture 완료·실제 수집 대기 |
| 4 | Reddit 4개 subreddit API collector | fixture 완료·API 인증 실행 대기 |
| 5 | MinIO raw 저장과 completeness gate | 구현·fixture 검증 완료, 실제 MinIO 검증 대기 |
| 6 | `current_raw_ingestion` DAG | schedule·import 검증·실제 Run 대기 |
| 7 | 자동 D-1·수동 날짜 범위 finalization DAG | 구현·단위 검증 완료, 실제 Run 대기 |
| 8 | 실제 하루 shadow 수집과 end-to-end 검증 | 예정 |

전체 완료 조건은 [2026 전환 계획](../planning/current-data-2026-migration.md)에 있다.

### 현재 구현 파일

```text
collectors/current/models.py       window·manifest 계약
collectors/current/google_news.py  Google News economy snapshot
collectors/current/reddit.py       Reddit OAuth·4개 subreddit pagination
storage/collection_cursor.py       원자적 source cursor
orchestration/current_ingestion.py raw·event·manifest·MinIO 게시
orchestration/current_daily.py    completeness gate·KST 일별 확정
dags/current_raw_ingestion.py      매시 05분·35분 Airflow DAG
dags/current_daily_pipeline.py     매일 00:10 D-1·수동 범위 확정 DAG
```

Reddit 실제 실행에는 로컬 `.env`에 다음 값이 필요하다.

```dotenv
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
REDDIT_USER_AGENT=news-comment-nlp-pipeline/0.2 by-your-reddit-account
```

실제 키는 Git에 저장하지 않는다. Reddit 앱 등록과 실호출은 shadow run 직전에 수행한다.
현재 current 전용 테스트 14건이 통과했다. 전체 회귀 테스트는 비-Spark를 포함해
`183 passed, 1 skipped`이며, Spark 3건은 제한된 실행 환경의 로컬 socket 생성 차단으로
실행되지 않았다. 이전 호스트 검증에서는 같은 Spark 테스트가 통과했다.

`current_daily_pipeline`은 source cursor가 대상 날짜의 다음 자정까지 도달했는지,
겹치는 모든 manifest가 `complete`인지, 각 events 파일이 존재하는지를 fail-closed로
확인한다. 통과하면 KST 날짜로 이벤트를 다시 필터링하고 event ID 중복을 제거해 다음
경로에 저장한다.

```text
data/finalized/current/date=YYYY-MM-DD/
├── events.jsonl
└── manifest.json
```

동일 날짜를 재실행하면 같은 event 순서와 checksum을 유지한다. 아직 이 파일을
Kafka→Spark→LLM 경로에 자동 전달하지는 않는다.

## 확장 순서

MVP가 안정적으로 동작한 뒤 collector와 분석 범위를 다음 순서로 확장한다.

1. 경제·사회 그룹에 `r/news`를 추가해 기존 5개 범위 복원
2. 정치·국제, 기술·디지털, 환경·과학 그룹을 추가해 20개 주제 subreddit으로 확대
3. `AskReddit`을 일반 비교군으로 추가해 기존 계획의 총 21개 범위 완성
4. 최근 7일·30일 대주제 비교 화면 추가
5. 당일 데이터를 `provisional`로 수집·표시하고 다음 날 `finalized`로 교체
6. 감정 분포·tone share 같은 제한된 판단을 Jev로 분리하고 Luna는 토픽·요약 생성에
   집중하는 hybrid 경로를 비용·품질 비교 후 선택적으로 도입

확장 단계마다 API 호출량, raw 저장량, LLM 비용과 source 편향을 다시 측정한다. 설정에
목록이 존재한다는 이유만으로 미검증 source를 운영 수집 대상으로 자동 활성화하지 않는다.

## 관련 문서

- [2026 current 전환 계획](../planning/current-data-2026-migration.md)
- [Raw ingestion 설계](raw-ingestion-design.md)
- [TextEvent v1](../architecture/data-contract.md)
- [Historical 2012 기준선](../historical/README.md)
- [2012 경제·사회 LLM 결과](../briefings/date7/economy-social-results-01-31.md)
- [Jev 분류 분리와 Luna 비용 절감 계획](../planning/jev-llm-cost-optimization.md)
