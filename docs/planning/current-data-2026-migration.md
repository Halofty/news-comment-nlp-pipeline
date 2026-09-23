# 2026 현재 데이터 전환 계획

## 1. 목표

2012년 archive를 재생하던 검증된 파이프라인을 유지하면서, 입력을 2026년 Google News
검색 결과와 직접 수집한 커뮤니티 데이터로 교체한다. 첫 운영 범위는 이미 종료된 날짜의
일별 확정 데이터다. 당일 수집 중인 데이터는 후속 단계에서 `provisional` 경로로
추가하며 확정 결과와 섞지 않는다.

이 작업은 새 파이프라인을 만드는 것이 아니라 동일한 `TextEvent v1` 계약 앞에 current
source adapter를 추가하는 v2 전환이다.

## 2. 버전과 파일 경계

| 구분 | 위치·버전 | 원칙 |
|---|---|---|
| 2012 기준선 | `archive/historical-2012` 브랜치와 `docs/historical/` | 결과·수치·발표 자료를 변경하지 않고 보존 |
| 2026 개발 | `feature/current-data-2026` 브랜치와 `docs/current/` | current collector와 일별 확정 정책의 기준 |
| 공통 구현 | `core/`, `producers/`, `spark_jobs/`, `storage/`, `llm_analysis/` | source 연도와 무관한 코드만 유지 |
| source adapter | `collectors/`와 `orchestration/` | archive와 current 입력을 명시적으로 구분 |
| 공통 설계 | `docs/architecture/` | 실제 구현된 공통 구조만 기록 |

코드 파일은 import 경로와 테스트를 한 번에 갱신할 수 있는 단계에서 이동한다. 문서만
먼저 정리한 뒤 빈 모듈이나 동작하지 않는 current collector를 현재 기능처럼 만들지
않는다.

## 3. 목표 실행 흐름

```text
종료된 분석 날짜 선택
  ├─ Google News RSS 검색 결과 수집
  └─ 선택한 커뮤니티의 공식 API/허용된 피드 직접 수집
        ↓
TextEvent v1 + raw snapshot
        ↓
Kafka bounded batch → Spark 검사·dedup → MinIO
        ↓
날짜·대주제별 OpenAI Batch → PostgreSQL
        ↓
Streamlit 조회 + Slack 완료/실패 알림
```

Google News와 커뮤니티 raw 데이터는 당일 30분마다 함께 수집한다. 초기 current 분석은
수동 날짜 실행과 매일 00:10 KST의 `D-1` 확정 실행을 지원한다. 일별 실행을 시작하기
전에 00:00 경계까지 catch-up 수집과 completeness 검사를 수행한다. 외부 API 결과가
아직 변할 수 있는 당일 분석은 완료 조건에 포함하지 않는다. 상세 설계는
[current raw ingestion 설계](../current/raw-ingestion-design.md)를 따른다.

## 4. 단계별 계획

| 단계 | 작업 | 완료 조건 | 상태 |
|---:|---|---|:---:|
| 0 | historical/current 문서와 Git 경계 정리 | 브랜치·문서 인덱스·2012 실행 기록 경로가 분리됨 | 완료 |
| 1 | current 데이터 정책 확정 | KST, 30분 수집, 00:10 D-1 확정, economy 뉴스와 Reddit 4개 MVP 결정; Reddit 보존·삭제 조건은 API 검증 직전 확인 | 완료 |
| 2 | collector 공통 interface 정의 | window·manifest·source cursor와 원자적 파일 저장 단위 테스트 | 완료 |
| 3 | Google News current collector 보강 | economy snapshot·URL dedup·정확한 게시 시각 fixture; 실제 shadow 수집 필요 | 진행 중 |
| 4 | Reddit current collector 구현 | 4개·OAuth·pagination·부분 수집·작성자 제거 fixture; 실제 인증 실행 필요 | 진행 중 |
| 5 | current 일별 DAG 연결 | raw ingestion과 completeness gate·일별 확정 구현; 기존 Kafka 이후 경로 연결 필요 | 진행 중 |
| 6 | D-1 자동 실행 | 00:10 KST schedule·수동 날짜 범위·멱등 확정 구현; 실제 Airflow/Slack 검증 필요 | 진행 중 |
| 7 | dashboard current view | 최근 7·30일, source 건수·수집 지연·확정 상태 표시 | 예정 |
| 8 | 당일 provisional 확장 | 확정 결과와 분리된 저장·표시·재확정 정책 검증 | 후속 |

한 번에 하나의 단계만 진행 중으로 표시한다. 코드, 자동 테스트, 실제 소규모 실행 결과와
문서가 모두 있어야 완료로 바꾼다.

Reddit API 자격증명 발급과 실호출은 구현 선행조건이 아니다. fixture와 로컬 raw로
나머지 경로를 먼저 완성하고, 실제 하루 shadow 수집 직전에 앱 등록·약관 확인·소량
read-only 검증을 수행한다.

## 5. 먼저 결정할 데이터 정책

### 시간 기준

- 분석 날짜 기준 timezone은 `Asia/Seoul`로 고정한다.
- 원본 시각은 timezone 포함 UTC로 `event_time`에 저장한다.
- 분석 날짜는 별도 파생 필드로 계산하며 UTC 날짜 문자열을 임의로 잘라 사용하지 않는다.
- raw ingestion은 매시 `05분·35분`, D-1 확정 실행은 다음 날 `00:10`에 수행한다.
- 일별 실행은 마지막 catch-up 수집과 completeness gate를 통과한 뒤 downstream을 연다.

### 뉴스

- MVP는 Google News `economy` 검색만 사용한다.
- Google News RSS 결과를 전체 기사 모집단이 아닌 검색 결과 표본으로 명시한다.
- URL, 제목, publisher, 게시 시각, 최초·최종 관측 시각을 기록한다.
- 요청별 반환 건수와 100건 상한 도달 여부를 품질 지표로 남긴다.

### Reddit

- MVP는 `Economics`, `business`, `TrueReddit`, `changemyview` 네 곳만 사용한다.
- 안정화 후 `r/news`, 나머지 세 대주제, `AskReddit` 순서로 최대 21개까지 확장한다.
- archive가 아니라 Reddit의 허용된 공식 API와 인증을 사용한다.
- 작성자 식별자는 분석 입력과 장기 저장에서 제외한다.
- `[deleted]`·`[removed]`, 수정, 삭제 요청과 재수집 정책을 정의한다.
- 원문을 외부 LLM에 보내기 전에 PII·과대 입력·도배 검사를 적용한다.
- API rate limit과 pagination cursor를 checkpoint로 기록한다.

## 6. 코드 정리 목표

collector interface가 확정된 뒤 다음 구조로 이동한다.

```text
collectors/
├── common.py
├── historical/
│   └── reddit_archive.py
├── current/
│   ├── google_news.py
│   └── reddit.py            # MVP 4개, 설정으로 단계적 확대
└── web_news/

orchestration/
├── historical_daily.py
├── current_daily.py
└── unified_daily.py
```

현재 `collectors/reddit.py`와 `orchestration/reddit_daily.py`는 historical archive 전용이다.
단순 이름 변경만 먼저 하지 않고 새 interface와 current collector 테스트가 준비될 때
호출부·테스트·문서를 같은 변경에서 이동한다.

## 7. 첫 번째 current 실행의 완료 조건

- 2026년 종료 날짜 하루를 입력값으로 선택한다.
- Google News와 커뮤니티에서 각각 한 건 이상 수집한다.
- 모든 이벤트가 `TextEvent v1` 검사를 통과하거나 사유와 함께 DLQ로 이동한다.
- Kafka 발행 건수와 Spark 처리·저장·제외·DLQ 건수의 합이 일치한다.
- MinIO raw/processed, PostgreSQL LLM 결과와 serving snapshot을 확인한다.
- 같은 날짜를 재실행해 최종 고유 결과가 증가하지 않는지 확인한다.
- Slack 완료 또는 실패 알림에서 날짜와 source를 식별할 수 있다.
- 수집 completeness를 보장하지 않는다는 한계를 report와 dashboard에 표시한다.

## 8. 이번 단계에서 하지 않는 것

- 당일 데이터를 확정 데이터로 표시
- 무허가 대규모 웹 크롤링
- 기사 전문 저장
- 기존 2012 raw 데이터 삭제 또는 MinIO 객체 이동
- Spark 상시 streaming을 current 운영 경로로 전환
- historical 분석 결과를 current 결과와 같은 추세선으로 연결
- 현재 v3 감정 분포 프롬프트를 바로 Jev로 교체

감정 분포·tone share처럼 제한된 판단을 Jev로 분리하고 Luna는 생성 작업에 집중시키는
비용 절감안은 [별도 후속 계획](jev-llm-cost-optimization.md)에서 관리한다.
