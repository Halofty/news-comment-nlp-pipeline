# 멘토 피드백 구현 계획

## 1. 문서 목적

이 문서는 2026-08-21에 받은 멘토 피드백을 한 번에 구현하지 않고 단계적으로 반영하기 위한 작업 계획과 진행 기록입니다.

각 단계는 다음 원칙으로 진행합니다.

1. 작업을 시작하기 전에 완료 조건을 먼저 확인합니다.
2. 한 번에 하나의 단계를 `진행 중`으로 둡니다.
3. 코드 작성 후 테스트·샘플 실행·문서 갱신까지 끝나야 완료로 표시합니다.
4. 새로운 의존성은 필요성과 운영 부담을 검토한 뒤 추가합니다.
5. 실제 원문 데이터와 개인정보는 Git에 포함하지 않습니다.

## 2. 피드백 요약

| 번호 | 피드백 | 반영 목표 |
|---:|---|---|
| 1 | README가 길고 세부 내용이 많음 | README에는 프로젝트 핵심과 빠른 시작만 남기고 상세 내용은 `docs/`로 분리 |
| 2 | 토큰 관리를 위해 Langfuse 사용 | 직접 원장을 만드는 대신 Langfuse 기반 관측 가능성 검토·도입 |
| 3 | 데이터 명세와 분석용 메타데이터 필요 | 데이터셋별 출처·규모·필드·품질·사용 범위를 문서와 메타데이터로 관리 |
| 4 | 악성·비정상 커뮤니티 댓글 대비 | 도배, 잘못된 Unicode, 과대 입력 등 품질·안전 규칙 정의 및 테스트 계획 수립 |
| 5 | Spark로 최소 100건, 가능하면 1,000건 이상 처리 | Spark 입력·Schema·정제·출력과 처리 결과 검증 구현 |

## 3. 전체 진행 현황

상태 표기:

- `[ ]` 시작 전
- `[-]` 진행 중
- `[x]` 완료
- `[!]` 외부 조건 또는 결정 필요

| 단계 | 작업 | 상태 | 진행률 | 완료 증거 |
|---:|---|:---:|---:|---|
| 0 | 피드백 구현 계획 작성 | `[x]` | 100% | 이 문서 작성 |
| 1 | README 축약과 상세 문서 분리 | `[x]` | 100% | README 437→163줄, 상세 문서 3개, 링크·테스트 검사 통과 |
| 2 | 데이터셋 명세와 메타데이터 작성 | `[x]` | 100% | 데이터셋 명세 2개, YAML 카탈로그·Schema, 표본 profile 2개, 자동 검사 6개 통과 |
| 3 | 커뮤니티 텍스트 품질·안전 기준 설계 | `[x]` | 100% | 정책 문서, 기준 구현, fixture 19개·Schema와 자동 검사 7개 통과 |
| 4 | Spark 100건 처리 MVP | `[x]` | 100% | 명시적 Schema·공통 변환·CLI, 합성 100건 행 회계와 profile |
| 5 | Spark 1,000건 이상 확장 검증 | `[x]` | 100% | 동일 코드 1,000건 처리, partition·품질·시간·운영 로그 점검 보고서 |
| 6 | Langfuse 도입 방식 조사와 결정 | `[x]` | 100% | 관리형 일본 리전·metadata-only 정책과 adapter 경계 ADR |
| 7 | Langfuse 토큰·비용 추적 연동 | `[-]` | 95% | adapter·360 token 대조·장애 fallback 완료, 실제 Cloud 확인만 대기 |
| 8 | Airflow로 Reddit 일별 수집·Spark 자동화 | `[x]` | 100% | 2016-01-01·2016-02-01 각 1,000건 수집, Spark 처리와 행 회계 검증 완료 |
| 9 | 2012년 수집·부하·장애 복구 | `[x]` | 100% | Google News 366일, Reddit 원본 12개월, Spark·DB 복구 누락·중복 0건 |
| 10 | MinIO 로컬 object storage | `[-]` | 60% | Compose 기동·health check·3개 bucket 생성을 검증, 실제 upload·Spark 연동 대기 |
| 11 | 6차시 보완·LLM Batch | `[-]` | 90% | GPT-5.6 Luna 요청·API CLI·Schema 검증·Airflow dry-run·예산 alert 완료, 실제 API key 대기 |

4차시 과제의 **8단계 Airflow 자동화**는 Reddit 하루 날짜 방식으로 완료했습니다.
5차시 부하·장애 과제도 2012년 데이터 확대, Spark 저장 전 강제 실패와 PostgreSQL
연결 실패 복구로 완료했습니다. 뉴스 기준 데이터는 GDELT에서 2012년 Google News
RSS와 Global Voices 보완 경로로 변경했습니다. 7단계의 관리형 Langfuse 실제 trace
확인은 외부 key 대기 상태이며, 10단계 MinIO는 로컬 서비스와 bucket 준비까지 완료된 상태입니다.

## 4. 단계별 구현 계획

### 1단계 — README 축약과 문서 분리

#### 목표

처음 방문한 사람이 README만 읽고 프로젝트의 목적, 전체 흐름, 현재 구현 상태와 실행 시작점을 빠르게 이해할 수 있도록 정리합니다.

#### README에 남길 내용

- 프로젝트 한 줄 소개
- 문제 정의와 핵심 목표
- 데이터 출처 요약
- 전체 시스템 구성도 또는 간단한 파이프라인 흐름
- 핵심 기술과 선택 이유 한 줄
- 현재 구현 상태
- 빠른 실행 방법
- 테스트 방법
- 상세 문서 링크

#### `docs/`로 이동할 내용

| 현재 README 내용 | 이동 또는 연결할 문서 |
|---|---|
| 데이터 필드와 공통 이벤트 Schema | 기존 `docs/architecture/data-contract.md` 보강 |
| PostgreSQL 테이블 구조 | `docs/architecture/storage-schema.md` 신규 작성 |
| LLM Batch 요청·토픽 통합·비용 전략 | `docs/architecture/llm-analysis-design.md` 신규 작성 |
| 로드 테스트와 장애 복구 시나리오 | `docs/planning/failure-and-load-test-plan.md` 신규 작성 |
| 상세 Collector·Kafka 실행 설명 | 기존 `docs/guides/ingestion-implementation.md` 활용 |
| 전체 아키텍처 | 기존 `docs/architecture/system-architecture.html` 활용 |

#### 작업 체크리스트

- [x] 기존 README 섹션을 핵심·상세로 분류
- [x] 상세 내용을 관련 문서로 이동하거나 기존 문서와 통합
- [x] README를 프로젝트 소개와 quick start 중심으로 다시 작성
- [x] 모든 상대 링크가 실제 파일을 가리키는지 확인
- [x] 중복되거나 서로 다른 설명이 남지 않았는지 확인
- [x] README에서 실제 구현과 목표 아키텍처를 명확히 구분

#### 완료 조건

- README의 상세 Schema·테이블·장애 시나리오가 별도 문서로 이동되어야 합니다.
- README만으로 프로젝트 목적과 실행 시작 방법을 이해할 수 있어야 합니다.
- 상세 정보는 README 링크를 통해 한 번의 이동으로 찾을 수 있어야 합니다.
- 문서 링크 검사와 Markdown 형식 검사를 통과해야 합니다.

---

### 2단계 — 데이터셋 명세와 메타데이터

#### 목표

사용 중인 데이터셋이 무엇이며 어디에서 왔고, 어떤 범위와 품질을 가지며, 파이프라인에서 어떻게 사용되는지 추적할 수 있게 합니다.

#### 제안 구조

```text
analysis/
├── README.md                       # 분석·검증 산출물 안내
├── datasets/
│   ├── gdelt-news.md               # 사람이 읽는 GDELT 명세
│   ├── reddit-comments.md          # 사람이 읽는 Reddit 명세
│   └── dataset-catalog.yaml        # 기계 판독 메타데이터
├── quality/
│   ├── text-quality-rules.md
│   └── validation-summary.md
└── reports/
    ├── gdelt-sample-profile.json
    └── reddit-sample-profile.json
```

실제 원문과 대용량 결과는 `data/`에 두고 Git에서 제외합니다. 공개 가능한 명세, 집계 통계와 검증 코드는 `analysis/`에 둡니다.

#### 데이터셋별 기록 항목

- 데이터셋 이름과 제공자
- 원본 URL과 라이선스·사용 조건 확인 위치
- 확인 기준일
- 원본 형식과 예상 규모
- 프로젝트에서 사용할 기간·커뮤니티·검색 범위
- 사용 필드와 제외 필드
- 원본 필드에서 `TextEvent v1`으로의 매핑
- 개인정보와 민감정보 처리 방식
- 알려진 결측·중복·시간·언어 문제
- 수집 또는 샘플 생성 명령
- 검증 표본 크기와 검증 결과
- 원본·중간·출력 데이터의 Git 포함 여부

#### 기계 판독 메타데이터 후보

```yaml
datasets:
  - id: gdelt-doc-news
    source: GDELT DOC API
    source_type: news
    format: json
    text_scope: title_only
    event_time_field: seendate
    contract_version: 1
  - id: pushshift-reddit-comments
    source: fddemarco/pushshift-reddit-comments
    source_type: comment
    format: parquet
    event_time_field: created_utc
    contract_version: 1
```

실제 필드는 구현 시 데이터 계약과 데이터셋 페이지를 다시 확인해 확정합니다.

#### 작업 체크리스트

- [x] `analysis/` 디렉터리와 안내 문서 생성
- [x] GDELT 데이터셋 명세 작성
- [x] Reddit 데이터셋 명세 작성
- [x] `dataset-catalog.yaml` Schema 결정
- [x] 현재 100건 표본의 기본 profile 작성
- [x] 수집일·기간·건수·필터를 재현할 수 있게 기록
- [x] 데이터 명세와 `docs/architecture/data-contract.md`의 역할 구분

#### 완료 조건

- 사람과 프로그램 모두 데이터셋 메타데이터를 읽을 수 있어야 합니다.
- 어떤 데이터가 어떤 조건으로 수집됐는지 재현할 수 있어야 합니다.
- 원본 데이터 명세와 공통 이벤트 계약의 차이가 분명해야 합니다.

---

### 3단계 — 커뮤니티 텍스트 품질·안전 기준

#### 목표

향후 커뮤니티 데이터가 확장될 때 비정상 입력 한 건이 메모리, 로그, Kafka, Spark 또는 LLM 비용에 과도한 영향을 주지 않도록 규칙과 측정 기준을 정의합니다.

#### 검토할 위험

| 위험 | 예시 | 필요한 대응 후보 |
|---|---|---|
| 도배성 반복 | 같은 문장·문자·URL 반복 | 반복 비율, n-gram 또는 content hash 기반 표시 |
| 과대 입력 | 매우 긴 댓글, 공백·결합문자 반복 | UTF-8 byte·문자·토큰 상한, truncate 또는 격리 |
| Unicode 이상 | 잘못된 byte, 제어 문자, 비정상 결합문자 | UTF-8 decode 정책, Unicode 정규화, 제어 문자 제거 |
| 보이지 않는 문자 | zero-width 문자로 필터 우회 | 허용·제거 범위와 원본 보존 정책 |
| 중복 게시 | 동일 또는 거의 동일한 댓글 반복 | 정확·근사 중복 기준과 시간 window |
| URL·이모지 과다 | 분석 가치가 낮은 자동 생성 댓글 | 비율 측정 후 제거가 아닌 quality flag 우선 검토 |
| 개인정보 | 이메일·전화번호·계정 식별값 | 마스킹 위치와 원본 접근 제한 |
| LLM 비용 공격 | 의도적으로 긴 prompt-like 본문 | 입력 token 상한과 분석 제외 사유 기록 |

#### 설계 원칙

- 원문을 조용히 변경하지 않고 정제 여부와 제외 이유를 기록합니다.
- Unicode 정규화 전후의 의미 손실 가능성을 테스트합니다.
- 문자 수뿐 아니라 UTF-8 byte 수와 LLM token 수를 구분합니다.
- 단순히 긴 글을 모두 제거하지 않고 상한, truncate와 격리 정책을 구분합니다.
- 품질 문제와 악성 행위를 코드가 확정적으로 단정하지 않고 quality flag로 표현합니다.
- Kafka Producer 이전의 방어와 Spark 정제 단계의 방어를 구분합니다.

#### 작업 체크리스트

- [x] `analysis/quality/text-quality-rules.md` 작성
- [x] 최대 문자·byte·token 측정 위치와 상한 결정
- [x] Unicode 정규화와 제어 문자 정책 결정
- [x] 반복·도배 탐지 기준 후보 비교
- [x] 제외, truncate, quality flag, DLQ의 적용 기준 구분
- [x] 정상·경계·악성 fixture 작성
- [x] Collector와 Spark 중 각 검사를 수행할 위치 결정

#### 완료 조건

- 각 위험에 대해 측정값, 기준, 처리 결과가 정의되어야 합니다.
- 최소 10개의 경계·악성 입력 fixture가 있어야 합니다.
- 정상적인 다국어·이모지 텍스트가 과도하게 제거되지 않는지 테스트해야 합니다.
- Spark 구현에서 사용할 품질 컬럼과 제외 사유가 확정되어야 합니다.

---

### 4단계 — Spark 100건 처리 MVP

#### 목표

현재 수집된 `TextEvent v1` JSONL 또는 Kafka `raw-text` 입력을 Spark DataFrame으로 읽고 최소 100건을 명시적 Schema로 처리합니다.

#### 1차 구현 범위

```text
TextEvent v1 JSONL
→ 명시적 Spark Schema
→ 필수 필드·시각 parsing
→ 기본 텍스트 정제
→ event_id 중복 제거
→ 품질 상태 컬럼 생성
→ Parquet 또는 JSONL 출력
→ 처리 통계 보고서
```

Kafka 연동 전에는 JSONL batch 입력으로 변환 로직을 검증하고, 같은 변환 함수를 이후 Structured Streaming에서 재사용합니다.

#### 제안 파일 구조

```text
spark_jobs/
├── __init__.py
├── schemas.py               # TextEvent v1 Spark StructType
├── transformations.py       # 정제·품질·중복 제거 함수
└── process_sample.py        # 100/1,000건 batch 실행 CLI

tests/
└── test_spark_transformations.py
```

#### 필수 출력 지표

- 입력 행 수
- Schema parsing 성공·실패 수
- 뉴스·댓글 행 수
- 고유·중복 `event_id` 수
- 빈 텍스트 수
- 길이 분포와 최대 UTF-8 byte 수
- Unicode·제어 문자 관련 quality flag 수
- 최종 출력 행 수
- 처리 시간

#### 작업 체크리스트

- [x] Java·PySpark 실행 환경 확인
- [x] PySpark 버전과 Python 버전 호환성 확인
- [x] `TextEvent v1` Spark Schema 작성
- [x] JSONL 100건 batch reader 구현
- [x] 공통 transformation 함수 구현
- [x] 중복·결측·길이·Unicode quality 컬럼 구현
- [x] 출력 경로와 포맷 결정
- [x] 100건 실행 결과 보고서 작성
- [x] transformation 단위 테스트 작성

#### 완료 조건

- 실제 또는 공개 가능한 표본 100건 이상을 처리해야 합니다.
- Schema inference 없이 명시적 `StructType`을 사용해야 합니다.
- 입력·제외·출력 건수가 일치하도록 집계해야 합니다.
- 같은 입력을 다시 실행했을 때 결과가 재현되어야 합니다.
- 실패한 행의 수와 이유를 확인할 수 있어야 합니다.

---

### 5단계 — Spark 1,000건 이상 확장 검증

#### 목표

100건에서 검증한 로직을 1,000건 이상에 적용하고 처리 시간과 품질 분포를 기록합니다.

#### 작업 체크리스트

- [x] Reddit 또는 합성·실제 혼합 표본 1,000건 준비
- [x] 1,000건 batch 처리 실행
- [x] 처리 시간과 입출력 건수 기록
- [x] 메모리 또는 partition 문제 확인
- [x] quality flag 분포 검토
- [x] 같은 event ID를 넣어 중복 제거 확인
- [x] 비정상 Unicode·과대 입력 fixture 포함 실행
- [x] 100건 결과와 비교 보고서 작성
- [x] 단계별 구조화 운영 로그 수집
- [x] 로그 순서·행 회계·원문 payload 미기록 자동 점검
- [x] 이후 Kafka Structured Streaming 전환 지점 정리

#### 완료 조건

- 1,000건 이상 입력의 처리 결과가 저장되어야 합니다.
- 데이터 손실 없이 모든 입력 행이 정상, 제외 또는 오류 상태로 집계되어야 합니다.
- 실행 명령과 환경을 다른 사람이 재현할 수 있어야 합니다.

---

### 6단계 — Langfuse 도입 조사와 결정

#### 목표

LLM 요청의 토큰·비용·지연·오류를 직접 원장으로만 관리하는 대신 Langfuse를 사용할 수 있는지 검토합니다. 기능뿐 아니라 함께 운영해야 하는 구성요소와 로컬 자원 부담도 평가합니다.

#### 먼저 결정할 사항

- 관리형 서비스와 self-hosted 중 어느 방식을 사용할지
- 프로젝트의 공개·민감 데이터 정책과 맞는지
- OpenAI Batch API 요청·결과를 어떤 단위로 trace할지
- 문서별 요청, Batch 작업과 재시도를 어떻게 연결할지
- prompt·응답 원문을 저장할지, metadata만 저장할지
- 토큰과 비용의 기준값을 OpenAI 응답과 어떻게 대조할지
- 로컬 Kafka·Spark·PostgreSQL·Airflow와 함께 실행할 자원이 충분한지
- Langfuse 장애가 분석 파이프라인을 중단시키지 않게 할 방법

#### 비교할 선택안

| 선택안 | 장점 | 주의점 |
|---|---|---|
| 관리형 Langfuse | 로컬 운영 부담 감소, 빠른 검증 | 외부 전송 데이터와 비용·정책 확인 필요 |
| Self-hosted Langfuse | 데이터와 운영 환경 통제 | 추가 서비스·저장소·자원·백업 부담 검토 필요 |
| 초기에는 얇은 adapter만 구현 | 애플리케이션 결합도 감소 | 실제 관측 기능 검증이 늦어질 수 있음 |

#### 작업 체크리스트

- [x] 현재 Langfuse 공식 설치·SDK·OpenAI 연동 문서 확인
- [x] 관리형과 self-hosted 구성요소 비교
- [x] 로컬 CPU·메모리·디스크 예상 부담 기록
- [x] 저장할 trace metadata와 개인정보 정책 결정
- [x] OpenAI Batch API 추적 가능 범위 검증
- [x] 선택 결과를 ADR로 작성
- [x] 장애 시 구조화 로그와 no-op sink 대체 방식 기록

#### 완료 조건

- 채택 방식과 선택 이유가 문서로 남아야 합니다.
- 필요한 서비스와 운영 부담이 명시되어야 합니다.
- LLM 분석 코드가 Langfuse에 강하게 결합되지 않도록 경계를 정의해야 합니다.

---

### 7단계 — Langfuse 토큰·비용 추적 연동

구현 구조, token·비용 대조, 예산 제어와 테스트 항목은 [Langfuse 구현·토큰 관리 계획](langfuse-implementation-plan.md)에서 관리합니다.

#### 목표

선택한 운영 방식으로 소량의 LLM 요청을 추적하고, 문서·Batch·재시도 단위의 토큰과 비용을 확인합니다.

#### 제안 추적 단위

```text
Trace: 하나의 LLM Batch 작업
├── batch_id
├── model
├── input document count
├── submitted_at / completed_at
├── status
└── Generation 또는 Observation: 문서별 요청
    ├── event_id / custom_id
    ├── prompt version
    ├── input tokens
    ├── output tokens
    ├── latency
    ├── retry count
    └── validation result
```

#### 작업 체크리스트

- [x] Langfuse client wrapper 작성
- [x] 환경 변수와 secret 관리 추가
- [x] trace 실패가 본 처리 실패로 전파되지 않도록 격리
- [x] 합성 문서 소량 요청 추적
- [x] 토큰 수를 API 응답과 대조
- [x] 재시도 요청의 중복 비용 구분
- [x] prompt·응답 저장 범위 검증
- [x] 실행·확인 방법 문서화

현재 환경에는 관리형 Langfuse key가 없어 실제 Cloud UI 확인은 남아 있습니다. 로컬에서는 실제 SDK 4.14.4의 tracing 비활성 client로 method 호환성을 확인하고, 구조화 로그 sink로 동일 payload를 검증했습니다. 결과는 [Langfuse 샘플 추적 검증](../../analysis/reports/langfuse-token-validation.md)에 기록합니다.

#### 완료 조건

- 최소 한 번의 LLM 분석 흐름이 trace로 확인되어야 합니다.
- `event_id` 또는 `custom_id`로 원본 문서와 추적 결과를 연결할 수 있어야 합니다.
- 입력·출력 토큰과 예상 비용을 확인할 수 있어야 합니다.
- Langfuse가 중단되어도 핵심 분석 결과를 저장할 수 있어야 합니다.

## 5. 단계 간 의존 관계

```text
README 축약 ───────────────────────────────┐
                                           │
데이터 명세 → 텍스트 품질 기준 → Spark 100건 → Spark 1,000건
                                           │
                                           └→ Kafka Streaming 확장

Langfuse 조사 → 운영 방식 결정 → LLM 추적 연동
```

- Spark 구현 전에 데이터 명세와 텍스트 품질 기준을 정합니다.
- Langfuse는 Spark 구현과 독립적으로 조사할 수 있지만, 실제 연동은 LLM Batch 코드 작성 시점에 진행합니다.
- README는 각 단계가 끝날 때마다 핵심 상태와 문서 링크만 갱신합니다.

## 6. 진행 기록

| 날짜 | 단계 | 변경 내용 | 검증 | 다음 작업 |
|---|---:|---|---|---|
| 2026-08-21 | 0 | 멘토 피드백을 7단계 구현 계획으로 분리 | 문서 구조·체크리스트 확인 | 1단계 README 내용 분류 |
| 2026-08-21 | 1 | README를 437줄에서 163줄로 축약하고 저장·LLM·장애 설계를 문서 3개로 분리 | 테스트 16개, Compose, Markdown 8개 로컬 링크 통과 | 2단계 `analysis/` 데이터 명세 작성 |
| 2026-08-23 | 2 | GDELT·Reddit 명세, YAML 카탈로그·JSON Schema, 검증 요약과 profile 작성 | 메타데이터 자동 검사 6개 통과 | 3단계 텍스트 품질·안전 기준 작성 |
| 2026-08-23 | 3 | 길이·byte·Unicode·반복·URL·PII 정책과 Spark 출력 컬럼 확정, Python 기준 구현과 fixture 19개 작성 | 품질 검사 7개 포함 전체 테스트 함수 29개 통과 | 4단계 Spark 100건 처리 MVP |
| 2026-08-23 | 4·5 | 명시적 Spark Schema와 공통 변환으로 합성 100·1,000건 처리, JSONL 출력과 행 회계·품질 profile·단계별 운영 로그 작성 | 100건 100/100, 1,000건 1,000/1,000 행 설명, 운영 로그 10개 이벤트 자동 점검 | 6단계 Langfuse 도입 조사 |
| 2026-08-23 | Streaming 확장 | Kafka Structured Streaming consumer, watermark 중복 제거, checkpoint, 4개 출력 경로와 계약 오류 DLQ 구현 | 실제 Kafka 1,000건에서 고유 981건 처리, 같은 checkpoint 재시작 0건, malformed JSON DLQ 1건 | PostgreSQL 멱등 적재 |
| 2026-08-24 | Standalone 확장 | Spark Master·Worker·제출 Driver를 Compose 서비스로 분리하고 Job 기본 master URL을 환경 설정화 | Worker 2 cores 등록, batch 100건과 streaming 고유 982건 Executor 처리, checkpoint 재제출 0건 | 장애·부하 실험과 PostgreSQL 적재 |
| 2026-08-24 | PostgreSQL 적재 | 핵심 4개 테이블 migration, transaction advisory lock, event·batch upsert와 선택적 Streaming sink 구현 | 실제 982건 적재, NUL 실패 rollback 후 재시도, 새 checkpoint 재처리 `already_committed`와 행 수 불변 확인 | 6단계 Langfuse 도입 조사 |
| 2026-08-24 | 6 | 공식 v4 SDK·Cloud·self-hosted 구성과 OpenAI Batch usage 추적 범위를 비교하고 관리형 일본 리전·metadata-only·adapter 경계를 채택 | ADR에 구성요소, 최소 11 vCPU·25.5 GiB RAM, 허용 metadata, fallback과 재검토 조건 기록 | 7단계 Langfuse 추적 adapter 구현 |
| 2026-08-24 | 7 | vendor 독립 관측 자료형, Langfuse·구조화 로그·no-op sink와 합성 Batch 검증 CLI 구현 | SDK 4.14.4 smoke test, 3건 360 token·$0.000265 대조, 관측 9개·전체 52개 테스트 통과 | 관리형 Cloud trace 확인 후 LLM Batch workflow 연결 |
| 2026-08-27 | 8 | 하루 날짜 Param으로 Reddit Collector와 기존 Spark batch를 연결하는 4-task DAG 및 실행 가이드 작성 | 관련 테스트 15개와 DAG import 오류 0건; 2016-01-01·2016-02-01 각 1,000건 실제 run 성공 | Airflow 성공 화면 캡처·GitHub 링크 제출 |
| 2026-08-31 | 9 | Google News 2012년 366일과 Reddit 원본 12개월 수집, 입력 확대와 Spark·PostgreSQL 장애 복구 실행 | Spark 2,935,785건과 DB 200건 모두 누락·중복 0건 | 계획 문서 동기화와 object storage 경계 추가 |
| 2026-08-31 | 10 | MinIO Compose, health check, raw·processed·checkpoint bucket 생성과 설계 문서 검증 | 로컬 object storage 기반 완료 | 작은 fixture upload와 Python·Spark 연동 |
| 2026-09-02 | 11 | GPT-5.6 Luna Batch JSONL·API CLI·Schema 검증·Airflow DAG·LLM migration 구현 | 90 tests, DAG import 0, dry-run 성공, 예산 차단·복구와 Langfuse fallback 확인 | 실제 OpenAI·Langfuse key 검증과 결과 upsert |

## 7. 다음 작업

다음 작업은 **실제 OpenAI·Langfuse key를 사용한 합성 2건 검증**입니다. Key가 없는
현재 환경에서는 API를 호출하지 않으며, 완료 후 MinIO fixture 작업으로 돌아갑니다.

구현 순서:

1. Compose에서 MinIO와 bucket 초기화 Job의 상태를 확인합니다.
2. 공개 fixture 한 개를 `news-raw`에 업로드하고 size·ETag를 확인합니다.
3. endpoint·bucket을 환경 변수로 받는 Python S3 adapter를 추가합니다.
4. Spark `s3a://` 의존성을 고정하고 작은 Parquet 읽기를 검증합니다.
5. Reddit 2~12월 일별 변환 결과의 MinIO 이동은 로컬·object 행 수 검증 후 진행합니다.
