# 뉴스 및 댓글 NLP 데이터 파이프라인

## 해결하려는 문제

뉴스와 커뮤니티 댓글은 형식, 생성 속도와 품질이 서로 다릅니다. 이 프로젝트는 두 출처의 텍스트를 같은 기준으로 비교할 수 있도록 다음 데이터 엔지니어링 문제를 해결합니다.

- 서로 다른 원본을 하나의 `TextEvent v1` 계약으로 표준화
- Kafka로 수집과 처리 속도를 분리하고 과거 데이터를 재생
- Spark로 계약 검사, 품질 판정, event-time 중복 제거와 경로 분기
- checkpoint와 PostgreSQL batch commit으로 재시작 시 중복 적재 방지
- 향후 LLM Batch 분석의 상태·오류·토큰·비용을 추적할 수 있는 기반 마련

최종 목표는 공통 키워드·토픽과 시간 window를 기준으로 뉴스 보도와 커뮤니티 반응의 감정·토픽 변화를 비교하는 것입니다.

## 데이터

| 출처 | 사용하는 범위 | 분석 텍스트 | 현재 검증 |
|---|---|---|---|
| [GDELT DOC API](https://www.gdeltproject.org/) | URL, 제목, 언어, 도메인, 관측 시각 | MVP에서는 기사 제목 | Collector 구현, 공유 IP rate limit으로 실제 100건 검증 대기 |
| [Pushshift Reddit Comments](https://huggingface.co/datasets/fddemarco/pushshift-reddit-comments) | 댓글 ID, 본문, 작성 시각, subreddit, score | 작성자를 제외한 댓글 본문 | 2016-01 실제 100건 계약 검증 완료 |

두 출처를 기사 단위로 직접 조인하지 않습니다. 실제 원문과 실행 산출물은 Git에 포함하지 않고, `analysis/`에는 공개 가능한 명세·집계·검증 결과만 저장합니다.

## 전체 흐름 — 파이프라인 개요

```text
GDELT · Reddit
→ Collector
→ TextEvent v1 계약 검증
→ JSONL staging
→ Kafka raw-text
→ Spark Structured Streaming
   ├─ 계약 오류 → contract_rejected + Kafka DLQ
   ├─ 품질 거부 → quality_rejected
   ├─ 격리 대상 → quarantine
   └─ 정상·flag → processed
→ partitioned Parquet + PostgreSQL 멱등 upsert
→ LLM Batch 감정·토픽·요약                 [예정]
→ Airflow orchestration·운영 관측           [예정]
```

Spark는 Standalone Master·Worker 구조로 실행하며, 별도의 `spark-runner`가 `spark-submit`과 Driver 역할을 담당합니다.

![전체 시스템 구성도](docs/system-architecture.png)

## 구현 상태

| 영역 | 상태 | 검증 근거 |
|---|---|---|
| GDELT·Reddit Collector | 구현 완료 | Reddit 실제 100건 통과, GDELT 실제 표본은 rate limit 해제 후 재검증 |
| `TextEvent v1`·JSON Schema | 구현 완료 | Python 계약과 JSON Schema 일치 |
| 데이터셋 명세·메타데이터 | 구현 완료 | GDELT·Reddit 명세, YAML 카탈로그와 profile |
| 텍스트 품질·안전 기준 | 구현 완료 | Unicode·반복·URL·PII·과대 입력 fixture 19개 |
| Kafka ingestion | 구현·통합 검증 완료 | 실제 Broker에 합성 1,000건 발행·소비 |
| Spark batch | 구현·검증 완료 | 동일 코드로 100건·1,000건 처리와 행 회계 확인 |
| Spark Structured Streaming | 구현·통합 검증 완료 | watermark 중복 제거, checkpoint 재시작, 4개 경로와 DLQ |
| Spark Standalone | 구현·통합 검증 완료 | Master·Worker 분리, Worker Executor 2 cores 실행 |
| PostgreSQL 적재 | MVP 구현·통합 검증 완료 | 정상 981건·계약 거부 1건, 새 checkpoint 재처리 중복 0건 |
| LLM Batch·Langfuse | 관측 adapter·합성 검증 완료 | 3건 360 token·$0.000265 대조, 실제 Cloud·Batch 연동 전 |
| Airflow orchestration | 계획 | 구현 전 |

전체 자동 테스트는 현재 52개가 통과합니다. PostgreSQL 적재는 1,000건 규모의 Driver chunk upsert 방식이며, 대규모 확장에서는 JDBC staging 또는 bulk load로 교체할 예정입니다.

## 저장소 구조

```text
news-comment-nlp-pipeline/
├── collectors/                  # GDELT·Reddit 수집과 공통 이벤트 변환
├── core/                        # 이벤트 계약과 텍스트 품질 규칙
├── producers/                   # Kafka 메시지 발행
├── jobs/                        # 토픽 초기화·replay·검증 CLI
├── spark_jobs/                  # batch·Structured Streaming Spark Job
├── storage/                     # JSONL과 PostgreSQL 저장 adapter
├── observability/               # Langfuse·구조화 로그·no-op 관측 adapter
├── sql/migrations/              # PostgreSQL 순차 migration
├── infra/spark/                 # Spark 제출 이미지
├── sample/                      # JSON Schema와 공개 합성 이벤트
├── analysis/                    # 데이터셋 명세·품질 fixture·검증 보고서
├── tests/                       # 단위·Spark 변환·저장 테스트
├── docs/                        # 설계·구현·운영 문서
├── docker-compose.yml           # Kafka·Spark Standalone·PostgreSQL
└── requirements.txt
```

## 상세 문서 링크

| 문서 | 내용 |
|---|---|
| [로컬 개발과 실행 가이드](docs/getting-started.md) | 환경 준비, 테스트, 서비스 시작과 Job 제출 |
| [기술 스택과 역할](docs/technology-stack.md) | 구성 요소의 책임, 선택 이유와 확장 지점 |
| [시스템 구성도](docs/system-architecture.html) | 전체 목표 아키텍처 |
| [Ingestion 구현 설명](docs/ingestion-implementation.md) | Collector부터 Kafka 적재 확인까지의 코드 흐름 |
| [TextEvent v1 데이터 계약](docs/data-contract.md) | 공통 Schema와 출처별 필드 매핑 |
| [데이터셋 명세와 메타데이터](analysis/README.md) | 데이터 출처·범위·카탈로그와 공개 가능 profile |
| [실제 표본 검증](docs/validation-report.md) | Reddit 100건과 GDELT 확인 상태 |
| [텍스트 품질·안전 규칙](analysis/quality/text-quality-rules.md) | 품질 측정값·임계값·상태와 출력 규격 |
| [Spark batch 검증](analysis/reports/spark-batch-validation.md) | 100건·1,000건 행 회계와 품질 분포 |
| [Spark 운영 로그 점검](analysis/reports/spark-run-log-review.md) | 1,000건 단계별 시간·행 회계·payload 미기록 검사 |
| [Spark Standalone 실행 구조](docs/spark-standalone.md) | Master·Worker·Driver 역할과 제출 명령 |
| [Spark Streaming Consumer](docs/spark-streaming-consumer.md) | Kafka 입력, watermark, checkpoint, DLQ와 sink |
| [Spark Streaming 통합 검증](analysis/reports/spark-streaming-consumer-validation.md) | 실제 Kafka 처리와 checkpoint 재시작 결과 |
| [PostgreSQL 저장 구조](docs/storage-schema.md) | 핵심 테이블, migration과 transaction upsert |
| [PostgreSQL 통합 검증](analysis/reports/postgres-integration-validation.md) | 982건 적재, rollback·재시도와 멱등성 결과 |
| [데이터와 보안 원칙](docs/data-security.md) | 원문·PII·자격 증명·보존과 외부 전송 기준 |
| [LLM 분석 설계](docs/llm-analysis-design.md) | Batch 분석과 Langfuse 관측 계획 |
| [Langfuse 도입 ADR](docs/adr/0001-langfuse-deployment.md) | 관리형·self-hosted 비교, 데이터 경계와 adapter 결정 |
| [Langfuse 구현·토큰 관리 계획](docs/langfuse-implementation-plan.md) | adapter 구조, token·비용 대조, 예산과 검증 계획 |
| [Langfuse 샘플 추적 검증](analysis/reports/langfuse-token-validation.md) | 합성 3건의 token·비용·재시도와 metadata-only 결과 |
| [장애·부하 테스트 계획](docs/failure-and-load-test-plan.md) | 입력·서비스 장애와 부하 시나리오 |
| [구현 로드맵](docs/roadmap.md) | 완료된 기반과 Langfuse·LLM·Airflow·확장 순서 |
| [피드백 구현 계획](docs/feedback-implementation-plan.md) | 단계별 완료 조건과 진행 기록 |
