# 뉴스 및 댓글 NLP 데이터 파이프라인

영어 뉴스 제목과 Reddit 댓글을 같은 데이터 계약으로 수집·정제하고, 날짜와 대주제별로
감정·양극화·세부 정서·토픽을 분석해 조회 가능한 결과로 제공하는 데이터 파이프라인입니다.

## 무엇을 해결하는가

- 서로 다른 뉴스·댓글 원본을 `TextEvent v1`으로 표준화합니다.
- Spark로 계약 검사, 품질 판정, 중복 제거와 일별 Parquet 저장을 수행합니다.
- MinIO에 raw·processed·LLM·report 산출물을 분리해 보존합니다.
- OpenAI Batch로 대량 분석 비용을 줄이고 Langfuse로 token·비용을 관측합니다.
- Airflow 한 번의 실행으로 수집부터 PostgreSQL 저장·Slack 알림·최종 읽기까지 연결합니다.
- Streamlit에서 PostgreSQL의 최종 분석 결과를 조회합니다.

## 데이터

| 출처 | 사용 범위 | 분석 텍스트 | 확보·검증 결과 |
|---|---|---|---:|
| Google News | 2012년 영어 검색 결과 | 기사 제목 | 366일 28,994건 |
| Pushshift Reddit Comments | 2012년 archive 중 선정 subreddit | 작성자를 제외한 댓글 본문 | 원본 12개월 239,814,057건 |
| Global Voices | 2012-01-01~2016-02-29 영어 archive | 기사 제목 | Google News 보완 collector 검증 |

기사와 댓글을 사용자 단위로 연결하지 않습니다. 날짜·대주제 단위로 집계하며, 뉴스와
댓글이 함께 있을 때 LLM 분석의 출처 비중은 각각 50%입니다. 상세 schema와 저장 모델은
[TextEvent v1](docs/architecture/data-contract.md)과
[PostgreSQL 저장 구조](docs/architecture/storage-schema.md)에 있습니다.

## 현재 파이프라인

최종 Airflow 배치 경로와 별도로 Kafka·Spark Structured Streaming 재생 경로를 구현해
검증했습니다. Kafka는 현재 최종 Airflow DAG의 직렬 단계가 아니라, 저장된 이벤트를
재생하거나 향후 실시간 입력을 처리하는 독립 경로입니다.

![전체 시스템 구성도](docs/architecture/system-architecture.png)

- [구성도 HTML](docs/architecture/system-architecture.html)
- [현재 전체 실행 방법](docs/guides/end-to-end-execution.md)
- [최신 end-to-end 실행 기록](docs/reports/latest-end-to-end-run.md)

## 저장 결과 읽기

Streamlit은 PostgreSQL의 분석 결과를 읽으며 기본적으로 최신 v3 schema만 보여줍니다.
분석 버전과 최근 조회 건수는 화면에서 변경할 수 있습니다.

![Streamlit 저장 결과](docs/streamlit_result.png)

> `docs/streamlit_result.png`는 발표 전에 실제 실행 화면으로 추가합니다.

## 빠른 실행

```bash
docker compose up -d postgres minio spark-master spark-worker
docker compose --profile serving up -d dashboard
export AIRFLOW_UID="$(id -u)"
docker compose -f infra/airflow/docker-compose.airflow.yml up -d
```

- Airflow: `http://localhost:8082`
- Streamlit: `http://localhost:8501`
- MinIO Console: `http://localhost:9101`

Airflow에서 `news_comment_end_to_end_pipeline`을 열고 날짜, 대주제, 실제 제출 여부를
설정해 Trigger합니다. OpenAI 비용이 발생하지 않는 확인은 `submit=false`, 실제 Batch
분석은 `submit=true`를 사용합니다. 파라미터 설명과 단계별 확인 명령은
[전체 실행 방법](docs/guides/end-to-end-execution.md)을 따릅니다.

## 검증된 결과

| 항목 | 현재 결과 |
|---|---:|
| 최신 단일 Airflow Run 범위 | 2012-08-01~2012-10-31, 92일 |
| Spark 입력 / 고유 저장 | 303,396 / 303,396건 |
| 계약 거부 / 중복 | 0 / 0건 |
| 뉴스 / Reddit | 7,343 / 296,053건 |
| OpenAI Batch | 92/92 완료, 실패 0건 |
| LLM 입력 / 출력 token | 20,265,672 / 52,166 |
| 해당 Run PostgreSQL 분석 | 92건 |
| 전체 누적 PostgreSQL 분석 | 187건 |
| serving snapshot | 92개 |
| 자동 테스트 | 149개 통과 |

최신 Run은 세 건의 구조화 출력 오류를 검증 단계에서 탐지했고, 완료된 OpenAI Batch를
재사용해 해당 task부터 복구했습니다. 전체 수치와 복구 경계는
[최신 실행 기록](docs/reports/latest-end-to-end-run.md)에 정리했습니다.

## 구현 상태와 한계

| 영역 | 상태 |
|---|---|
| Collector, TextEvent 계약, Spark batch | 구현·실데이터 검증 완료 |
| Kafka, Spark Structured Streaming, DLQ | 독립 재생 경로 구현·복구 검증 완료 |
| MinIO, PostgreSQL 멱등 저장 | 구현·재시작 검증 완료 |
| OpenAI Batch, Langfuse, Slack | 구현·실제 Batch 검증 완료 |
| Airflow 단일 DAG, Streamlit | 구현·end-to-end 검증 완료 |
| 기사 전문 수집 | 계획 — 현재는 제목만 사용 |
| MinIO에서 AWS S3 전환 | 계획 — S3-compatible 경계만 검증 |
| 대규모 PostgreSQL bulk load | 계획 — 현재 Driver chunk upsert |

## 저장소 구조

```text
collectors/       외부 데이터 수집과 TextEvent 변환
core/             데이터 계약과 텍스트 품질 규칙
spark_jobs/       batch·Structured Streaming 작업
storage/          JSONL·PostgreSQL·MinIO adapter
llm_analysis/     Batch 요청 생성과 v3 결과 검증
observability/    Langfuse와 구조화 로그 fallback
notifications/    Slack 완료 알림
orchestration/    Airflow에서 재사용하는 실행 helper
dags/             최종 단일 DAG와 과거 DAG 제외 규칙
serving/          PostgreSQL 조회와 Streamlit 화면
analysis/         데이터 명세·품질 fixture·검증 보고서
docs/             아키텍처·실행 가이드·발표 기록
```

문서 전체 목록은 [docs/README.md](docs/README.md), 부하·장애·복구 결과는
[Date 6](docs/briefings/date6/date6.md), 최종 발표 정리는
[Date 8](docs/briefings/date8/date8.md)에서 확인할 수 있습니다.
