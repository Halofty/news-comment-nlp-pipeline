# 7차시 — 서빙 레이어 완성과 최종 발표 준비

## 1. 프로젝트와 데이터

영어 뉴스 제목과 Reddit 댓글을 공통 `TextEvent v1`으로 수집·정제하고, 날짜와
대주제별 감정·양극화·세부 정서·토픽을 분석해 조회 가능한 결과로 제공한다.

| 데이터 | 범위 | 결과 |
|---|---|---:|
| Google News | 2012년 영어 검색 결과 | 366일 28,994건 |
| Reddit 원본 | 2012년 archive | 12개월 239,814,057건 |
| 최신 end-to-end Run | 2012-08-01~10-31 economy | 뉴스 7,343 + 댓글 296,053건 |

기사와 댓글을 사용자 단위로 연결하지 않는다. LLM에는 날짜·대주제별 집계를 보내며,
두 출처가 모두 있으면 뉴스와 Reddit의 출처 비중을 각각 50%로 적용한다.

## 2. 파이프라인과 데이터 모델

- 최신 구성도: [HTML](../../architecture/system-architecture.html) ·
  [PNG](../../architecture/system-architecture.png)
- 공통 입력: [TextEvent v1](../../architecture/data-contract.md)
- 최종 저장: [PostgreSQL schema](../../architecture/storage-schema.md)
- Object storage: [MinIO 설계](../../architecture/object-storage.md)

현재 발표용 경로는 Collector → Kafka bounded batch → Spark Batch → MinIO → OpenAI
Batch → v3 검증 → PostgreSQL → Streamlit/Slack이며 Airflow가 전 과정을 제어한다.
Spark는 날짜별 발행 전·후 offset을 읽고 Run ID·날짜가 일치하는 이벤트만 처리한다.
Structured Streaming은 checkpoint 복구 실험용 별도 실행 방식으로 유지한다.

## 3. 끝까지 이어진 실행 결과

| 지표 | 결과 |
|---|---:|
| Airflow Run | `manual__2026-09-06T17:41:03.997288+00:00` |
| 날짜 / mapped Batch | 92일 / 92개 |
| Spark 입력 / 고유 저장 | 303,396 / 303,396건 |
| 계약 거부 / 중복 | 0 / 0건 |
| MinIO | 920개 객체, 206,879,418 bytes |
| OpenAI Batch | 92/92 완료, 실패 0 |
| LLM 입력 / 출력 token | 20,265,672 / 52,166 |
| PostgreSQL 저장 | 이 Run 92건, 전체 누적 187건 |
| serving snapshot | 92개 ready |

전체 실행 방법은 [현재 end-to-end 실행 방법](../../guides/end-to-end-execution.md),
task별 수치와 복구 기록은
[최신 end-to-end 실행 기록](../../reports/latest-end-to-end-run.md)에 분리했다.

위 92일 수치는 Kafka 편입 전 실데이터 Run이다. 현재 14단계 DAG는
`kafka-bounded-smoke-20260907`에서 151건을 수집·Kafka 발행·Spark 처리했고 14/14 task가
성공했다. 발행·offset 범위·Run 필터·Spark 입력·행 회계가 모두 151건으로 일치했다.

## 4. 부하·장애·복구에서 확인한 것

| 실험 | 확인 결과 |
|---|---|
| Spark 100→1,000건 | 고유 저장 99→981건, 행 회계상 미처리 0 |
| 2012년 1월 대량 처리 | 2,935,785건 복구 후 누락·중복 0 |
| PostgreSQL 연결 실패 | 저장 0건에서 복구 후 200건, 중복 재실행에도 200건 유지 |
| Spark·MinIO 재시작 | checkpoint 기준 99→0→50건, 최종 고유 149건 유지 |
| Langfuse 장애 | primary 실패를 구조화 로그 fallback으로 보존 |
| 최신 LLM v3 검증 실패 | 완료 Batch를 재사용해 세 날짜만 재검증·저장, 유료 중복 제출 0 |

아직 보장하지 못하는 범위는 Kafka broker·Spark worker의 장시간 장애, 여러 노드의
분산 MinIO 장애, PostgreSQL 대규모 bulk load, 기사 전문 수집이다.

## 5. 저장 결과를 쓰는 장면

Streamlit이 PostgreSQL의 `document_analyses` 계열 결과를 읽는다. 기본 화면은
source-balanced v3만 조회하며 필요하면 v1·v2·전체로 전환할 수 있다. 감정 분포,
polarization, 주요 topic, positive/negative tones를 사람이 읽을 수 있는 형태로 표시한다.

![Streamlit 저장 결과](../../streamlit_result.png)

> 발표 전에 실제 화면을 `docs/streamlit_result.png`로 캡처한다.

## 6. 발표 시연 순서

1. 최신 PNG 구성도에서 Kafka가 포함된 bounded batch 경로를 설명한다.
2. Airflow에서 현재 14단계 smoke Run과 과거 92일 실데이터 Run을 보여준다.
3. MinIO에서 날짜·Run ID별 processed·LLM·report 객체를 확인한다.
4. Langfuse에서 token·비용 trace 하나를 확인한다.
5. Streamlit에서 v3 분석 결과와 최근 조회 건수를 바꿔 본다.
6. Slack의 날짜별 Batch 완료 알림을 보여준다.

대용량 수집·외부 API 제출·장애 재현은 다시 실행하지 않고 완료 화면과 본 실행 기록으로
대체한다. 실시간 데모는 저장 결과 조회만 수행하므로 1~2분 안에 끝나며 실패해도 원본과
저장 결과를 변경하지 않는다.

## 7. 요구사항 점검

| 요구사항 | 상태 | 근거 |
|---|:---:|---|
| 저장 결과를 읽는 장면 | 캡처 대기 | Streamlit 구현·실조회 완료, `docs/streamlit_result.png`만 추가 예정 |
| 입력→처리→저장→읽기 단일 실행 | 완료 | Kafka 포함 smoke Run 14/14 task success; 92일 실데이터 Run 별도 완료 |
| README 실행 방법 | 완료 | README 요약 + 별도 실행 가이드 링크 |
| 최신 구성도와 데이터 모델 | 완료 | HTML·PNG, TextEvent v1, PostgreSQL schema |
| 실행 결과 표 | 완료 | 본 문서 3장과 최신 실행 기록 |
| 부하·장애·복구 및 미보장 범위 | 완료 | 본 문서 4장과 Date 6·7 보고서 |
| 저장 결과 사용 장면 | 캡처 대기 | Streamlit 실제 조회는 완료 |
| 남은 문제와 다음 단계 | 완료 | 본 문서 4장과 아래 항목 |

## 8. 다음 단계

S3 전환, 기사 전문 수집, PostgreSQL bulk load 등 아직 구현하지 않은 기능은 현재
구성도와 구현 상태에서 제외하고 [후속 확장 계획](../../planning/future-expansions.md)에서
별도로 관리한다.
