# 기술 스택과 역할

## 구성 요소

| 기술 | 현재 역할 | 선택 이유 |
|---|---|---|
| Python | Collector, Kafka job, Spark Driver 로직과 저장 adapter | 데이터 처리 생태계와 단위 테스트 용이성 |
| Apache Kafka | 이벤트 버퍼, 수집·처리 속도 분리와 과거 데이터 replay | Producer와 Consumer의 독립적 재시작 |
| Apache Spark | 명시적 Schema, 품질 처리, watermark 중복 제거와 micro-batch | batch와 streaming 변환 로직 공유 |
| PostgreSQL | 원본·정제 이벤트, 계약 오류와 batch commit 저장 | transaction, unique constraint와 조회 편의성 |
| Docker Compose | Kafka·Spark Standalone·PostgreSQL·MinIO 로컬 실행 | 재현 가능한 단일 개발 환경 |
| MinIO | raw·processed·checkpoint용 로컬 S3 호환 저장소 | S3 비용 없이 object storage 경계 검증 |
| OpenAI Batch API | GPT-5.6 Luna 감정·토픽·키워드·요약 분석 | 요청 생성·API 작업·검증 CLI 구현, 실제 제출은 key 대기 |
| Langfuse | LLM trace·토큰·비용·지연 관측 | metadata-only adapter와 구조화 로그 fallback 검증 |
| Apache Airflow | 수집·Spark와 LLM Batch dry-run·제출 제어 | 날짜·입력·예산·실제 제출 여부를 Param으로 관리 |

## 현재 실행 구조

```text
Python Collector / replay
        ↓
Kafka KRaft broker
        ↓
Spark Standalone
├─ Master
├─ Worker / Executor
└─ spark-runner / Driver
        ↓
Parquet + PostgreSQL + MinIO
        ↓
GPT-5.6 Luna Batch + Langfuse
```

Spark는 현재 한 컴퓨터의 Master 1대·Worker 1대 구성입니다. 프로세스와 네트워크 경계는 분리했지만 물리적 분산 환경은 아닙니다.

## 확장 시 변경 지점

- 여러 Worker를 물리 노드에 배치하면 bind mount 대신 S3·HDFS 계열 공유 저장소가 필요합니다.
- PostgreSQL 적재량이 커지면 Driver chunk insert를 JDBC staging·COPY 기반 bulk load로 교체합니다.
- 설정과 비밀정보는 Compose 기본 개발값에서 환경별 secret manager로 이동합니다.
- Airflow는 비즈니스 로직을 포함하지 않고 현재 `jobs/`와 `spark_jobs/` 실행 단위를 호출합니다.

전체 목표 구성은 [시스템 구성도](system-architecture.html), Spark 역할은 [Standalone 실행 구조](../guides/spark-standalone.md)에서 확인합니다.
