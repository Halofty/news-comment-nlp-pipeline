# 기술 스택과 역할

## 구성 요소

| 기술 | 현재 역할 | 선택 이유 |
|---|---|---|
| Python | Collector, Kafka job, Spark Driver 로직과 저장 adapter | 데이터 처리 생태계와 단위 테스트 용이성 |
| Apache Kafka | 이벤트 버퍼, 수집·처리 속도 분리와 과거 데이터 replay | Producer와 Consumer의 독립적 재시작 |
| Apache Spark | 명시적 Schema, 품질 처리, watermark 중복 제거와 micro-batch | batch와 streaming 변환 로직 공유 |
| PostgreSQL | 원본·정제 이벤트, 계약 오류와 batch commit 저장 | transaction, unique constraint와 조회 편의성 |
| Docker Compose | Kafka·Spark Standalone·PostgreSQL 로컬 실행 | 재현 가능한 단일 개발 환경 |
| OpenAI Batch API | 감정·토픽·키워드·요약 분석 | 비동기 대량 분석 후보, 아직 미구현 |
| Langfuse | LLM trace·토큰·비용·지연 관측 | MVP는 관리형 일본 리전과 metadata-only adapter 사용 |
| Apache Airflow | 수집·처리·분석 작업의 스케줄과 재시도 | 실행 단위가 안정화된 후 도입 예정 |

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
Parquet + PostgreSQL
```

Spark는 현재 한 컴퓨터의 Master 1대·Worker 1대 구성입니다. 프로세스와 네트워크 경계는 분리했지만 물리적 분산 환경은 아닙니다.

## 확장 시 변경 지점

- 여러 Worker를 물리 노드에 배치하면 bind mount 대신 S3·HDFS 계열 공유 저장소가 필요합니다.
- PostgreSQL 적재량이 커지면 Driver chunk insert를 JDBC staging·COPY 기반 bulk load로 교체합니다.
- 설정과 비밀정보는 Compose 기본 개발값에서 환경별 secret manager로 이동합니다.
- Airflow는 비즈니스 로직을 포함하지 않고 현재 `jobs/`와 `spark_jobs/` 실행 단위를 호출합니다.

전체 목표 구성은 [시스템 구성도](system-architecture.html), Spark 역할은 [Standalone 실행 구조](spark-standalone.md)에서 확인합니다.
