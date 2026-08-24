# 구현 로드맵

## 완료된 기반

- GDELT·Reddit Collector와 `TextEvent v1`
- 데이터셋 명세·기계 판독 메타데이터
- 텍스트 품질·안전 규칙과 fixture
- Kafka Producer·replay·DLQ 토픽
- Spark 100건·1,000건 batch 검증
- Spark Standalone Structured Streaming과 checkpoint
- PostgreSQL 핵심 테이블과 micro-batch 멱등 적재

## 다음 순서

1. Langfuse 관리형·self-hosted 도입 방식과 데이터 경계를 결정합니다.
2. LLM Batch 요청 생성·제출·polling·결과 검증과 PostgreSQL 상태 저장을 구현합니다.
3. 토큰·비용·지연 trace를 연결하고 Langfuse 장애 시 fallback을 검증합니다.
4. Airflow DAG로 수집·Spark·LLM 실행 단위와 retry/backoff를 연결합니다.
5. Kafka 중단, Spark Worker 종료, PostgreSQL 장애와 재시작 실험을 수행합니다.
6. 대규모 입력에서 Consumer lag, Spark 처리량과 PostgreSQL bulk load를 측정합니다.
7. 다중 노드 실행 시 공유 object storage와 배포 패키지 구조로 전환합니다.

단계별 완료 조건과 기록은 [피드백 구현 계획](feedback-implementation-plan.md), 장애 시나리오는 [장애·부하 테스트 계획](failure-and-load-test-plan.md)에서 관리합니다.
