# 구현 로드맵

## 완료된 기반

- GDELT·Reddit Collector와 `TextEvent v1`
- 데이터셋 명세·기계 판독 메타데이터
- 텍스트 품질·안전 규칙과 fixture
- Kafka Producer·replay·DLQ 토픽
- Spark 100건·1,000건 batch 검증
- Spark Standalone Structured Streaming과 checkpoint
- PostgreSQL 핵심 테이블과 micro-batch 멱등 적재
- 관리형 Langfuse 도입 방식과 metadata-only 데이터 경계
- Airflow 날짜 parameter 수집·Spark 자동화
- Google News 2012년 366일과 Reddit 2012년 원본 12개월 수집
- Spark 저장 직전 중단과 PostgreSQL 연결 실패·멱등 복구 실험
- MinIO Compose와 raw·processed·checkpoint bucket 초기화

## 다음 순서

1. Google News 100건 도달 요청을 검색어 단위로 재수집하고 날짜별 재개·retry를 구현합니다.
2. Reddit 2012년 2~12월을 21개 subreddit의 UTC 일별 Parquet로 변환해 연간 분석 데이터를 완성합니다.
3. 작은 fixture를 MinIO에 업로드·검증하고 Python adapter와 Spark `s3a://` 읽기를 순서대로 연결합니다.
4. LLM Batch 요청 생성·제출·polling·결과 검증과 PostgreSQL 상태 저장을 구현합니다.
5. 결정된 [Langfuse adapter 경계](../adr/0001-langfuse-deployment.md)로 실제 token·비용·지연 trace와 장애 fallback을 검증합니다.
6. 기존 Airflow DAG를 2012년 Reddit·Google News·LLM 실행 단위로 최신화합니다.
7. Kafka Broker·Spark Streaming checkpoint 중단 복구와 PostgreSQL 적재 중 연결 중단을 추가 검증합니다.
8. Consumer lag, Spark 처리량과 PostgreSQL bulk load를 측정하고 최종 end-to-end 데모를 작성합니다.

단계별 완료 조건과 기록은 [피드백 구현 계획](feedback-implementation-plan.md), 장애 시나리오는 [장애·부하 테스트 계획](failure-and-load-test-plan.md)에서 관리합니다.
