# 구현 로드맵

## 완료된 기반

- Google News·Global Voices·Reddit Collector와 `TextEvent v1` (GDELT는 legacy 경로)
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
- GPT-5.6 Luna Batch 요청 생성·API CLI·결과 Schema 검증
- Langfuse 장애의 구조화 로그 fallback과 LLM 예산 경고 dry-run
- Airflow LLM Batch 수동 DAG와 기본 dry-run 보호

## 다음 순서

1. Google News 100건 도달 요청을 검색어 단위로 재수집하고 날짜별 재개·retry를 구현합니다.
2. Reddit 2012년 2~12월을 21개 subreddit의 UTC 일별 Parquet로 변환해 연간 분석 데이터를 완성합니다.
3. 작은 fixture를 MinIO에 업로드·검증하고 Python adapter와 Spark `s3a://` 읽기를 순서대로 연결합니다.
4. 제출한 OpenAI 소량 Batch 결과를 회수하고 검증 결과를 PostgreSQL에 upsert합니다.
5. Langfuse key로 실제 token·비용·지연 trace를 UI에서 확인합니다.
6. 기존 Airflow DAG와 LLM DAG를 dataset 또는 명시적 의존성으로 연결합니다.
7. Kafka Broker·Spark Streaming checkpoint 중단 복구와 PostgreSQL 적재 중 연결 중단을 추가 검증합니다.
8. Consumer lag, Spark 처리량과 PostgreSQL bulk load를 측정하고 최종 end-to-end 데모를 작성합니다.

단계별 완료 조건과 기록은 [피드백 구현 계획](feedback-implementation-plan.md), 장애 시나리오는 [장애·부하 테스트 계획](failure-and-load-test-plan.md)에서 관리합니다.
