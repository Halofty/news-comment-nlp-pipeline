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
- 경제·사회 2012년 1월 일별 31개·월간 1개 Batch와 Langfuse usage 대조
- 실제 LLM 결과 32건의 PostgreSQL 멱등 upsert
- Reddit 수집→Spark→LLM 요청 생성 Airflow 통합 DAG 실행
- MinIO checksum·멱등 adapter, Spark S3A와 Airflow 처리 결과 동기화
- raw·processed·LLM·report 현재 정식 파일 869개·40.62GB 전체 이전과 새 산출물 자동 게시

## 기술 확장 로드맵

1. Google News 100건 도달 요청의 검색어 단위 재수집과 날짜별 resume·retry
2. Reddit 2012년 2~12월의 21개 subreddit UTC 일별 Parquet 변환
3. MinIO 기반 Structured Streaming checkpoint 복구 검증
4. Kafka Broker·Spark checkpoint·PostgreSQL 연결 중단 복구 추가 검증
5. Consumer lag·Spark 처리량·PostgreSQL bulk load 측정과 end-to-end 데모

단계별 완료 조건과 기록은 [피드백 구현 계획](feedback-implementation-plan.md), 장애 시나리오는 [장애·부하 테스트 계획](failure-and-load-test-plan.md)에서 관리합니다.
