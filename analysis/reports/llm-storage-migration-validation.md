# LLM 저장 migration 검증

- 실행일: 2026-09-02 KST
- PostgreSQL: 로컬 Compose `news_pipeline`
- migration: `sql/migrations/004_llm_analysis.sql`
- 실행 결과: 오류 없이 완료

생성 확인:

```text
document_analyses
llm_batch_jobs
llm_batch_requests
```

기존 테이블이나 데이터를 삭제하지 않았으며 세 테이블과 조회용 index 3개를
`CREATE ... IF NOT EXISTS`로 추가했습니다. 실제 OpenAI Batch 결과 upsert adapter는
아직 연결하지 않았으므로 테이블 생성과 결과 저장 완료를 구분합니다.
