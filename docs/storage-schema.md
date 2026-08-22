# PostgreSQL 저장 구조 설계

## 1. 목적

Spark가 처리한 원본·정제 이벤트와 LLM 분석 결과, 파이프라인 실행 상태를 PostgreSQL에 저장하기 위한 목표 Schema입니다.

이 문서는 **설계 초안**이며 현재 PostgreSQL DDL과 적재 코드는 아직 구현되지 않았습니다. 실제 구현 시 `sql/migrations/`의 DDL과 이 문서를 함께 갱신합니다.

## 2. 저장 원칙

- Kafka 원본 이벤트와 정제 결과를 분리합니다.
- 분석 결과는 감정, 토픽과 요약 테이블로 나눕니다.
- `event_id`를 원본 문서의 안정적인 식별자로 사용합니다.
- Spark `batch_id`와 LLM Batch 상태를 기록해 재실행을 제어합니다.
- 대량 적재는 행 단위 INSERT 대신 staging과 upsert를 사용합니다.
- 데이터 규모가 커지면 원본 이벤트를 객체 저장소로 분리하는 방안을 검토합니다.

## 3. 목표 테이블

```text
PostgreSQL
├── raw_text_events              # Kafka 원본 이벤트
├── text_documents_clean         # 정제·비식별화 문서
├── text_data_quality            # 결측·중복·지연·품질 통계
├── document_sentiments          # 문서별 감정 결과
├── document_topics              # 문서와 토픽 연결
├── sentiment_window_metrics     # 시간대별 감정 집계
├── topic_window_metrics         # 시간대별 토픽 집계
├── topic_summaries              # 주요 토픽 이름과 요약
├── text_alert_events            # 급증·이상 이벤트
├── llm_batch_jobs               # LLM Batch 작업 상태
├── llm_batch_requests           # 요청별 결과와 재처리 상태
├── stream_batch_commits         # Spark micro-batch 중복 방지
└── pipeline_run_history         # 작업 실행 이력
```

토큰·비용 관측은 Langfuse 도입을 검토하고 있으므로 별도 `llm_usage_ledger`를 확정하지 않습니다. 운영에 반드시 필요한 최소 사용량 정보만 PostgreSQL에 둘지 여부는 Langfuse 도입 결정 후 확정합니다.

## 4. 영역별 책임

| 영역 | 주요 테이블 | 저장 내용 |
|---|---|---|
| 원본 | `raw_text_events` | `TextEvent v1`, Kafka 위치, 수신 시각 |
| 정제 | `text_documents_clean` | 정규화 텍스트, 언어, 품질 상태 |
| 품질 | `text_data_quality` | 실행별 결측·중복·지연·길이 통계 |
| 분석 | `document_sentiments`, `document_topics` | LLM 검증 완료 결과 |
| 집계 | `sentiment_window_metrics`, `topic_window_metrics` | 출처·시간 window별 지표 |
| 운영 | `stream_batch_commits`, `pipeline_run_history` | 재시작·멱등성·실행 이력 |
| LLM 작업 | `llm_batch_jobs`, `llm_batch_requests` | Batch ID, custom ID, 상태와 재시도 |

## 5. Spark 적재 흐름

```text
Spark foreachBatch
→ PostgreSQL staging 테이블 적재
→ 트랜잭션 시작
→ INSERT ... ON CONFLICT로 본 테이블 병합
→ stream_batch_commits에 batch_id 기록
→ 트랜잭션 완료
```

같은 `batch_id`가 이미 완료된 경우 다시 병합하지 않습니다. `event_id`의 중복 처리는 Spark와 PostgreSQL 양쪽에서 방어합니다.

## 6. 구현 전 결정 사항

- [ ] 테이블별 컬럼과 데이터 타입 확정
- [ ] 원본 JSONB와 검색용 정규 컬럼 범위 결정
- [ ] 기본키·외래키·unique constraint 결정
- [ ] 시간·출처·토픽 조회를 위한 index 결정
- [ ] staging 테이블 수명과 정리 정책 결정
- [ ] 보존 기간과 개인정보 삭제 정책 결정
- [ ] Langfuse와 PostgreSQL의 LLM 관측 데이터 책임 구분
- [ ] migration과 rollback 절차 작성

## 7. 관련 문서

- [TextEvent v1 데이터 계약](data-contract.md)
- [LLM 분석 설계](llm-analysis-design.md)
- [장애 및 부하 테스트 계획](failure-and-load-test-plan.md)
- [멘토 피드백 구현 계획](feedback-implementation-plan.md)

