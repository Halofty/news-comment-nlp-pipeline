# 장애 및 부하 테스트 계획

## 1. 목적

수집량 증가, 잘못된 입력과 외부 서비스 장애 상황에서 데이터가 유실·중복되거나 파이프라인 전체가 불필요하게 중단되지 않는지 확인하기 위한 목표 테스트 계획입니다.

Producer 오류 단위 테스트와 실제 Kafka 1,000건 발행·Spark 소비, checkpoint 재시작, malformed JSON DLQ 분기는 검증했습니다. Broker 중단, Spark 강제 종료, PostgreSQL·LLM 장애 실험은 아직 수행하지 않았습니다.

## 2. 공통 측정 지표

- 입력·성공·제외·오류 행 수
- 초당 처리량
- Kafka Producer 지연과 Consumer lag
- Spark micro-batch 처리 시간
- PostgreSQL 적재 시간과 충돌 건수
- 중복·지연 이벤트 수
- DLQ 또는 retry 상태 수
- 재시작 전후 데이터 유실·중복 여부
- LLM 요청·오류·재시도와 토큰 사용량

## 3. 데이터 입력 시나리오

| 시나리오 | 입력 | 확인 항목 |
|---|---|---|
| 속도 증가 | 댓글 replay 1·10·100배 | 처리량, lag, Producer queue |
| 정확 중복 | 동일 `event_id` 반복 | Spark·DB 중복 제거 |
| 내용 중복 | 다른 ID의 동일 텍스트 | 품질 flag와 분석 정책 |
| 잘못된 JSON | 파싱 불가능한 메시지 | DLQ 분기와 다음 행 처리 |
| 필드 결측 | `text`, `event_time` 누락 | 계약 오류와 원본 위치 기록 |
| 지연 이벤트 | 순서를 섞은 과거 시각 | watermark 전후 처리 결과 |
| 과대 텍스트 | 매우 긴 댓글 | byte·token 상한과 격리 |
| Unicode 경계 | 제어·zero-width·결합문자 | 정규화·품질 flag·의미 보존 |
| 도배 입력 | 문자·문장·URL 반복 | 반복 탐지와 처리 비용 제한 |

커뮤니티 텍스트의 상세 규칙과 fixture는 피드백 구현 계획 3단계에서 `analysis/quality/` 아래에 작성합니다.

## 4. 서비스 장애 시나리오

### Kafka

- Broker를 일시 중단한 상태에서 Producer를 실행합니다.
- Broker 복구 후 미전달 메시지가 성공 또는 명시적 실패로 처리되는지 확인합니다.
- Broker 재생성 후 volume의 메시지가 유지되는지 확인합니다.
- 토픽 보존 기간과 파티션 설정이 기대값과 같은지 확인합니다.

### Spark

- Streaming query 실행 중 Driver를 중단합니다.
- 같은 checkpoint로 재시작합니다.
- 처리 전·후 offset과 출력 행 수를 비교합니다.
- 동일 micro-batch가 재실행돼도 중복 적재되지 않는지 확인합니다.

### PostgreSQL

- `foreachBatch` 적재 중 연결을 중단합니다.
- staging과 본 테이블의 트랜잭션 상태를 확인합니다.
- 같은 `batch_id`를 재실행해 중복 여부를 확인합니다.

### LLM Batch와 Langfuse

- API 오류, 누락 응답, Batch 만료를 재현합니다.
- `pending`·`retry` 상태에서 재처리되는지 확인합니다.
- 예산 한도 도달 시 신규 제출만 중단되는지 확인합니다.
- Langfuse가 사용 불가능해도 분석 결과 저장이 계속되는지 확인합니다.

## 5. 단계별 실행 계획

| 단계 | 범위 | 완료 증거 |
|---:|---|---|
| 1 | Producer 단위 오류 | 자동화 테스트 결과 |
| 2 | 실제 Kafka 발행·소비 | 완료: 합성 1,000건 발행, Spark 입력 확인 |
| 3 | Spark 100·1,000건 batch | 완료: 처리 통계 보고서 |
| 4 | Kafka→Spark streaming | 완료: 중복 제거·checkpoint 재시작·DLQ 결과 |
| 5 | PostgreSQL 장애·재시작 | batch ID와 중복 적재 결과 |
| 6 | LLM·Langfuse 장애 | 상태 전이와 토큰·비용 기록 |

## 6. 결과 보고서 형식

```text
실험 이름:
실행 날짜:
코드 revision:
환경:
입력 데이터와 건수:
실행 명령:
예상 결과:
실제 결과:
처리량·지연·오류 수:
데이터 유실·중복 여부:
발견한 문제:
후속 작업:
```

실제 원문과 대용량 실행 결과는 `data/`에 저장하고, 공개 가능한 집계 보고서만 저장소에 포함합니다.

## 7. 완료 기준

- [ ] 정상 입력과 오류 입력 건수가 모두 설명 가능해야 합니다.
- [ ] 장애 전후 데이터 유실 여부를 수치로 확인해야 합니다.
- [ ] 재실행 시 중복 방지 기준을 검증해야 합니다.
- [ ] 오류가 발생한 원본의 topic·partition·offset 또는 파일 줄을 추적해야 합니다.
- [ ] 재현 가능한 명령과 환경을 기록해야 합니다.

## 8. 관련 문서

- [Ingestion 구현 설명](../guides/ingestion-implementation.md)
- [PostgreSQL 저장 구조](../architecture/storage-schema.md)
- [LLM 분석 설계](../architecture/llm-analysis-design.md)
- [멘토 피드백 구현 계획](feedback-implementation-plan.md)
