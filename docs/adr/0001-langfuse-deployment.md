# ADR-0001: 관리형 Langfuse로 LLM 관측 시작

- 상태: 채택
- 결정일: 2026-08-24
- 적용 범위: LLM Batch MVP와 개발·검증 환경

## 배경

OpenAI Batch 분석에서 문서별 토큰, 비용, 처리 시간, 오류와 재시도를 관측해야 한다. 핵심 처리 상태와 분석 결과는 PostgreSQL이 관리하지만, 관측 화면과 집계를 위해 별도 사용량 원장을 직접 구축하는 대신 Langfuse 도입을 검토했다.

현재 로컬 환경은 Kafka, Spark Master·Worker·Driver와 PostgreSQL을 함께 실행한다. 따라서 기능 적합성뿐 아니라 추가 서비스, 메모리, 디스크, 백업 부담과 외부 전송 데이터 범위를 함께 고려했다.

## 결정

MVP에서는 **Langfuse Cloud 일본 리전**을 사용한다. 최초 검증은 Hobby 범위와 합성 데이터로 수행하고, 운영 전 가격·보존·법적 요구사항을 다시 검토한다.

애플리케이션은 Langfuse Python SDK v4를 직접 호출하지 않고 `ObservabilitySink` adapter를 통해 사용한다. 기본 구현은 Langfuse sink이고, 설정 누락이나 전송 장애 시에는 no-op 또는 구조화 로그 sink로 전환한다. Langfuse 실패는 Batch 제출, 결과 검증 또는 PostgreSQL 저장을 실패시키지 않는다.

### 관측 단위

```text
Trace: 내부 LLM batch 작업 1건
├── Span: 요청 JSONL 생성
├── Span: OpenAI Batch 제출
├── Span: 상태 확인과 결과 파일 적재
└── Generation: 문서별 custom_id 1건
    ├── event_id / custom_id
    ├── prompt_version / model
    ├── attempt / status / error_code
    ├── input_tokens / output_tokens / total_tokens
    └── submitted_at / completed_at
```

OpenAI Batch는 비동기 작업이므로 동기 OpenAI wrapper로 장시간 context를 유지하지 않는다. 제출 전에 생성한 내부 `llm_batch_id`를 안정적인 trace seed로 사용하고, 결과 파일을 가져올 때 동일 trace에 문서별 generation을 기록한다.

OpenAI 응답의 usage를 토큰 수 기준값으로 사용한다. 문서별 usage 합계를 Batch 객체의 usage와 대조하고 불일치는 PostgreSQL 운영 로그에 남긴다. Batch 할인 가격이 일반 호출 가격과 다를 수 있으므로 비용은 Batch 전용 Langfuse model definition 또는 명시적인 `cost_details`로 기록하며, 가격 기준일과 버전을 metadata에 남긴다.

### 전송 허용 목록

다음 값만 Langfuse에 전송한다.

- 내부 `llm_batch_id`, `event_id`, `custom_id`
- 모델, prompt version, schema version과 실행 환경
- 상태, 오류 코드, 재시도 횟수와 단계별 시각
- 입력·출력·전체 token 수와 계산된 비용
- Schema 검증 성공 여부와 품질 flag의 집계값

다음 값은 전송하지 않는다.

- 기사·댓글 원문
- prompt와 응답 원문
- 제목, URL, 커뮤니티와 작성자 또는 사용자 식별값
- API key, DSN, 파일 내용과 자유 형식 오류 메시지

SDK의 `input`과 `output`은 비워 둔다. metadata는 allowlist 방식으로 구성하고, 전송 직전 masking hook을 방어 계층으로 추가한다.

## 선택안 비교

| 선택안 | 판단 | 이유 |
|---|---|---|
| 관리형 Cloud | 채택 | 설치·업그레이드·백업 부담 없이 토큰·비용 관측을 빠르게 검증할 수 있다. 일본 리전을 선택할 수 있다. |
| Self-hosted | 보류 | 데이터 통제에는 유리하지만 현재 MVP에 비해 서비스와 자원 부담이 크다. |
| 자체 사용량 원장만 구현 | 보조 수단 | 장애 시 fallback에는 유용하지만 UI·집계·모델 가격 관리를 다시 구현해야 한다. |
| adapter만 만들고 실제 연동 연기 | 기각 | 결합도는 낮추지만 피드백의 실제 토큰·비용 추적을 검증하지 못한다. |

## Self-hosted 운영 부담

공식 v4 구성은 Langfuse Web, Worker, PostgreSQL, ClickHouse, Redis/Valkey와 S3 호환 Blob Storage를 필요로 한다. 공식 최소 사양을 단순 합산하면 약 **11 vCPU, 25.5 GiB RAM**이며, 여기에 디스크·백업·업그레이드와 기존 Kafka·Spark 자원이 추가된다.

| 구성요소 | 공식 최소 사양 | 운영 책임 |
|---|---:|---|
| Web | 2 CPU, 4 GiB | UI·API와 인증 |
| Worker | 2 CPU, 4 GiB | 비동기 event 처리 |
| PostgreSQL | 2 CPU, 4 GiB | 트랜잭션 데이터와 migration |
| Redis/Valkey | 1 CPU, 1.5 GiB | queue·cache |
| ClickHouse | 2 CPU, 8 GiB | trace 분석 저장소 |
| S3 또는 MinIO | MinIO 기준 2 CPU, 4 GiB | 원시 event와 media 저장·수명 주기 |

Docker Compose는 저규모·로컬 검증용이며 고가용성, 확장과 백업을 제공하지 않는다. 데이터 국외 전송을 허용할 수 없거나 관리형 비용이 자체 운영 비용을 넘는 경우에만 별도 환경의 self-hosted 구성을 다시 평가한다.

## 데이터와 보존

Cloud 일본 리전도 프로젝트 외부로 metadata를 전송하는 서비스이므로 실제 데이터 사용 전 조직의 국외 이전·처리자 정책을 확인한다. Hobby의 30일 data access window는 삭제 보장과 같다고 간주하지 않는다. 원문을 보내지 않는 정책과 별개로 운영 전 보존·삭제 조건과 DPA를 다시 확인한다.

Langfuse는 관측 복제본이며 기준 저장소가 아니다. Batch 상태, 분석 결과, 재시도 횟수와 최소 usage 대조값은 PostgreSQL에 유지한다. Langfuse trace가 유실되어도 처리 재개와 중복 방지가 가능해야 한다.

## 구현 경계

7단계 구현은 다음 인터페이스를 따른다.

```text
Batch workflow ──> ObservabilitySink
                    ├── LangfuseSink
                    ├── StructuredLogSink
                    └── NoOpSink
```

- application code는 Langfuse 객체나 decorator를 직접 노출하지 않는다.
- 관측 호출은 예외를 내부에서 처리하고 제한 시간 안에 반환한다.
- 짧게 실행되는 CLI는 종료 전에 명시적으로 flush하되 flush 실패를 경고로만 기록한다.
- secret은 `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`, `LANGFUSE_BASE_URL` 환경 변수로 주입한다.
- 개발·검증 환경은 `LANGFUSE_TRACING_ENVIRONMENT`로 분리한다.

## 재검토 조건

- 원문 또는 평가용 prompt·응답 저장이 필요해질 때
- 조직 정책이 관리형 서비스로의 metadata 전송을 금지할 때
- trace 양과 보존 기간 때문에 Cloud 비용이 자체 운영 비용을 넘을 때
- 다중 사용자 운영, 장기 보존 또는 고가용성이 필요할 때
- self-hosted 자원을 기존 데이터 파이프라인과 분리해 운영할 수 있을 때

## 공식 근거

- [Langfuse self-hosting 구조와 배포 옵션](https://langfuse.com/self-hosting)
- [Self-hosted 최소 인프라 사양](https://langfuse.com/self-hosting/configuration/scaling)
- [Langfuse Cloud 리전](https://langfuse.com/security/data-regions)
- [Token·cost 추적과 수동 usage 입력](https://langfuse.com/docs/observability/features/token-and-cost-tracking)
- [민감 데이터 masking](https://langfuse.com/docs/observability/features/masking)
- [Data retention](https://langfuse.com/docs/administration/data-retention)
- [Python SDK v4 변경 사항](https://langfuse.com/docs/observability/sdk/upgrade-path/python-v3-to-v4)
- [OpenAI Batch 객체와 문서별 usage](https://platform.openai.com/docs/api-reference/batch/object?api-mode=responses)

