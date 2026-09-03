# OpenAI API와 Langfuse Cloud 구성 기록

이 문서는 2026-09-03 실제 LLM Batch 실행에 사용한 외부 서비스 구성을 기록한다. 실제
API key와 secret은 문서·로그·Git 이력에 포함하지 않는다.

## 1. 구성 상태

| 구성 | 적용 결과 |
|---|---|
| OpenAI 프로젝트 | `news-comment-nlp-pipeline` 범위 프로젝트 사용 |
| OpenAI 모델·endpoint | `gpt-5.6-luna`, Responses Batch `/v1/responses` |
| OpenAI 인증 | 프로젝트 범위 API key를 Git에서 제외된 `.env`로 주입 |
| 비용 보호 | 로컬 예산 사전검사와 OpenAI project limit 병행 |
| Langfuse | Cloud Japan 프로젝트와 프로젝트 key pair 사용 |
| Langfuse 전송 범위 | Batch ID, 상태, token, 비용, 시간 metadata만 전송 |
| Airflow | `.env` credential 전달과 `submit=false` dry-run 검증 완료 |

OpenAI API Platform의 프로젝트·API key·결제는 ChatGPT 앱 구독과 별개의 실행
환경이다. Langfuse Japan 프로젝트는 `https://jp.cloud.langfuse.com` endpoint와 함께
사용한다.

## 2. 환경변수

`.env.example`의 다음 항목을 로컬 `.env`에 설정했다.

```dotenv
OPENAI_API_KEY=<project-scoped-secret>
OPENAI_MODEL=gpt-5.6-luna
LLM_DAILY_BUDGET_USD=0.01
LLM_BUDGET_WARNING_RATIOS=0.70,0.90,1.00

LANGFUSE_ENABLED=true
LANGFUSE_PUBLIC_KEY=<project-public-key>
LANGFUSE_SECRET_KEY=<project-secret-key>
LANGFUSE_BASE_URL=https://jp.cloud.langfuse.com
LANGFUSE_TRACING_ENVIRONMENT=development
LANGFUSE_TIMEOUT_SECONDS=5
```

CLI는 `--model`, `--daily-budget-usd`, `--sink langfuse`를 명시적으로 전달한다.
Organization 관리용 Admin key는 사용하지 않으며, 실행 로그에는 credential 값이 아닌
존재 여부만 기록한다.

## 3. 실제 연결과 실행 결과

연결 검증은 공개 sample metadata로 먼저 수행한 뒤 실제 경제·사회 분석에 적용했다.

| 검증 | 결과 |
|---|---:|
| Langfuse sample generation | 3 |
| sample token | 입력 300 / 출력 60 / 전체 360 |
| 일별 실제 generation | 31 |
| 월간 실제 generation | 1 |
| 실제 입력 token | 5,436,874 |
| 실제 출력 token | 7,595 |
| 실제 총비용 | $0.5482444 |
| usage reconciliation | 32건 모두 `matched` |
| 실제 전송 fallback | 0건 |

기사·댓글 원문, prompt와 LLM 응답 본문은 Langfuse에 전송하지 않았다. 실제 분석과
비용은 [경제·사회 1월 최종 결과](economy-social-results-01-31.md)에 기록했다.

Langfuse 연결 검증 명령은 다음과 같다.

```bash
python -m jobs.verify_langfuse \
  --sink langfuse \
  --output analysis/reports/langfuse-cloud-verification.jsonl \
  --input-price-per-million 0.10 \
  --cached-input-price-per-million 0.01 \
  --output-price-per-million 0.60
```

## 4. Airflow 반영 결과

Docker Compose는 프로젝트 루트 `.env`의 OpenAI·Langfuse credential을 Airflow
컨테이너에 전달한다. 컨테이너 재생성 후 값 자체를 출력하지 않고 세 credential의 존재를
확인했다.

```text
credentials-configured
```

`llm_batch_pipeline`은 합성 2건, `submit=false` 설정으로 4개 task와 예산 차단을
검증했다. 실제 경제·사회 일별 31개와 월간 1개 Batch는 CLI에서 완료했으며 Airflow
dry-run은 실제 분석 건수와 비용에 포함하지 않는다.

## 5. 데이터·자격 증명 경계

- `.env`와 원본·응답 파일은 Git에 포함하지 않는다.
- Langfuse에는 내부 ID, 모델, 상태, token, 비용과 시간만 전송한다.
- 기사·댓글 원문, URL, 작성자, prompt와 응답 본문은 관측 payload에서 제외한다.
- Batch 결과는 출력 순서가 아닌 `custom_id`로 manifest와 대조한다.
- Langfuse 장애 시 구조화 로그 fallback으로 관측 실패를 핵심 처리와 격리한다.

## 6. 공식 문서

- [OpenAI API Platform](https://platform.openai.com/)
- [GPT-5.6 Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna)
- [OpenAI Batch API](https://developers.openai.com/api/docs/guides/batch)
- [OpenAI Usage API](https://platform.openai.com/docs/api-reference/usage)
- [Langfuse Cloud 리전](https://langfuse.com/security/data-regions)
- [Langfuse Python SDK](https://python.reference.langfuse.com/langfuse)
