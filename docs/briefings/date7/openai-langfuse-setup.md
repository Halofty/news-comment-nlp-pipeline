# 6차시 OpenAI API와 Langfuse Cloud 설정 가이드

이 문서는 저장소에 포함할 수 없는 계정·결제·API key 설정을 사용자가 직접 완료하기
위한 체크리스트입니다. 실제 key는 문서, 터미널 로그, 화면 캡처 또는 Git commit에
남기지 않습니다.

## 1. 설정 결과

설정이 끝나면 다음 상태가 되어야 합니다.

```text
OpenAI 전용 프로젝트 + 프로젝트 범위 API key
→ GPT-5.6 Luna Batch 소량 제출 가능
→ OpenAI Usage에서 해당 프로젝트 비용 확인

Langfuse Cloud Japan 프로젝트 + 프로젝트 API key pair
→ metadata-only sample trace 전송
→ Langfuse Traces/Generations에서 token·cost 확인
```

OpenAI API와 ChatGPT 앱의 이용 환경은 구분해서 생각해야 합니다. 이 파이프라인은
OpenAI API Platform의 프로젝트, API key와 API 결제 상태를 사용합니다.

## 2. OpenAI API에서 직접 할 일

### 2.1 프로젝트와 결제 준비

1. [OpenAI API Platform](https://platform.openai.com/)에 로그인합니다.
2. Settings의 Projects에서 `news-comment-nlp-pipeline` 전용 프로젝트를 만듭니다.
3. API Billing에서 결제 수단 또는 사용 가능한 credit을 확인합니다.
4. 프로젝트의 budget·usage notification을 낮은 값으로 설정합니다.
5. Models 또는 Limits 화면에서 `gpt-5.6-luna`를 사용할 수 있는지 확인합니다.

GPT-5.6 Luna의 Batch queue는 free tier를 지원하지 않으므로 실제 Batch 제출 전 API
결제 상태가 필요합니다. Dashboard의 budget·알림 동작은 계정 설정에 따라 달라질 수
있으므로, 이 프로젝트는 별도로 `daily_budget_usd`를 검사해 초과 요청을 제출 전에
차단합니다. 최초 확인은 합성 데이터 2건과 매우 낮은 예산으로 수행합니다.

바로 가기:

- [API keys](https://platform.openai.com/api-keys)
- [Projects](https://platform.openai.com/settings/organization/projects)
- [Billing](https://platform.openai.com/settings/organization/billing/overview)
- [Limits](https://platform.openai.com/settings/organization/limits)
- [Usage](https://platform.openai.com/usage)

화면 메뉴 이름과 URL은 Platform UI 개편에 따라 달라질 수 있습니다.

### 2.2 API key 발급

1. 방금 만든 OpenAI 프로젝트를 선택합니다.
2. Project API keys에서 새 secret key를 만듭니다.
3. 로컬 개인 검증에는 프로젝트 범위 개인 key를 사용합니다.
4. Airflow를 장기 운영할 때는 가능하면 해당 프로젝트의 service account key로
   교체합니다.
5. Organization 관리용 Admin key는 애플리케이션에 넣지 않습니다.
6. key는 발급 직후 암호 관리자에 저장하고, 노출되면 즉시 폐기·재발급합니다.

Restricted 권한을 선택한다면 이 workflow가 사용하는 Files 업로드·조회와 Batches
생성·조회·결과 다운로드가 가능해야 합니다. 첫 실제 제출 전에 요청 수가 2건인지와
예상 최대 비용이 예산 이내인지 다시 확인합니다.

### 2.3 로컬 환경변수

저장소의 `.env.example`을 참고해 Git에서 제외된 `.env`에 다음 값을 직접 입력합니다.

```dotenv
OPENAI_API_KEY=<OpenAI 프로젝트에서 발급한 secret key>
OPENAI_MODEL=gpt-5.6-luna
LLM_DAILY_BUDGET_USD=0.01
LLM_BUDGET_WARNING_RATIOS=0.70,0.90,1.00
```

현재 CLI에서는 모델과 예산을 각각 `--model`, `--daily-budget-usd`로 전달하고,
Airflow에서는 DAG Run configuration의 `model`, `daily_budget_usd`로 전달합니다.
`.env`의 모델·예산 값은 운영 설정을 한곳에 기록하기 위한 기본 정책값이며, 실행 시
전달한 값이 실제 검사 기준입니다.

셸에서 직접 CLI를 실행할 때는 현재 셸에 `.env`를 읽힌 뒤 작업합니다.

```bash
set -a
source .env
set +a
```

key가 존재하는지만 확인하고 실제 값은 출력하지 않습니다.

```bash
test -n "${OPENAI_API_KEY:-}" && echo "OPENAI_API_KEY configured"
```

## 3. Langfuse Cloud에서 직접 할 일

### 3.1 일본 리전 계정과 프로젝트

1. [Langfuse Cloud Japan](https://jp.cloud.langfuse.com/)에서 계정을 만듭니다.
2. Organization을 만들거나 선택합니다.
3. `news-comment-nlp-pipeline` 프로젝트를 만듭니다.
4. Project → Settings → API Keys에서 프로젝트 key pair를 만들거나 확인합니다.
5. Public key와 Secret key를 암호 관리자에 저장합니다.

Langfuse Cloud의 리전은 서로 분리되어 있습니다. 일본 리전에서 만든 프로젝트 key는
`https://jp.cloud.langfuse.com`과 함께 사용해야 하며, EU 또는 US URL로 바꾸면 같은
프로젝트가 보이지 않습니다.

### 3.2 로컬 환경변수

같은 `.env`에 다음 값을 직접 입력합니다.

```dotenv
LANGFUSE_ENABLED=true
LANGFUSE_PUBLIC_KEY=<Langfuse 프로젝트의 public key>
LANGFUSE_SECRET_KEY=<Langfuse 프로젝트의 secret key>
LANGFUSE_BASE_URL=https://jp.cloud.langfuse.com
LANGFUSE_TRACING_ENVIRONMENT=development
LANGFUSE_TIMEOUT_SECONDS=5
```

`LANGFUSE_ENABLED=true`는 배포 설정에서 연동 의도를 나타내며, 현재 검증 CLI는
`--sink langfuse`를 명시해야 실제 Cloud sink를 선택합니다.

이 프로젝트는 기사·댓글 원문, prompt와 LLM 응답을 Langfuse에 보내지 않습니다.
내부 ID, 모델, 상태, token, 계산 비용과 시간처럼 ADR에서 허용한 metadata만 보냅니다.
Langfuse의 보존·사용자 초대 설정을 바꾸기 전에도 이 데이터 경계를 유지합니다.

## 4. Langfuse 연결 검증

환경변수를 현재 셸에 읽힌 뒤 공개 sample metadata를 전송합니다.

```bash
python -m jobs.verify_langfuse \
  --sink langfuse \
  --output analysis/reports/langfuse-cloud-verification.jsonl \
  --input-price-per-million 0.10 \
  --cached-input-price-per-million 0.01 \
  --output-price-per-million 0.60
```

성공 기준:

- 명령 결과의 `reconciliation_status`가 `matched`
- `generation_count`가 3
- 입력 300, 출력 60, 전체 360 token
- Langfuse의 Traces/Generations 화면에 `llm-sample-20260824-001` 관련 trace 표시
- trace의 input·output 본문은 비어 있고 metadata와 usage만 표시

trace가 보이지 않으면 key pair, 일본 리전 base URL, `development` environment를 먼저
확인합니다. 짧게 실행되는 CLI는 내부에서 `flush()`를 호출합니다. Langfuse 연결이
실패해도 구조화 로그 fallback을 통해 LLM 핵심 처리는 계속할 수 있습니다.

## 5. Airflow에 key 반영

Docker Compose는 프로젝트 루트의 `.env` 값을 Airflow 컨테이너에 전달합니다. 값을
수정한 뒤에는 기존 컨테이너를 재시작하는 것만으로 환경변수가 갱신되지 않을 수
있으므로 다시 생성합니다.

```bash
docker compose \
  --env-file .env \
  -f infra/airflow/docker-compose.airflow.yml \
  up -d --build --force-recreate airflow
```

값을 노출하지 않고 전달 여부만 확인합니다.

```bash
docker compose \
  --env-file .env \
  -f infra/airflow/docker-compose.airflow.yml \
  exec airflow sh -lc \
  'test -n "$OPENAI_API_KEY" && test -n "$LANGFUSE_PUBLIC_KEY" && test -n "$LANGFUSE_SECRET_KEY" && echo credentials-configured'
```

Airflow UI는 [http://localhost:8082](http://localhost:8082)에서 엽니다.
`llm_batch_pipeline`의 첫 실행은 반드시 `submit=false`, `limit=2`,
`daily_budget_usd=0.01`로 수행합니다. dry-run의 4개 task와 preflight report를 확인한
뒤 새 Run에서만 `submit=true`로 바꿉니다.

```json
{
  "input_path": "sample/synthetic-events.jsonl",
  "output_root": "data/airflow-output/llm-batch",
  "model": "gpt-5.6-luna",
  "limit": 2,
  "daily_budget_usd": "0.01",
  "submit": false
}
```

실제 제출 후에는 Airflow log나 문서에 key를 복사하지 않고 Batch ID만 기록합니다.
Batch 완료 뒤 `request_counts.total`, `completed`, `failed`, usage와 결과 JSONL 수를
manifest 수와 대조합니다.

## 6. 제출 전에 사용자가 확인할 체크리스트

- [x] OpenAI 전용 프로젝트를 만들었다.
- [x] OpenAI API Billing과 GPT-5.6 Luna 사용 가능 여부를 확인했다.
- [ ] 낮은 프로젝트 budget·usage notification을 설정했다.
- [x] 프로젝트 범위 OpenAI API key를 `.env`에만 저장했다.
- [x] Langfuse Cloud Japan에서 프로젝트와 key pair를 만들었다.
- [x] Langfuse base URL을 일본 리전으로 설정했다.
- [x] Airflow 컨테이너를 다시 생성해 환경변수를 반영했다.
- [x] Airflow `submit=false`, 2건 dry-run이 성공했다.
- [ ] Langfuse sample trace에서 300/60/360 token을 확인했다.
- [x] OpenAI 2건 Batch만 실제 제출했다.
- [ ] OpenAI Usage와 Batch 결과를 확인하고 화면·로그를 secret 없이 캡처했다.
- [x] key가 화면·로그·Git history에 포함되지 않았는지 확인했다.

## 7. 공식 문서

- [OpenAI API quickstart와 key 환경변수](https://platform.openai.com/docs/quickstart/make-your-first-api-request)
- [GPT-5.6 Luna 모델](https://developers.openai.com/api/docs/models/gpt-5.6-luna)
- [OpenAI Batch API](https://developers.openai.com/api/reference/resources/batches)
- [OpenAI Usage API](https://platform.openai.com/docs/api-reference/usage)
- [OpenAI API data controls](https://platform.openai.com/docs/models/default-usage-policies-by-endpoint)
- [Langfuse Cloud 리전](https://langfuse.com/security/data-regions)
- [Langfuse 프로젝트 API key](https://langfuse.com/docs/api-and-data-platform/features/public-api)
- [Langfuse Python SDK 설정](https://python.reference.langfuse.com/langfuse)
- [Langfuse 연결 문제 확인](https://langfuse.com/docs/observability/sdk/troubleshooting-and-faq)
