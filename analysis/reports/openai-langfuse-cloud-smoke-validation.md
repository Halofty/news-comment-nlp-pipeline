# OpenAI Batch·Langfuse Cloud 소량 검증

- 실행일: 2026-09-03 KST
- 입력: 공개 합성 `TextEvent v1` 2건
- 모델: `gpt-5.6-luna`
- prompt version: `news-comment-analysis-v1`
- 원문·API key 기록: 없음

## 1. 환경과 사전 검사

`OPENAI_API_KEY`, `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`,
`LANGFUSE_BASE_URL`의 값은 출력하지 않고 비어 있지 않은지만 확인했다. `.env`는
`.gitignore`에 포함되어 있다.

| 항목 | 결과 |
|---|---:|
| 입력 | 2 |
| 요청 JSONL | 2 |
| skip | 0 |
| 예상 입력 token | 589 |
| 최대 출력 token | 600 |
| 예상 최대 비용 | $0.0004189 |
| 일별 사전 검사 예산 | $0.01 |
| budget status | `ok` |

로컬 산출물은 Git에서 제외된
`data/llm/smoke-2026-09-03/`에 저장했다.

## 2. Langfuse Cloud Japan

로컬 `.venv`에 requirements의 `langfuse>=4,<5`를 설치하고 일본 리전 endpoint에서
`auth_check()`를 실행했다.

| 검증 | 결과 |
|---|---|
| 인증 | 성공 |
| trace seed | `llm-sample-20260824-001` |
| generation | 3 |
| 입력 token | 300 |
| 출력 token | 60 |
| 전체 token | 360 |
| usage reconciliation | `matched` |
| metadata-only 정책 | 원문·prompt·응답을 전송하지 않음 |

Cloud 전송 명령은 오류와 fallback 경고 없이 종료됐다. 이후 실제 일별 31건과 월간
1건도 같은 metadata-only 정책으로 전송해 usage 대조가 일치했다.

## 3. OpenAI Responses Batch

사전 검사를 통과한 동일 합성 2건을 실제 Batch API에 한 번만 제출했다.

| 항목 | 값 |
|---|---|
| 내부 batch ID | `llm-smoke-2026-09-03` |
| OpenAI Batch ID | `batch_6a985c7773cc8190be8e30c5c6d011ae` |
| endpoint | `/v1/responses` |
| completion window | `24h` |
| 확인된 요청 수 | 2 |
| 제출 직후 상태 | `in_progress` |
| 현재 실패 | 0 |

이 표는 제출 직후 상태를 남긴 smoke 기록이다. 이후 실제 경제·사회 Batch 32개가 모두
완료됐으며 결과·Schema·usage 검증은
[Date 7 최종 보고서](../../docs/briefings/date7/economy-social-results-01-31.md)에 기록했다.

```text
manifest 2건 = completed + failed
validated 2건 = 고유 custom_id 2건
응답 JSON = news_comment_analysis Schema 통과
Batch usage = 문서별 usage 합계
```

## 4. Airflow 반영

`.env`를 명시해 Airflow 이미지를 다시 만들고 컨테이너를 재생성했다. 컨테이너 내부에서
OpenAI key, Langfuse key pair와 일본 리전 base URL이 존재하는지만 확인했으며 실제 값은
출력하지 않았다.

```text
credentials-configured
```

실제 Batch 제출은 CLI에서 수행했고 Airflow는 `submit=false` dry-run으로 검증했다.
