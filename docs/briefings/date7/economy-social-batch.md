# 경제·사회 2012년 1월 LLM Batch 실행 기록

## 1. 확정 범위

- 분석 기간: 2012-01-01 ~ 2012-01-31
- Reddit: `Economics`, `business`, `news`, `TrueReddit`, `changemyview`
- Web News: `google_news_topic_group=economy`
- 표본 추출: 없음
- 이번 제출 대상: 일별 분석 31건
- 월간 분석 1건: 일별 응답 31건이 모두 완료·검증된 뒤 별도 제출
- 모델과 API: `gpt-5.6-luna`, Responses API, Batch
- 관측 정보: Langfuse에는 원문·key 없이 Batch·stage·token·cost metadata만 기록

## 2. Langfuse 사전 점검

실제 Batch 전에 metadata-only sample trace를 Japan 리전에 전송했다.

| 항목 | 결과 |
|---|---:|
| generation | 3 |
| input token | 300 |
| output token | 60 |
| total token | 360 |
| usage reconciliation | `matched` |
| 예상 sample 비용 | $0.000265 |

따라서 key, base URL, SDK 초기화, trace/generation 기록과 `flush()`가 동작한다.
구조화 로그 fallback 테스트도 기존 자동 테스트에 포함되어 있다.

## 3. 일별 31건 사전검사

`data/experiments/week5/january-recovery`를 전체 순회해 경제·사회 범위만 선택했다.
댓글과 뉴스 제목을 임의로 제한하지 않았으며 빈 값, `[deleted]`, `[removed]`만 제외한다.

| 항목 | 결과 |
|---|---:|
| 일별 요청 | 31 |
| Reddit 입력 | 71,209 |
| Web News 입력 | 633 |
| 제외 | 0 |
| 추정 입력 token, 25% 안전계수 포함 | 7,649,759 |
| 최대 출력 token | 31,000 |
| 가장 큰 일별 입력 | 367,049 token |
| 272K 초과 장문 할증 대상 | 10일 |
| 요청 JSONL 크기 | 24,482,598 bytes |
| 보수적 최대 예상 비용 | $1.1047614 |
| 로컬 비용 상한 | $1.25 |
| 판정 | `ok` |

Luna의 최대 입력 922,000 token보다 가장 큰 일별 입력이 작으므로 개별 요청 크기는
통과한다. 272K를 초과하는 요청은 공식 장문 가격 배수를 별도로 반영했다.

생성 명령:

```bash
python -m jobs.economy_period_batch prepare-daily \
  --input data/experiments/week5/january-recovery \
  --config config/analysis-groups.yaml \
  --request-output data/llm/economy-social-2012-01/daily-requests.jsonl \
  --manifest-output data/llm/economy-social-2012-01/daily-manifest.jsonl \
  --report data/llm/economy-social-2012-01/daily-preflight.json \
  --year 2012 --month 1 --max-output-tokens 1000 \
  --safety-multiplier 1.25 --budget-usd 1.25
```

## 4. 전체 묶음 실패와 안전한 복구

31건을 한 Batch로 제출했으나 조직의 Luna Batch queue 한도 5,000,000 token보다
요청 입력량이 커서 검증 단계에서 실패했다.

- OpenAI Batch ID: `batch_6a987a43d8d88190ae07b33463b657c2`
- 상태: `failed`
- 오류 코드: `token_limit_exceeded`
- 완료/실패 request count: 0/0
- 사용 token: 0
- 발생 비용: $0

이는 데이터나 요청 JSON 오류가 아니다. queue에 등록되기 전 실패했으므로 데이터 누락이나
중복 결과도 발생하지 않았다. 복구 방식은 **날짜별 요청 1건을 날짜별 독립 Batch 1개로
제출하는 것**으로 확정했다. 각 날짜는 다음과 같이 독립적으로 관리된다.

```text
data/llm/economy-social-2012-01/days/YYYY-MM-DD/
├── requests.jsonl       # 해당 날짜 요청 1건
├── manifest.jsonl       # 해당 날짜 manifest 1건
└── preflight.json       # 입력량·장문 할증·비용 검사

data/llm_response/economy-social/2012/01/days/YYYY-MM-DD/
└── batch-state.json     # 날짜별 OpenAI Batch ID와 상태
```

조직 queue 한도는 Batch별 한도가 아니라 진행 중인 Batch의 합계다. 날짜별로 나눠도 31개를
동시에 모두 등록할 수 없으므로, 안전계수 기준 합계가 500만 token 아래인 날짜만 먼저
제출하고 완료된 날짜의 token이 queue에서 빠진 후 다음 날짜를 제출한다.

| 날짜 | 독립 Batch 수 | 추정 입력 token | 상태 |
|---|---:|---:|---|
| 1~15일 | 15 | 3,425,531 | 날짜별 제출·완료 |
| 16~21일 | 6 | 1,444,267 | 날짜별 제출·완료 |
| 22~31일 | 10 | 2,779,961 | 날짜별 제출·완료 |

앞선 21개가 모두 완료되어 queue가 비워진 것을 확인한 뒤 22~31일도 날짜별 독립
Batch로 제출했다. 최종적으로 31개의 독립 Batch가 모두 `completed`됐으며, 날짜별
`request_counts`는 완료 1건·실패 0건이다. 31개 결과를 manifest와 대조한 결과 Schema,
`custom_id`, 날짜 연속성, 유일성, usage 합계가 모두 일치했다.

대표 Batch ID:

- 1일: `batch_6a987b9bf2c88190827f67dd9cebf3ae`
- 21일: `batch_6a987c94ee8c8190992f024b8f602ea7`
- 22일: `batch_6a98c9cbab3c8190aa047d329d9481f3`
- 31일: `batch_6a98c9d9167881909d5ba3fd8b1d2976`

나머지 Batch ID는 로컬 날짜별 state와 `submission-days-22-31.json`에서 확인한다.

## 5. 결과 회수와 후속 실행

날짜별 제출은 다음 도구를 사용한다. 이미 성공적으로 제출된 날짜는 state 파일을 기준으로
건너뛰므로 같은 명령을 다시 실행해도 중복 제출하지 않는다.

```bash
python -m jobs.submit_economy_daily_range \
  --artifact-root data/llm/economy-social-2012-01/days \
  --response-root data/llm_response/economy-social/2012/01/days \
  --year 2012 --month 1 --start-day 22 --end-day 31 \
  --summary-output data/llm_response/economy-social/2012/01/submission-days-22-31.json
```

위 명령은 2026-09-03에 실행 완료했다. 완료 결과는 다음 명령으로 내려받아 각 날짜의
manifest와 대조하고, 31일 통합 JSONL과 usage 보고서를 생성했다.

```bash
python -m jobs.collect_economy_daily_results \
  --artifact-root data/llm/economy-social-2012-01/days \
  --response-root data/llm_response/economy-social/2012/01/days \
  --year 2012 --month 1 --start-day 1 --end-day 31 \
  --combined-output data/llm_response/economy-social/2012/01/daily-results-01-31.validated.jsonl \
  --report data/llm_response/economy-social/2012/01/daily-results-01-31.report.json
```

최종 결과는 31/31 검증 성공, 실패·누락·중복 0건이며 실제 비용은 `$0.5477199`다.
상세 결과는 [1월 최종 보고서](economy-social-results-01-31.md)에 정리했다.

월간 요청은 검증된 일별 결과 31행을 입력으로 다음 명령에서 생성한다.

```bash
python -m jobs.economy_period_batch prepare-monthly \
  --daily-results data/llm_response/economy-social/2012/01/daily-results-01-31.validated.jsonl \
  --request-output data/llm/economy-social-2012-01/monthly/requests.jsonl \
  --manifest-output data/llm/economy-social-2012-01/monthly/manifest.jsonl \
  --report data/llm/economy-social-2012-01/monthly/preflight.json \
  --year 2012 --month 1 --budget-usd 0.05
```

## 6. 관련 파일

- `llm_analysis/economy_period.py`: 전체 범위 선택, 일별·월간 요청 생성, 비용 검사
- `jobs/economy_period_batch.py`: 재현 가능한 CLI
- `jobs/submit_economy_daily_range.py`: 날짜마다 독립 Batch를 제출하고 중복 제출 방지
- `jobs/collect_economy_daily_results.py`: 완료 결과 회수, manifest·Schema·usage 검증
- `jobs/openai_batch.py`: 업로드·제출·상태 조회와 Langfuse/fallback 기록
- `tests/test_economy_period_batch.py`: 표본 미사용, 그룹 필터, 31일·월간 생성 테스트

공식 기준: [GPT-5.6 Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna),
[Batch API](https://developers.openai.com/api/docs/guides/batch)

## 7. 테스트 결과

- Python 3.11 (`.venv311`): 전체 `95 passed`
- 경제·사회 요청·OpenAI workflow·Langfuse 집중 검사: `16 passed`
- Python 3.14 (`.venv`): LLM 관련 검사는 통과하지만 PySpark 3.5의 `cloudpickle`
  호환 문제로 Spark 테스트 3개가 실패한다.

따라서 현재 Spark와 전체 자동 검증의 기준 실행환경은 Python 3.11이다. Python 3.14에서
발생한 실패는 이번 LLM Batch 변경으로 인한 회귀가 아니다.
