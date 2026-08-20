# 실제 데이터 표본 검증 결과

- 검증일: 2026-08-20
- 데이터 계약: [`data-contract.md`](data-contract.md)
- JSON Schema: [`../sample/schema.json`](../sample/schema.json)
- 실제 원문 저장 위치: `data/validation/` (`.gitignore` 대상)

## 1. Reddit 월별 표본

Hugging Face의 `fddemarco/pushshift-reddit-comments` 데이터셋에서 `2016-01` Parquet 파일을 스트리밍으로 읽어 유효 댓글 100건을 수집했습니다. 커뮤니티 필터는 적용하지 않았습니다.

```bash
.venv/bin/python -m collectors.reddit \
  --month 2016-01 \
  --limit 100 \
  --output data/validation/reddit-2016-01-100.jsonl
```

### 검증 결과

| 항목 | 결과 |
|---|---:|
| 실제 이벤트 | 100건 |
| JSON Schema 통과 | 100건 |
| Schema 오류 | 0건 |
| 필수 필드 결측 | 0건 |
| 중복 `event_id` | 0건 |
| 고유 커뮤니티 | 79개 |
| 댓글 점수 최솟값 | -11 |
| 댓글 점수 최댓값 | 32 |
| 댓글 점수 평균 | 2.63 |
| 본문 길이 최솟값 | 9자 |
| 본문 길이 최댓값 | 1,634자 |
| 본문 길이 평균 | 156.91자 |
| 가장 이른 이벤트 시각 | 2016-01-01 00:00:00 UTC |
| 가장 늦은 이벤트 시각 | 2016-01-01 00:00:04 UTC |

상위 커뮤니티는 `AskReddit` 8건, `GlobalOffensive` 4건, `CFB`와 `news` 각 3건이었습니다. 표본은 월별 파일의 앞부분에서 수집했으므로 전체 월간 분포를 대표하지 않습니다. 이 검증의 목적은 데이터 계약과 변환 로직의 정상 동작을 확인하는 것입니다.

### 판단

- `engagement`는 음수가 될 수 있으므로 최소값을 0으로 제한하지 않습니다.
- Reddit의 `language=unknown` 정책은 현재 계약과 일치합니다.
- 작성자 정보 없이도 필요한 분석 필드를 구성할 수 있습니다.
- 100건 모두 계약을 통과했으므로 Reddit 매핑은 MVP에 사용할 수 있습니다.

## 2. GDELT 표본

영어 전체 검색과 `climate change` 검증 쿼리로 100건 수집을 시도했지만, 실행 환경의 공유 IP에 대해 GDELT가 다음 rate-limit 안내를 반환했습니다.

```text
Please limit requests to one every 5 seconds ...
```

GDELT는 HTTP 성공 응답과 함께 JSON 대신 안내문을 반환할 수 있으므로 Collector에서 이를 명확한 오류로 처리해야 합니다. 이번 검증에서는 실제 100건을 저장하지 않았으며 GDELT 계약 검증은 아직 완료되지 않았습니다.

### 다음 재검증 조건

- 충분한 간격을 두고 단일 요청으로 실행
- 구체적인 검증 쿼리 사용
- `maxrecords=100` 유지
- 응답이 JSON인지 먼저 확인
- 성공 시 Reddit과 동일하게 Schema 통과, 결측, 중복, 언어와 도메인 분포 집계

## 3. 현재 결론

Reddit 실제 데이터 계약은 검증을 통과했습니다. GDELT는 API 응답 구조를 앞서 소량 확인했지만 이번 100건 검증은 rate limit으로 완료하지 못했으므로, 2회차 완료 전 재시도가 필요합니다.
