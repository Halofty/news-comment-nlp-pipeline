# 감정·양극화·세부 정서 분석 v3

## 변경 이유

2012년 1월 경제·사회 일별 분석 31건 중 `mixed`가 27건(87.1%)이었다. 하지만 이
결과들의 `sentiment_score`는 대부분 음수였다. 하루치 수천 건에 서로 다른 의견이
공존한다는 사실과, 전체 분위기의 방향을 하나의 `mixed` 필드가 동시에 표현한 것이
원인이다.

v2에서 `mixed`를 우세 감정과 양극화로 분리했지만, 이후 실행에서는 대부분의 날짜가
`negative`로만 표시돼 부정의 성격과 원인을 비교하기 어려웠다. v3는 방향 판정은
유지하면서 긍정·부정 각각을 고정된 세부 정서와 원인으로 다시 분해한다.

## v3 결과 구조

```json
{
  "dominant_sentiment": "negative",
  "sentiment_score": -0.42,
  "estimated_sentiment_distribution": {
    "positive": 0.14,
    "neutral": 0.26,
    "negative": 0.60
  },
  "polarization": "high",
  "polarization_score": 0.78,
  "positive_tones": [
    {
      "tone": "hope",
      "share": 0.65,
      "drivers": ["possible recovery"]
    }
  ],
  "negative_tones": [
    {
      "tone": "anxiety",
      "share": 0.60,
      "drivers": ["job security", "living costs"]
    },
    {
      "tone": "frustration",
      "share": 0.40,
      "drivers": ["economic policy"]
    }
  ]
}
```

- `dominant_sentiment`: 가장 우세한 방향. `positive`, `neutral`, `negative`만 허용한다.
- `sentiment_score`: 전체 방향을 -1~1로 표현한다.
- `estimated_sentiment_distribution`: 세 감정의 추정 비율이며 합계는 1이어야 한다.
- `polarization`: 의견 충돌을 `low`, `medium`, `high`로 표현한다.
- `polarization_score`: 충돌 강도를 0~1로 표현한다.
- `positive_tones`: `optimism`, `hope`, `satisfaction`, `trust`, `enthusiasm`,
  `gratitude`, `relief` 중 반복적으로 나타난 긍정 정서와 원인이다.
- `negative_tones`: `anger`, `anxiety`, `frustration`, `distrust`, `sadness`,
  `cynicism`, `disappointment` 중 반복적으로 나타난 부정 정서와 원인이다.
- 각 `share`는 전체 감정에서의 비율이 아니라 해당 polarity 내부의 조건부 비율이다.
  해당 polarity가 0일 때만 빈 배열을 허용해 근거 없는 세부 정서를 만들지 않는다.

`mixed`를 단순 삭제한 것이 아니라, 기존 의미를 **우세 감정**과 **의견 대립**으로
분리했다. 예를 들어 부정 의견이 우세하지만 논쟁도 큰 날은 `negative + high`로
기록한다.

## 검증 규칙

신규 Schema v3 결과는 JSON Schema 검증 후 다음 의미 검증도 통과해야 한다.

1. 분포 세 값의 합이 `1.0 ± 0.01`이어야 한다.
2. `dominant_sentiment`는 분포에서 공동 1위를 포함한 최대 비율이어야 한다.
3. `positive`의 점수는 양수, `negative`의 점수는 음수여야 한다.
4. 긍정·부정 tone 배열은 각각 중복 label이 없어야 한다.
5. 각 tone 배열의 `share` 합은 `1.0 ± 0.01`이어야 한다.
6. `drivers`는 입력에서 반복적으로 확인되는 원인만 기술한다.

모델이 소수 tone을 생략하거나 반올림해 합계가 어긋나는 경우, 비어 있지 않은 tone의
합계가 0보다 크고 1.25 이하이면 보고된 tone 안에서 조건부 비율을 결정적으로
재정규화한다. 합계가 0이거나 1.25를 초과한 결과는 의미 오류로 거부한다. tone별 자유
문장 `rationale`은 `drivers`·최상위 `summary`와 중복되고
출력 절단 위험을 키우므로 제거했으며, 일별 출력 한도는 900 token으로 설정했다.

기존 Schema v1·v2 결과는 다시 작성하지 않으며 읽기 호환성을 유지한다. 월간 v3 요청에
v1 일별 결과를 사용할 때는 기존 점수 방향으로 주 감정을 결정하고, 기존 `mixed`는
`high` 양극화라는 제한적인 호환 정보로 변환한다. 정확한 분포와 점수는 v2로 새로
분석한 결과에서만 사용한다. Batch 결과 검증은 schema version별로 v1·v2·v3를 각각
선택하므로 새 스키마 도입이 기존 결과를 무효화하지 않는다.

## 저장과 화면

PostgreSQL `document_analyses`에는 다음 열을 추가한다.

- `sentiment_distribution jsonb`
- `polarization text`
- `polarization_score double precision`
- `positive_tones jsonb`
- `negative_tones jsonb`

기존 `sentiment` 열에는 v3의 `dominant_sentiment`를 저장한다. Streamlit 화면에서는
평균 감정 점수와 평균 양극화 점수, 감정 분포와 양극화 분포를 각각 보여준다. 기존 v1
행의 양극화·세부 정서 필드는 `NULL`이므로 결과를 추정해서 채우지 않는다. Slack 완료
알림에는 긍정·부정 세부 정서를 비율과 주요 driver와 함께 최대 3개씩 표시한다.

## Langfuse 비교 기준

실제 API 요청 전후로 `economy-society-daily-v1`과
`group-daily-v3-emotional-tones-compact-source-balanced`를 별도 prompt version으로
기록한다. v3 일별 요청은 Reddit 댓글과 뉴스 제목을 별도 구역으로 나누고, 각 출처
내부의 반복 경향을 먼저 판단한 뒤 최종 결과에서 Reddit 50%·뉴스 50%의 출처 단위
가중치를 적용한다. 따라서 원본 행 수가 많은 Reddit이 단순 건수만으로 뉴스보다 큰
비중을 갖지 않는다. 한쪽 출처가 없는 날은 존재하는 출처만 사용하며 없는 근거를
생성하지 않는다. 출력 JSON Schema는 기존 v3와 동일하다. 대표 날짜의
사람 평가와 비교해 주 감정 일치율, 라벨·점수 일관성, Schema 통과율, 비용을 확인한다.
단순히 특정 라벨 비율을 낮추는 것을 성공 기준으로 삼지 않는다.

## 2012년 8~10월 실행 복구

`2012-08-01`~`2012-10-31` 경제·사회 그룹 92일 실행에서 OpenAI Batch 자체는
92건 모두 완료됐지만, 다음 3건은 모델이 일부 tone을 생략한 뒤 보고된 tone의 합계를
1.0으로 다시 맞추지 않아 로컬 의미 검증에서 실패했다.

| 날짜 | 실패한 합계 |
|---|---|
| 2012-08-30 | negative 0.65 |
| 2012-08-31 | positive 0.72, negative 0.58 |
| 2012-10-11 | positive 0.40 |

tone은 polarity 내부의 조건부 구성비이므로 합계가 0보다 크고 1.25 이하인 비어 있지
않은 배열은 보고된 tone 안에서 1.0으로 재정규화하도록 검증기를 보완했다. 기존 run의
실패 map index 29·30·71만 Clear해 저장된 OpenAI Batch 결과를 재사용했으며, 추가 Batch
제출 없이 세 건 모두 검증·PostgreSQL 저장·Slack 알림에 성공했다. 최종적으로 92일의
분석 결과 92건과 serving snapshot 92건을 확인했다.

같은 실행에서 mapped task들이 PostgreSQL DDL을 동시에 적용해 deadlock이 한 차례
발생했으므로, migration은 `prepare_llm_storage` task에서 DAG run당 한 번 실행하고 이후
mapped 분석 저장 task들이 이를 기다리도록 변경했다.
