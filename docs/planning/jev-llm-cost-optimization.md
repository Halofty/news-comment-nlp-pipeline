# Jev 분류 분리와 Luna 비용 절감 계획

## 1. 상태와 목적

이 문서는 **후속 확장/변경 후보**를 기록한다. 현재 v3 프롬프트, OpenAI Batch 요청,
PostgreSQL 결과와 Streamlit 동작은 변경하지 않는다. Jev의 품질·비용·이용 조건을 실제로
비교하기 전에는 현재 구현으로 표시하지 않는다.

목표는 현재 GPT-5.6 Luna 한 요청이 모두 담당하는 작업 중 bounded classification을
Jev로 분리해 Luna의 입력·출력 부담과 총비용을 줄이는 것이다.

## 2. 현재 `%` 성격의 출력

현재 schema는 `%` 문자열을 반환하지 않고 다음 값을 `0~1` 실수로 반환한다.

- `estimated_sentiment_distribution.positive|neutral|negative`
- `polarization_score`
- `positive_tones[].share`
- `negative_tones[].share`

이 값들은 선택지·점수·확률 분포처럼 답의 공간이 제한된 판단이다. 반면 `topics`,
`keywords`, `summary`, tone의 자유 텍스트 `drivers`는 생성 작업이다.

## 3. 목표 역할 분리

```text
Reddit comments + Google News titles
              ↓
        품질 검사·dedup
              ↓
    Jev: 제한된 판단과 확률
      ├─ positive / neutral / negative
      ├─ positive·negative tone category
      └─ 필요한 score·confidence
              ↓
Python: source별 집계와 파생값 계산
      ├─ 감정 분포
      ├─ dominant sentiment
      ├─ sentiment score
      └─ polarization 및 tone share
              ↓
Luna: 압축된 집계와 대표 근거만 입력
      ├─ topics
      ├─ keywords
      ├─ one-sentence summary
      └─ 선택적으로 tone drivers
```

Jev는 긴 설명을 생성하는 대체 LLM으로 사용하지 않는다. 정해진 선택지나 척도에 대한
확률 판단에만 사용하고, 생성이 필요한 결과는 Luna에 남긴다.

## 4. source 50:50 유지

현재 뉴스와 Reddit의 source-level 비중은 각각 50%다. Jev로 전환해도 record 수가 많은
Reddit이 결과를 지배하지 않도록 다음 순서를 사용한다.

1. Reddit record의 Jev 분포를 Reddit 내부에서 평균한다.
2. Google News record의 Jev 분포를 뉴스 내부에서 평균한다.
3. 두 source가 모두 있으면 두 평균을 각각 0.5 가중치로 결합한다.
4. 한 source만 있으면 존재하는 source를 1.0으로 사용한다.
5. `dominant_sentiment`와 `sentiment_score`는 결합 분포에서 Python으로 계산한다.

tone share도 같은 방식으로 source별 집계 후 결합한다. 단순히 전체 record의 결과를 한 번에
평균하지 않는다.

## 5. Luna 부담을 줄이는 방법

Jev 도입만 하고 Luna에 원문 전체를 그대로 보내면 입력 token 비용은 크게 줄지 않는다.
따라서 hybrid 전환은 다음을 함께 검증한다.

- Luna schema에서 감정 분포·polarization·tone share를 제거한다.
- Luna prompt에서 비율 계산과 합계 1.0 규칙을 제거한다.
- Luna에는 Jev 집계표와 source별 대표 근거만 보낸다.
- 대표 근거는 무작위가 아니라 dedup 후 주제 다양성과 source 균형을 유지해 선택한다.
- `topics` 수, `keywords` 수와 `summary` 길이는 현재보다 늘리지 않는다.
- `max_output_tokens=900`을 그대로 가정하지 않고 실제 출력 p95를 측정해 낮춘다.
- 동일한 날짜·source snapshot·분석 버전은 캐시해 중복 유료 요청을 막는다.
- Luna는 계속 Batch API를 사용하고 reasoning effort는 낮게 유지한다.

비용 비교는 추정치가 아니라 provider usage를 기준으로 다음처럼 계산한다.

```text
hybrid total cost
= Jev input cost
 + Luna compressed input cost
 + Luna output cost
 + 실패·재시도 비용
```

Jev 호출을 record마다 수행하면 호출 수가 매우 커질 수 있으므로 batch/line 처리, 중복 제거,
짧은 record 제외 또는 chunk 전략을 비교한다. 비용 감소보다 호출량과 운영 복잡도가 더
커지면 도입하지 않는다.

## 6. 결과 schema와 버전

기존 v3 결과를 덮어쓰지 않는다. hybrid 결과는 별도 버전으로 저장한다.

```json
{
  "schema_version": 4,
  "analysis_pipeline": "jev-classification-luna-generation",
  "classification_provider": "jev",
  "generation_provider": "openai",
  "distribution_aggregation": "equal-source-weight",
  "prompt_version": "group-daily-v4-jev-luna"
}
```

사용자에게 보이는 핵심 감정 분포·tone share 형태는 가능한 한 유지하되, 각 값의 provenance를
저장한다. Jev 응답의 원본 확률, model/version, 질문 spec version과 confidence도 재현 가능한
범위에서 별도 metadata로 남긴다.

## 7. 검증 계획

먼저 기존 2012년 경제·사회 31일 결과를 고정 benchmark로 사용하고, 그다음 2026 current
shadow data로 비교한다.

| 비교 항목 | 기준 |
|---|---|
| dominant sentiment 일치율 | v3 Luna 결과와 hybrid 결과 |
| 감정 분포 차이 | class별 MAE와 Jensen-Shannon divergence |
| tone 안정성 | 상위 tone 일치율과 날짜별 변동 |
| topic 품질 | 수동 검토와 기존 topic overlap |
| 비용 | 같은 입력 snapshot의 실제 총비용 |
| Luna token | 입력·출력 token 절감률 |
| 처리 시간 | Jev 분류부터 Luna Batch 완료까지 |
| 실패율 | API 오류, 누락, schema 실패, 재시도 |

비용이 줄어도 감정 분포가 한쪽으로 붕괴하거나 source 50:50이 깨지면 전환하지 않는다.

## 8. 도입 단계

1. Jev 공식 API·모델·가격·데이터 처리 조건 확인
2. `JevClassificationProvider` interface와 offline fake 구현
3. 감정 3분류 질문 spec과 fixture 테스트
4. source별 확률 집계·50:50 결합 테스트
5. polarization과 tone을 계산할 수 있는지 별도 평가
6. 기존 31일 shadow 비교와 비용 report
7. Luna compact schema·prompt v4 실험
8. 2026 current 데이터 shadow 실행
9. 품질·비용 기준 통과 후 feature flag로 선택적 활성화
10. 안정화 후 기본 경로 전환 여부 결정

## 9. 위험과 보류 조건

- 새로운 외부 provider로 원문이 전송되므로 개인정보·보존·학습 사용 조건을 다시 검토한다.
- Jev가 주는 확률이 실제 모집단 비율과 같은 의미인지는 benchmark로 검증해야 한다.
- record 단위 확률 평균이 집단 감정의 정답이라고 가정하지 않는다.
- polarization은 단순 긍정/부정 비율만으로 충분하지 않을 수 있다.
- Jev 장애가 Luna 요청을 무제한 fallback시켜 예산을 초과하지 않도록 한다.
- provider·model version이 바뀌면 같은 날짜 결과가 달라질 수 있으므로 version을 고정한다.

위 조건을 확인하기 전에는 Jev를 시스템 구성도의 현재 경로에 추가하지 않는다.

