# 경제·사회 일별 LLM 분석 중간 결과: 1월 1~21일

> 이 문서는 최초 완료 구간의 중간 기록이다. 31일 전체 결과는
> [1월 1~31일 최종 보고서](economy-social-results-01-31.md)에서 확인한다.

## 1. 실행 범위와 검증 결과

2012년 1월 경제·사회 그룹 중 먼저 완료된 1~21일 결과를 정리했다. Reddit 원문은
`Economics`, `business`, `news`, `TrueReddit`, `changemyview` 전체를 사용했고,
Google News는 `economy` 주제 제목을 사용했다. 표본 추출은 하지 않았다.

| 지표 | 결과 |
|---|---:|
| 완료 Batch | 21/21 |
| Reddit 입력 | 45,750건 |
| Web News 입력 | 405건 |
| 전체 입력 레코드 | 46,155건 |
| 다운로드 결과 | 21건 |
| Schema·`custom_id` 검증 성공 | 21건 |
| 누락·중복·실패 | 0건 |
| usage reconciliation | 21건 모두 `matched` |

원본과 응답은 Git 대상이 아닌
`data/llm_response/economy-social/2012/01/daily-results-01-21.validated.jsonl`에
저장했다. 보고서에는 원문과 credential을 포함하지 않는다.

## 2. 실행 시간과 비용

| 지표 | 결과 |
|---|---:|
| 최초 제출 | 2026-09-03 04:40:11 KST |
| 마지막 완료 | 2026-09-03 07:38:07 KST |
| 전체 wall-clock | 2시간 57분 56초 |
| 날짜별 최소 실행 시간 | 1시간 38분 41초 |
| 날짜별 최대 실행 시간 | 2시간 54분 56초 |
| 날짜별 평균 실행 시간 | 약 2시간 14분 16초 |
| 실제 입력 token | 3,458,521 |
| 실제 출력 token | 4,987 |
| reasoning token | 2,145 |
| cached input token | 0 |
| 실제 비용 | **$0.3488443** |
| 사전 최대 예상 비용 | $0.6625543 |

실제 비용은 25% 입력 안전계수와 날짜별 최대 출력량을 사용한 사전 최대 추정치의 약
52.7%였다. 1~21일 실제 입력은 모두 272K 이하라 장문 할증이 발생하지 않았다.
여러 날짜 Batch가 병렬로 실행되었으므로 wall-clock 기준 관측 처리량은 약 4.32
레코드/초다. 이는 모델 자체 속도가 아니라 제출부터 모든 결과 준비까지의 종단 처리량이다.

## 3. 감성 결과

| 감성 | 날짜 수 |
|---|---:|
| `mixed` | 19 |
| `negative` | 2 |
| `neutral` | 0 |
| `positive` | 0 |

일별 `sentiment_score` 평균은 **-0.333**이다. 경제 위기, 실업, 불평등, 부채,
기업 권력과 규제 논쟁이 반복되어 전반적으로 부정적 성향이 강하지만, 정책 해법에 대한
찬반이 함께 나타나 대부분 `mixed`로 분류됐다.

## 4. 반복 주제

LLM이 날짜마다 표현을 다르게 사용하므로, 일별 `topics`, `keywords`, `summary`를
키워드 규칙으로 상위 개념에 중복 매핑했다. 아래 날짜 수는 상호 배타적이지 않다.

| 정규화한 주제 | 등장 날짜 | 해석 |
|---|---:|---|
| 시장·규제·기업 권력 | 21/21 | 자유시장과 정부 개입, 금융·기업 규제 논쟁 |
| 불평등·경제 이동성 | 20/21 | 소득·부의 격차, 세금, 경영진 보수 |
| 일자리·노동·임금·자동화 | 20/21 | 실업, 최저임금, 아웃소싱, 자동화 |
| 부채·금융위기·긴축 | 18/21 | 국가부채, 유로존, 구제금융, 통화정책 |
| 정치·시민권 | 18/21 | 선거, 양극화, 경찰권, 구금과 전쟁 |
| 교육·의료 | 12/21 | 학비·학자금 부채, 교육재정, 의료비 |
| 디지털 권리·저작권 | 8/21 | SOPA, PIPA, ACTA, Megaupload, 검열 |
| 에너지·기후·자원 | 5/21 | 석유, 기후정책, 물과 공공재 가격 |

시계열상 1월 중반 이후에는 SOPA/PIPA와 인터넷 자유가 뚜렷한 반복 주제로 부상하며,
경제·사회 범위가 금융 문제뿐 아니라 기술정책과 시민권 논쟁까지 포착하고 있음을 보여준다.

## 5. 날짜별 결과

| 날짜 | Reddit | News | 입력 token | 출력 token | 비용 | 감성 | 주요 주제 |
|---|---:|---:|---:|---:|---:|---|---|
| 2012-01-01 | 1,481 | 1 | 104,688 | 249 | $0.010618 | negative (-0.45) | Economic stagnation and inequality; Debt and austerity; Civil liberties |
| 2012-01-02 | 2,276 | 11 | 186,674 | 224 | $0.018802 | mixed (+0.00) | Economic policy and debt; Wages; Inequality and taxation |
| 2012-01-03 | 2,350 | 22 | 192,443 | 203 | $0.019366 | mixed (-0.35) | Debt and fiscal policy; Markets and regulation; Labor and inequality |
| 2012-01-04 | 1,648 | 19 | 116,571 | 232 | $0.011796 | mixed (-0.35) | Economic policy; Corporate power; Energy and climate |
| 2012-01-05 | 2,255 | 17 | 171,368 | 225 | $0.017272 | mixed (-0.25) | Income inequality; Corporate governance; Economic mobility |
| 2012-01-06 | 3,010 | 20 | 248,313 | 261 | $0.024988 | mixed (-0.20) | Recovery and jobs; Corporate power; Markets and public goods |
| 2012-01-07 | 1,831 | 5 | 125,710 | 263 | $0.012729 | mixed (-0.35) | Jobs and recovery; Regulation; Debt and central banks |
| 2012-01-08 | 1,242 | 15 | 99,481 | 214 | $0.010077 | mixed (-0.55) | Economic crisis; Corporate power; Automation and employment |
| 2012-01-09 | 2,276 | 24 | 195,004 | 234 | $0.019641 | mixed (-0.18) | Regulation; Labor and inequality; Copyright and free speech |
| 2012-01-10 | 2,949 | 35 | 225,615 | 219 | $0.022693 | mixed (-0.35) | Work and automation; Capitalism; Labor rights |
| 2012-01-11 | 2,305 | 30 | 145,940 | 240 | $0.014738 | negative (-0.62) | Greek debt; Inequality; Labor rights and outsourcing |
| 2012-01-12 | 2,338 | 19 | 154,921 | 266 | $0.015652 | mixed (-0.35) | Automation; Inequality and taxation; Recovery and debt |
| 2012-01-13 | 2,600 | 26 | 173,067 | 235 | $0.017448 | mixed (-0.25) | Economic policy; Corporate regulation; Employment and labor |
| 2012-01-14 | 1,930 | 15 | 124,720 | 216 | $0.012602 | mixed (-0.35) | Corporate taxation; Inequality; Healthcare and aging |
| 2012-01-15 | 1,898 | 11 | 166,581 | 339 | $0.016862 | mixed (-0.35) | Inequality; Financial regulation; Government and capitalism |
| 2012-01-16 | 2,064 | 19 | 156,283 | 246 | $0.015776 | mixed (-0.45) | Inequality and mobility; Jobs; Markets and regulation |
| 2012-01-17 | 2,737 | 26 | 222,179 | 252 | $0.022369 | mixed (-0.35) | Inequality; Regulation and taxation; Debt and finance |
| 2012-01-18 | 1,324 | 29 | 90,086 | 306 | $0.009192 | mixed (-0.20) | Internet censorship; Global labor; Inequality |
| 2012-01-19 | 3,103 | 25 | 252,126 | 170 | $0.025315 | mixed (-0.35) | Inequality; Financial regulation; Education and student debt |
| 2012-01-20 | 2,439 | 24 | 180,966 | 181 | $0.018205 | mixed (-0.35) | Megaupload; SOPA/PIPA; Financial regulation |
| 2012-01-21 | 1,694 | 12 | 125,785 | 212 | $0.012706 | mixed (-0.35) | Internet copyright; Inequality and taxation; Austerity |

## 6. 출력 품질 점검

JSON Schema와 길이 제한은 21건 모두 통과했지만, 일부 `topics`·`keywords`에서 다음
문제를 확인했다.

- 영문 label 뒤에 다른 문자권 문자가 붙은 사례
- zero-width Unicode 문자가 반복된 사례
- 여러 keyword가 한 문자열에 합쳐진 사례
- `keep short` 같은 생성 과정의 메타 문구가 label에 포함된 사례

이는 구조 검증만으로 의미 품질을 보장할 수 없다는 증거다. 월간 요청에는 다음 후처리를
적용했다.

1. NFKC 정규화 후 Unicode `Cf`/제어문자 제거
2. topic·keyword label의 허용 문자와 단어 수 검사
3. 쉼표 없이 지나치게 긴 label과 메타 문구 차단
4. 비정상 label만 제외하되 원래 일별 응답은 보존
5. 정제 전후 건수와 제외 사유 기록

## 7. 후속 완료 결과

- 22~31일 결과 다운로드·검증 완료
- 31개 `event_id`의 날짜 연속성·유일성·누락 검증 완료
- 의미 품질 필터를 적용한 월간 입력 생성 완료
- 일별 31개 응답을 사용한 1월 통합 Batch 1건 완료
- 최종 실제 비용과 Langfuse usage를 최종 보고서에 반영

최종 수치와 결과는 [1월 전체 결과](economy-social-results-01-31.md)에 정리했다.

OpenAI Batch 결과는 출력 순서가 입력 순서와 다를 수 있으므로 `custom_id`로 manifest와
대조했다. 공식 절차는 [OpenAI Batch API](https://developers.openai.com/api/docs/guides/batch)를
기준으로 했다.
