# 최신 end-to-end 실행 기록

## 실행 식별자

| 항목 | 값 |
|---|---|
| DAG | `news_comment_end_to_end_pipeline` |
| Run ID | `manual__2026-09-06T17:41:03.997288+00:00` |
| 데이터 범위 | 2012-08-01~2012-10-31, 양 끝 포함 92일 |
| 대주제 | `economy` |
| Reddit 제한 | 없음 (`limit=0`) |
| OpenAI 제출 | 실제 제출 (`submit=true`) |
| prompt | `group-daily-v3-emotional-tones-compact-source-balanced` |

이 Run은 날짜별 mapped task 92개를 만들었다. 최종 상태는 `success`이며 10개 task
종류의 모든 instance가 성공했다.

## 단계별 결과

| 단계 | task instance | 처리·저장 결과 |
|---|---:|---:|
| 날짜 설정 | 1 | 92개 날짜 설정 생성 |
| 원본 수집·병합 | 92 | 뉴스 7,343 + Reddit 296,053건 |
| Spark 처리 | 92 | 입력 303,396건 |
| Spark 고유 저장 | 92 | 303,396건 |
| 계약 거부 / 중복 | 92 | 0 / 0건 |
| MinIO 게시 | 92 | 920개 객체, 206,879,418 bytes |
| LLM 요청 준비 | 92 | 날짜별 economy 요청 1개 |
| OpenAI Batch | 92 | 완료 92, 실패 0 |
| PostgreSQL 분석 저장 | 92 | 이 Run 92건 |
| serving snapshot 읽기 | 92 | ready 92개 |

Spark application별 실행 시간의 합은 624.603초다. mapped task가 일부 병렬로
실행되므로 이 값은 DAG wall-clock 시간과 같지 않다.

## LLM 사용량

| 지표 | 값 |
|---|---:|
| 입력 token | 20,265,672 |
| 출력 token | 52,166 |
| 기록된 비용 | $2.545403 |
| PostgreSQL 전체 누적 분석 | 187건 |
| 최신 source-balanced v3 누적 | 93건 |

각 Batch는 Reddit과 web news를 먼저 출처별로 해석하고, 두 출처가 모두 있으면
출처 수준 결론에 각각 50% 가중치를 적용한다. 행 수나 token 수가 많은 Reddit이
뉴스보다 더 큰 출처 비중을 갖지 않도록 한 설정이다.

## 실패와 복구 기록

처음 실행에서 세 날짜의 v3 응답이 tone share 합계 검증을 통과하지 못했다.

| 날짜 | 원인 | 복구 |
|---|---|---|
| 2012-08-30 | negative tone 합계 0.65 | 허용 범위 안의 비정규 합계를 1.0으로 정규화 |
| 2012-08-31 | positive 0.72, negative 0.58 | 동일 Batch 결과 재사용 후 정규화·재검증 |
| 2012-10-11 | positive tone 합계 0.40 | 동일 Batch 결과 재사용 후 정규화·재검증 |

빈 tone 배열이 아닌 경우 합계가 `0 < sum <= 1.25`이면 1.0으로 정규화하고, 0 또는
1.25 초과는 계속 거부한다. 복구 시 완료된 OpenAI Batch ID를 재사용해 유료 요청을
새로 만들지 않았고, 해당 task부터 재실행했다. 복구 후 92개 응답 모두 검증·저장됐으며
요청 실패·누락·중복은 0건이다.

Airflow metadata의 최종 복구 구간은 2026-09-06 23:06:00~23:06:51 UTC로 약 51.4초다.
원래 제출부터 수동 원인 분석과 복구까지의 경과 시간에는 Batch 대기와 사람의 개입
시간이 포함되므로 순수 처리 성능으로 사용하지 않는다.

## 최종 결과 확인

- Airflow: Grid의 10개 task 종류와 mapped instance 92개가 모두 `success`
- MinIO: 날짜와 Run ID prefix 아래 object와 checksum 기록 확인
- PostgreSQL: 이 Run 분석 92건, 전체 누적 187건 확인
- Langfuse: 날짜별 generation token·비용 trace 확인
- Slack: 날짜별 완료 알림 확인
- Streamlit: v3 결과, 감정 분포, polarization, positive/negative tones, topic 조회

발표 화면은 저장소의 `docs/streamlit_result.png`를 사용한다. 실행 절차는
[현재 end-to-end 실행 방법](../guides/end-to-end-execution.md)에 있다.
