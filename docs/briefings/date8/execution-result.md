# Date 8 end-to-end·서빙 실행 결과

## 실행 개요

> 이 절은 최종 단일 DAG로 통합하기 전 `reddit_spark_llm_pipeline`의 과거 실행 기록이다.

2026-09-05 KST에 Airflow DAG `reddit_spark_llm_pipeline`을 Run ID
`date8-serving-demo-20260905`로 실행했다. 발표 중 유료·중복 요청을 만들지 않도록
`submit=false`로 두고 수집부터 최종 저장 결과 읽기까지 연결했다.

```text
Reddit 날짜 입력
→ Collector 100건
→ Spark 계약·품질 처리
→ MinIO 결과 저장
→ LLM 요청 10건 생성·예산 검사
→ dry-run 제출 검증
→ 저장된 Spark report 재조회
→ serving-snapshot.json
→ Streamlit 표시
```

## 실행 Param

| Param | 값 |
|---|---|
| 날짜 | `2016-01-01` 하루 |
| 수집 제한 | 100건 |
| Spark | `local[1]`, JSONL, 1 partition |
| LLM 준비 제한 | 10건 |
| OpenAI 제출 | `false` |
| MinIO 게시 | `true` |

## 단계별 결과

| 단계 | 상태 | 결과 |
|---|:---:|---|
| `prepare_parameters` | success | 날짜·출력 경로·Run ID 확정 |
| `collect_reddit_day` | success | Reddit 댓글 100건 수집 |
| `run_spark` | success | 입력 100, 고유 유효 100 |
| `verify_spark` | success | 입력 회계 100 = 처리 회계 100 |
| `store_spark_output_in_minio` | success | 객체 2개, 128,906 bytes |
| `prepare_llm_parameters` | success | Spark 결과를 LLM 입력으로 연결 |
| `build_and_budget_check` | success | 요청 10건, 예산 검사 통과 |
| `submit_or_dry_run` | success | dry-run, 외부 API 제출 0건 |
| `verify_pipeline` | success | Spark·MinIO·LLM 단계 통합 검증 |
| `read_saved_result` | success | report 재조회 후 serving snapshot 저장 |

전체 DAG Run은 UTC 15:26:49부터 15:27:33까지 약 43초 걸렸고 10개 task가 모두
성공했다. Spark 자체 처리 시간은 5.708초였다.

## 최종 저장·읽기 결과

| 지표 | 값 |
|---|---:|
| 입력·회계 행 | 100 / 100 |
| 계약 거부·중복 | 0 / 0 |
| 품질 accept·quarantine | 99 / 1 |
| MinIO 객체·용량 | 2 / 128,906 bytes |
| LLM 요청 준비·skip | 10 / 0 |
| PostgreSQL 분석 결과 | 32행 |
| PostgreSQL Batch 결과 | 32행 |

Streamlit은 PostgreSQL에서 분석 32행과 Batch 32행을 읽었으며, 감정 분포는
`mixed` 27건·`negative` 5건이었다. `read_saved_result`가 생성한 snapshot도 같은
화면에서 읽는다.

실행 산출물은 Git에서 제외되는 `data/airflow-output/date8-demo/`에 저장되고 MinIO에도
복사된다. 공개 가능한 실행 수치는 이 문서에 남긴다.

## Slack 알림 검증 상태

완료된 실제 Batch `batch_6a987...`를 Airflow에서 preview 모드로 조회했다. Run
`date8-slack-preview-mode-20260905`는 약 2초 만에 성공했고 manifest 1행·결과 1행·
검증 1행·실패와 누락 0행을 확인했다. 경제 정체와 불평등 등 상위 topic 5개가 알림
payload에 포함됐다. Airflow import 오류도 0건이다.

이후 `.env`에 실제 `SLACK_WEBHOOK_URL`을 설정하고 Airflow Run
`date8-slack-webhook-20260905`를 실행했다. Run은 약 6초 만에 성공했으며 상태 파일은
`delivery_mode=slack_webhook`, `notification_status=sent`로 기록됐다.

- 코드·DAG·실제 완료 Batch 기반 preview payload: 완료
- 자동 테스트: 완료
- 실제 Slack Incoming Webhook POST: 완료
- 발표 화면에서는 해당 Slack 채널의 수신 메시지를 최종 확인

## 검증 명령 결과

| 검사 | 결과 |
|---|---|
| 전체 pytest | `127 passed` |
| Airflow DAG import | `[]` |
| end-to-end DAG | success, 10/10 tasks |
| Dashboard DB 조회 | analyses 32, batches 32 |
| Batch Slack preview | success, 결과 1/1 검증, topic 5개 |
| Batch Slack webhook | success, 약 6초, `slack_webhook`/`sent` |

알림 표시를 보완한 Run `date8-slack-timing-v2-20260905`도 약 4초 만에 성공했다.
manifest에서 데이터 날짜 `2012-01-01`과 대주제 `사회·경제`를 읽었고,
`started:slack_webhook`과 `completed:slack_webhook` 두 이벤트를 각각 저장했다. 시작
알림에는 `in_progress`·시작 시각을, 완료 알림에는 시작·종료 시각·소요 시간과 검증
건수·상위 topic을 표시했다.

## 2012-02-01 원격 수집부터 단일 Run 재현

날짜를 코드에서 바꾸지 않고 Airflow DAG Run Param으로 `2012-02-01`을 주입했다.
`reddit_source_mode=remote`를 사용해 로컬의 월 Parquet를 재사용하지 않고 원격 Reddit
archive에서 해당 일자의 파일을 만드는 단계부터 시작했다.

| 항목 | 결과 |
|---|---:|
| Airflow Run | `date8-reproduce-2012-02-01` |
| 전체 task | 10/10 success |
| 전체 실행 시간 | 약 38초 |
| 원격 수집 | 100건, 약 20.6초 |
| 수집 파일 날짜 검증 | `2012-02-01`만 존재 |
| Spark 입력·회계·고유 저장 | 100 / 100 / 100 |
| 품질 accept·quarantine | 98 / 2 |
| 계약 거부·중복 | 0 / 0 |
| Spark 처리 시간 | 6.047초 |
| MinIO 저장 | 2객체, 138,378 bytes |
| LLM 요청 준비 | 10건, skip 0, 예산 `ok` |
| OpenAI 제출 | `dry_run` |

이 표의 실행은 단일 날짜 버전에서 수행한 과거 기록이다. 현재 DAG에서는
`start_date`와 `end_date`로 범위를 지정하면 날짜별 mapped task와 날짜별 독립 OpenAI
Batch로 확장된다. Airflow Variable보다 Run Param을 선택한 이유는 실행별 입력값이 Run
이력에 함께 남고 동시에 실행되는 서로 다른 날짜가 전역값을 덮어쓰지 않기 때문이다.

## 남은 보장 범위

- 이번 데모는 발표 안전성을 위해 OpenAI 제출을 수행하지 않았다. 실제 LLM 32건의
  완료·적재·비용 결과는 Date 7 실험을 근거로 사용한다.
- Slack Webhook POST는 검증했지만 메시지 보존·검색·재전송 보장은 Slack Workspace 정책에
  따른다.
- Airflow polling task는 Batch가 끝날 때까지 worker slot을 점유한다. 운영 확장에서는
  OpenAI webhook 또는 deferrable sensor가 적절하다.
- Streamlit은 로컬 read-only 발표용이며 인증·TLS·다중 사용자 운영은 범위 밖이다.
