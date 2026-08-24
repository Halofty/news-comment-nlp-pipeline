# Spark 1,000건 운영 로그 점검

- 실행일: 2026-08-23
- 입력: 결정적 합성 `TextEvent v1` 1,000건
- 로그: [`spark-1000-run-log.jsonl`](spark-1000-run-log.jsonl)
- 원본 실행 로그 SHA-256: `7da4fc5cd578b1551a33d62a507f005830a260bfe003097c4cfe6ac32b06a9b7`
- 점검 결과: **통과**

## 확인 결과

| 항목 | 결과 |
|---|---:|
| 로그 이벤트 | 10개, sequence 1~10 연속 |
| 실행 ID | 단일 `run_id` |
| 입력 | 1,000건 |
| 계약 거절 | 0건 |
| 중복 | 19건 |
| 고유 유효 출력 | 981건 |
| 행 회계 | 1,000/1,000 |
| 품질 상태 합계 | 981/981 |
| 원문 관련 payload key | 0개 |

Spark session 기동 이후 집계 완료까지 12.519초, 프로세스 시작부터 session 종료와 보고서 기록까지 25.867초가 걸렸습니다. 기존 처리 보고서의 12.519초는 Spark session 준비 시간을 제외하므로, 운영 지연을 판단할 때는 전체 로그 시간도 함께 봐야 합니다.

로그에는 원문, 제목, URL, 작성자, 커뮤니티, 이벤트 ID와 자유 형식 metadata를 기록하지 않습니다. 입력 checksum, 단계별 건수·소요 시간, 품질 집계와 실행 환경만 남깁니다.

저장소의 공개 사본은 원본과 JSON 레코드가 같으며 Git에서 다루기 쉽도록 줄바꿈만 정규화했습니다. 따라서 위 SHA-256은 `data/`에 보존된 원본 실행 파일을 가리킵니다.

## 재현과 자동 점검

```bash
python -m spark_jobs.process_sample \
  --input data/spark-input/synthetic-1000.jsonl \
  --output data/spark-output/synthetic-1000-logged \
  --report data/spark-reports/synthetic-1000-logged-report.json \
  --log data/spark-logs/synthetic-1000-run.jsonl \
  --format jsonl \
  --master "local[2]"

python -m jobs.validate_run_log \
  --log data/spark-logs/synthetic-1000-run.jsonl \
  --report data/spark-reports/synthetic-1000-logged-report.json
```

실패 실행은 마지막에 `run_failed`와 예외 유형만 남기며 예외 메시지는 기록하지 않습니다. 현재 로그는 드라이버 단계 관측용이며 Spark executor 상세 진단 로그나 장기 보관용 중앙 로그 수집기는 아직 포함하지 않습니다.
