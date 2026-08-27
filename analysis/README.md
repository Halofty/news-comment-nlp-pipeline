# 분석 및 데이터셋 메타데이터

이 디렉터리에는 원문 데이터가 아니라 공개 가능한 데이터셋 명세, 기계 판독 메타데이터와 집계 검증 결과를 저장합니다. 실제 뉴스·댓글 원문과 실행 중 생성한 파일은 `data/`에 저장하며 Git에 포함하지 않습니다.

## 디렉터리 구성

```text
analysis/
├── README.md
├── datasets/
│   ├── gdelt-news.md
│   ├── reddit-comments.md
│   ├── dataset-catalog.yaml
│   └── dataset-catalog.schema.json
├── quality/
│   ├── validation-summary.md
│   ├── text-quality-rules.md
│   ├── text-quality-fixtures.jsonl
│   └── text-quality-fixtures.schema.json
└── reports/
    ├── gdelt-sample-profile.json
    ├── reddit-sample-profile.json
    ├── spark-100-profile.json
    ├── spark-1000-profile.json
    ├── spark-batch-validation.md
    ├── spark-1000-run-log.jsonl
    ├── spark-run-log-review.md
    ├── spark-streaming-consumer-run-log.jsonl
    ├── spark-streaming-consumer-restart-log.jsonl
    ├── spark-streaming-consumer-dlq-log.jsonl
    ├── spark-streaming-consumer-validation.md
    ├── langfuse-sample-trace.jsonl
    └── langfuse-token-validation.md
```

## 문서별 책임

| 위치 | 책임 |
|---|---|
| `datasets/*.md` | 출처, 이용 조건, 원본 구조, 프로젝트 사용 범위와 알려진 한계를 사람이 읽을 수 있게 설명 |
| `dataset-catalog.yaml` | 프로그램이 읽을 수 있는 데이터셋별 출처·범위·필드·검증 상태 |
| `dataset-catalog.schema.json` | 카탈로그 구조와 필수 필드를 JSON Schema Draft 2020-12로 검증 |
| `reports/*.json` | 특정 표본 실행에서 얻은 공개 가능한 집계 결과와 재현 명령 |
| `reports/spark-batch-validation.md` | 동일한 Spark 코드로 실행한 100·1,000건 결과 비교와 확장 판단 |
| `reports/spark-1000-run-log.jsonl` | 원문을 제외한 1,000건 Spark 실행의 단계별 운영 로그 |
| `reports/spark-run-log-review.md` | 이벤트 순서·행 회계·시간·민감 payload 미기록 점검 결과 |
| `reports/spark-streaming-consumer-*.jsonl` | Kafka 1,000건 처리·checkpoint 재시작·DLQ 검증의 원문 제외 운영 로그 |
| `reports/spark-streaming-consumer-validation.md` | 실제 Kafka→Spark 처리 행 회계, 재시작과 malformed JSON DLQ 결과 |
| `reports/langfuse-sample-trace.jsonl` | 원문 없이 합성 Batch·단계·generation·token 대조를 기록한 관측 로그 |
| `reports/langfuse-token-validation.md` | 합성 3건의 token·비용·재시도, SDK와 fallback 검증 결과 |
| `quality/validation-summary.md` | 데이터셋별 검증 결과와 아직 해소하지 못한 조건 요약 |
| `quality/text-quality-rules.md` | 커뮤니티 텍스트의 측정값·임계값·상태·Spark 출력 규격 |
| `quality/text-quality-fixtures.jsonl` | 정상 다국어와 경계·악성 입력의 기계 판독 기대 결과 |
| `docs/architecture/data-contract.md` | 두 출처를 통합한 `TextEvent v1` 출력 필드의 의미와 변환 계약 |

데이터셋 명세는 원본을 설명하고, 데이터 계약은 Collector가 만든 표준 출력물을 설명합니다. 원본 명세가 바뀌어도 `TextEvent v1`의 의미가 자동으로 바뀌지는 않습니다.

## 검증

```bash
python3 -m pytest -q \
  tests/test_dataset_metadata.py \
  tests/test_text_quality.py
```

이 테스트는 YAML 카탈로그와 품질 fixture가 각 JSON Schema를 통과하는지, 참조한 로컬 문서와 profile이 존재하는지, profile 집계와 품질 기대 판정이 서로 모순되지 않는지 확인합니다.

## 갱신 규칙

- 공식 데이터 페이지와 이용 조건의 확인 날짜를 함께 갱신합니다.
- 실제 표본을 다시 만들면 기존 숫자를 덮어쓰기 전에 실행 조건과 검증 날짜가 같은지 확인합니다.
- 검증을 수행하지 못한 데이터셋은 통계를 추정하지 않고 `blocked` 또는 `not_run`으로 기록합니다.
- 원문, 사용자명, 댓글 ID와 원문 URL 목록은 profile에 포함하지 않습니다.
