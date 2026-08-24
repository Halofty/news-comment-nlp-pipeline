# Spark 100·1,000건 batch 검증

- 실행일: 2026-08-23
- 입력: 결정적으로 생성한 공개 가능 `TextEvent v1` 합성 데이터
- 환경: Python 3.11.9, PySpark 3.5.9, Java 17.0.19, `local[2]`
- Schema: [`../../spark_jobs/schemas.py`](../../spark_jobs/schemas.py)
- 변환: [`../../spark_jobs/transformations.py`](../../spark_jobs/transformations.py)
- 품질 정책: [`../quality/text-quality-rules.md`](../quality/text-quality-rules.md)

## 목적

100건 MVP와 1,000건 확장 검증이 서로 다른 코드가 되지 않도록 동일한 생성기, 명시적 Spark Schema, transformation과 CLI를 사용했습니다. 입력에는 정상 뉴스·댓글과 함께 중복 ID, zero-width·제어 문자, 반복, URL 과다, 개인정보 후보, 연속 결합문자와 64 KiB 초과 텍스트를 일정한 위치에 포함했습니다.

실제 Reddit 원문은 공개 저장소에 포함하지 않으므로 이 실행은 처리 정확성과 재현성 검증용입니다. 실제 분포나 운영 성능을 대표하지 않습니다.

## 실행 명령

```bash
python3 -m jobs.generate_synthetic_events \
  --count 100 \
  --output data/spark-input/synthetic-100.jsonl

python3 -m spark_jobs.process_sample \
  --input data/spark-input/synthetic-100.jsonl \
  --output data/spark-output/synthetic-100 \
  --report data/spark-reports/synthetic-100-report.json \
  --log data/spark-logs/synthetic-100-run.jsonl \
  --format jsonl \
  --master "local[2]"
```

같은 명령에서 `100`을 `1000`으로 바꾸어 확장 실행했습니다. 입력 SHA-256은 각각 [`spark-100-profile.json`](spark-100-profile.json)과 [`spark-1000-profile.json`](spark-1000-profile.json)에 기록했습니다.

1,000건 재실행의 단계별 운영 로그와 자동 점검 결과는 [`spark-run-log-review.md`](spark-run-log-review.md)에 기록했습니다.

## 결과 비교

| 지표 | 100건 | 1,000건 |
|---|---:|---:|
| 입력 행 | 100 | 1,000 |
| Schema parsing 성공 | 100 | 1,000 |
| 계약 오류 | 0 | 0 |
| 중복 `event_id` | 1 | 19 |
| 고유 출력 | 99 | 981 |
| 행 회계 | 100/100 | 1,000/1,000 |
| `accept` | 90 | 891 |
| `flag` | 5 | 50 |
| `quarantine` | 3 | 30 |
| `reject` | 1 | 10 |
| 최대 문자 수 | 16,385 | 16,385 |
| 최대 UTF-8 byte | 65,540 | 65,540 |
| 처리 partition | 2 | 4 |
| 실행 시간 | 11.005초 | 11.954초 |

품질 상태 합계는 중복 제거 후 출력 행 수와 일치합니다. 입력 행은 모두 고유 출력, 중복 또는 계약 오류 중 하나로 설명되며 유실된 행이 없습니다.

## 확장성 판단

- 입력 크기에 따라 250행당 1개를 기준으로 2~64개의 처리 partition을 선택하며 CLI에서 재정의할 수 있습니다.
- transformation은 batch와 Structured Streaming DataFrame 모두에 적용할 수 있도록 SparkSession이나 파일 경로를 직접 참조하지 않습니다.
- 품질 fixture와 정확히 같은 판정을 보장하기 위해 현재는 scalar Python UDF를 사용합니다. 1,000건에서는 문제가 없었지만 대용량 전환 전 native Spark 식 또는 Pandas UDF와 처리량을 비교해야 합니다.
- Windows 로컬 환경에는 공식 Hadoop native helper가 없어 `toLocalIterator()`로 partition을 순차 소비하는 단일 JSONL sink를 사용했습니다. 전체 결과를 메모리에 수집하지 않지만 분산 sink는 아닙니다.
- Linux·클러스터 환경에서는 CLI의 기본 `parquet` sink와 `source_type` partitioning을 사용합니다.
- 100건과 1,000건 실행 시간 차이가 작은 이유는 데이터 처리보다 Spark JVM 시작·종료 비용의 비중이 크기 때문입니다. 이 결과만으로 선형 확장이나 처리량을 주장하지 않습니다.

## 다음 전환 지점

Kafka Structured Streaming에서는 `readStream`으로 `raw-text` value를 읽어 동일한 명시적 Schema와 `transform_events()`를 적용합니다. 이후 event-time watermark, checkpoint와 `foreachBatch` PostgreSQL upsert를 추가합니다. 정확한 `event_id` 중복 제거 기준은 이번 batch 구현과 동일하게 유지하되 streaming에서는 watermark 범위와 state 크기를 함께 결정해야 합니다.
