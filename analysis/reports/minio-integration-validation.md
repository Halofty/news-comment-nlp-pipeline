# MinIO Object Storage 통합 검증

- 검증일: 2026-09-03 KST
- MinIO bucket: `news-raw`, `news-processed`, `news-llm`, `news-reports`, `news-checkpoints`
- Spark: 3.5.7 / Hadoop client 3.3.4

## 1. Python adapter와 fixture 무결성

`storage/object_store.py`는 path-style S3 client를 만들고 객체의 byte 크기와 SHA-256
metadata를 대조한다. `sample/synthetic-events.jsonl`을 같은 key로 두 번 업로드한 뒤
다운로드했다.

| 항목 | 결과 |
|---|---|
| bucket/key | `news-raw/fixtures/text-events/synthetic-events.jsonl` |
| 크기 | 994 bytes |
| SHA-256 | `5b0fb18cf4dcb27cd01fdc1eb4160c7c23980e7417dcbb0544733ab826aaf31c` |
| 첫 업로드 | `uploaded` |
| 동일 파일 재업로드 | `unchanged` |
| 다운로드 검증 | 크기·SHA-256 일치 |

동일 파일은 `head_object`의 크기와 `sha256` metadata가 모두 일치하면 실제 업로드를
생략한다. 변경된 파일은 같은 key에 덮어쓴 뒤 다시 검증한다.

## 2. Spark S3A 읽기·쓰기

Spark 이미지에 Hadoop 3.3.4와 맞는 `hadoop-aws 3.3.4`,
`aws-java-sdk-bundle 1.12.262`를 고정했다. 로컬 MinIO는 HTTP, path-style access와
`SimpleAWSCredentialsProvider`를 사용한다.

```text
s3a://news-raw/fixtures/text-events/synthetic-events.jsonl
→ Spark text read: 2행
→ Parquet write
→ s3a://news-processed/fixtures/text-events/synthetic-events-parquet
→ Spark Parquet read: 2행
```

| 지표 | 결과 |
|---|---:|
| 입력 행 | 2 |
| 출력 행 | 2 |
| 누락 | 0 |
| Local 실행 시간 | 3.805초 |
| Standalone Master·Worker 실행 시간 | 5.726초 |
| Standalone Executor | Worker 1개 / 2 cores |
| 실행 상태 | `completed` |

## 3. Airflow 통합 실행

`reddit_spark_llm_pipeline`에 `store_spark_output_in_minio` task를 추가했다.

```text
Reddit 수집 100건
→ Spark 처리·저장 100건
→ MinIO news-processed 동기화
→ LLM 요청 10건 생성·예산 검사
→ submit=false dry-run
```

| 지표 | 결과 |
|---|---:|
| Spark 입력·고유 저장 | 100 / 100 |
| MinIO 객체 | 2 |
| MinIO 저장 크기 | 128,906 bytes |
| LLM 요청 준비 | 10 |
| DAG import 오류 | 0 |
| 최종 DAG 상태 | `success` |

객체별 key·크기·SHA-256은 run 디렉터리의 `minio-storage.json`에 저장한다. Airflow
XCom에는 객체 목록을 넣지 않고 bucket, prefix, 객체 수와 총 byte만 전달한다.

## 4. 실행 명령

```bash
docker compose up -d --wait minio minio-init

python -m jobs.minio_storage upload \
  --file sample/synthetic-events.jsonl \
  --key fixtures/text-events/synthetic-events.jsonl

docker compose build spark-runner
docker compose run --rm --no-deps spark-runner \
  --master local[2] spark_jobs/minio_roundtrip.py \
  --master local[2] \
  --report data/minio-validation/spark-s3a.json

docker compose up -d --wait spark-master spark-worker
docker compose run --rm --no-deps --use-aliases spark-runner \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-runner \
  spark_jobs/minio_roundtrip.py \
  --master spark://spark-master:7077 \
  --report data/minio-validation/spark-s3a-standalone.json
```

Airflow 컨테이너는 Linux Docker host gateway를 통해 `localhost:9000`의 MinIO에
접근한다. DAG의 `minio_enabled=false`를 지정하면 object storage task를 명시적으로
건너뛸 수 있다.

## 5. 전체 데이터 확대 완료

이 fixture 검증 뒤 정식 `data/` 산출물 전체를 bucket별로 복사하고 새 DAG의 raw,
processed, LLM 산출물 자동 게시를 검증했다. 상세 결과는
[MinIO 전체 데이터 이전 검증](minio-data-migration-validation.md)에 정리했다.

남은 독립 범위는 Structured Streaming checkpoint의 MinIO 전환 시험이다.

기존 로컬 원본은 자동으로 삭제하지 않는다. 로컬 삭제는 object checksum·행 수와
복구 실행을 확인한 뒤 별도 작업으로 진행한다.
