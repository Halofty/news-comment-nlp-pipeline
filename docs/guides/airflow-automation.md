# Airflow 기반 Reddit 일별 수집·Spark 자동화

## 실행

```bash
export AIRFLOW_UID="$(id -u)"
docker compose -f infra/airflow/docker-compose.airflow.yml up --build -d
docker compose -f infra/airflow/docker-compose.airflow.yml logs airflow
```

`http://localhost:8082`에서 `reddit_daily_spark_batch`와 `gdelt_daily_spark_batch`를 활성화합니다.

## 변경 전: 2016-01-01

```json
{
  "start_date": "2016-01-01",
  "end_date": "2016-01-01",
  "limit": 1000,
  "output_root": "data/airflow-output",
  "output_format": "parquet",
  "partitions": 2,
  "spark_master": "local[2]"
}
```

## 변경 후: 2016-02-01

```json
{
  "start_date": "2016-02-01",
  "end_date": "2016-02-01",
  "limit": 1000,
  "output_root": "data/airflow-output",
  "output_format": "parquet",
  "partitions": 2,
  "spark_master": "local[2]"
}
```

두 실행은 날짜만 다릅니다. 날짜에서 `RC_YYYY-MM.parquet`를 결정하고 UTC 하루 범위만 필터링합니다. `start_date`와 `end_date`가 다르면 첫 task에서 실패합니다.

전체 일일 backfill에서는 `limit`을 `0`으로 지정합니다. 이 값은 무제한을 뜻하며, 1~99 또는 10,000 초과 양수는 허용하지 않습니다. 대규모 원격 월 파일은 먼저 로컬 Parquet로 준비한 뒤 Collector의 `--input-parquet` 옵션을 사용하면 날짜 predicate pushdown을 적용할 수 있습니다.

성공 후 다음을 확인합니다.

1. `collect_reddit_day`의 수집 건수
2. `run_existing_spark_job`의 Parquet와 report
3. `verify_row_accounting`의 입력·설명 행 수
4. 두 날짜 run의 Grid 또는 Graph 화면

## GDELT 날짜 실행

GDELT DAG도 같은 Spark·출력 설정을 사용하고 다음 값만 날짜별로 변경합니다.

```json
{
  "start_date": "2026-08-14",
  "end_date": "2026-08-14",
  "query": "artificial intelligence",
  "max_records": 100,
  "output_root": "data/airflow-output",
  "output_format": "parquet",
  "partitions": 2,
  "spark_master": "local[2]"
}
```

두 번째 실행에서는 `start_date`와 `end_date`만 `2026-08-15`로 바꿉니다. Collector는 유효 이벤트 100건을 안정적으로 확보하기 위해 API에 10건을 추가 요청합니다. 이 환경에서는 HTTPS TLS 연결이 실패하므로 HTTP endpoint를 사용합니다.
