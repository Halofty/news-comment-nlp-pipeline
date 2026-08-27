# Airflow 기반 Reddit 일별 수집·Spark 자동화

## 실행

```bash
export AIRFLOW_UID="$(id -u)"
docker compose -f infra/airflow/docker-compose.airflow.yml up --build -d
docker compose -f infra/airflow/docker-compose.airflow.yml logs airflow
```

`http://localhost:8082`에서 `reddit_daily_spark_batch`를 활성화합니다.

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

성공 후 다음을 확인합니다.

1. `collect_reddit_day`의 수집 건수
2. `run_existing_spark_job`의 Parquet와 report
3. `verify_row_accounting`의 입력·설명 행 수
4. 두 날짜 run의 Grid 또는 Graph 화면
