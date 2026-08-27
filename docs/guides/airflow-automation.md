# Airflow 기반 Spark batch 자동화

## 목적

기존 `spark_jobs.process_sample` 처리 코드를 수정하지 않고 Airflow에서 반복 실행합니다. 실행할 때 JSONL 입력 파일, 실행 이름, 출력 형식, partition 수와 Spark master를 바꿀 수 있습니다.

```text
Airflow 수동 실행 + Params
→ prepare_parameters
→ run_existing_spark_job
→ verify_row_accounting
→ data/airflow-output/<run_label>/<airflow_run_id>/
```

핵심 파일은 다음과 같습니다.

| 파일 | 역할 |
|---|---|
| `dags/spark_parameterized_batch.py` | DAG, 입력 Param과 세 task 정의 |
| `orchestration/spark_batch.py` | 경로 검증, 기존 Spark CLI 실행, 결과 행 회계 검사 |
| `spark_jobs/process_sample.py` | 기존 Spark 데이터 계약 검사·품질 판정·중복 제거·저장 코드 |
| `infra/airflow/Dockerfile` | Java와 PySpark가 포함된 Airflow 실행 이미지 |
| `infra/airflow/docker-compose.airflow.yml` | 과제 검증용 단일 노드 Airflow 환경 |

## 실행 준비

대용량 원본과 실행 결과는 `.gitignore`의 `data/` 규칙으로 Git에 올라가지 않습니다. 아래 명령은 공개 가능한 결정적 합성 입력을 로컬에 생성합니다.

```bash
python -m jobs.generate_synthetic_events \
  --count 100 \
  --output data/airflow-input/synthetic-100.jsonl

python -m jobs.generate_synthetic_events \
  --count 1000 \
  --output data/airflow-input/synthetic-1000.jsonl
```

Airflow를 시작합니다.

```bash
docker compose -f infra/airflow/docker-compose.airflow.yml up --build -d
docker compose -f infra/airflow/docker-compose.airflow.yml logs airflow
```

로그에 출력된 관리자 계정으로 `http://localhost:8082`에 로그인합니다. 개발·과제 검증용 `airflow standalone` 구성이며 운영 배포 구성이 아닙니다.

## 파라미터를 바꾼 두 번의 실행

UI에서 `spark_parameterized_text_batch`를 찾아 활성화한 뒤 **Trigger DAG w/ config**에서 값을 입력합니다.

첫 실행:

```json
{
  "input_file": "data/airflow-input/synthetic-100.jsonl",
  "run_label": "assignment-100",
  "output_root": "data/airflow-output",
  "output_format": "parquet",
  "partitions": 2,
  "spark_master": "local[2]"
}
```

두 번째 실행:

```json
{
  "input_file": "data/airflow-input/synthetic-1000.jsonl",
  "run_label": "assignment-1000",
  "output_root": "data/airflow-output",
  "output_format": "parquet",
  "partitions": 4,
  "spark_master": "local[2]"
}
```

입력 파일과 partition 값을 바꾸어도 DAG와 Spark 코드는 고치지 않습니다. 입력과 출력 경로는 프로젝트 밖으로 벗어날 수 없게 검사하며, shell 문자열 대신 인자 배열로 기존 CLI를 호출합니다.

## 결과 확인과 제출 증거

각 실행은 다음 파일을 별도 디렉터리에 만듭니다.

```text
data/airflow-output/<run_label>/<airflow_run_id>/
├── output/
│   ├── events/
│   └── rejected/
├── report.json
└── run.log.jsonl
```

마지막 `verify_row_accounting` task는 `input_rows == accounted_rows`인지 확인하며 불일치하면 DAG를 실패시킵니다. 제출할 때 다음 세 가지를 준비합니다.

1. GitHub의 DAG 및 Airflow 구성 코드 링크
2. 두 DAG run이 성공한 Graph 또는 Grid 화면 캡처
3. 각 run의 `verify_row_accounting` 로그와 `report.json` 주요 결과

`data/`의 입력과 결과는 제출하지 않고, 개인정보·비밀키가 없는 결과 집계만 `analysis/reports/airflow-assignment-validation.md`에 옮겨 기록합니다.

## 종료

```bash
docker compose -f infra/airflow/docker-compose.airflow.yml down
```

Airflow metadata를 포함한 named volume까지 지우려는 경우에만 `down --volumes`를 사용합니다.
