# Week 5 — Airflow를 이용한 Spark Batch 자동화

## 1. 과제 목표와 완료 상태

이번 과제의 목표는 Week 4에서 만든 Spark 처리 작업을 Airflow DAG로 자동화하고, 코드를 고치지 않은 채 입력값만 바꾸어 다시 실행하는 것입니다.

```text
Airflow Trigger + 실행 파라미터
→ 입력·출력 경로 검증
→ 기존 Spark batch 실행
→ 처리 결과와 행 회계 검증
→ 실행별 결과 저장
```

| 과제 요구사항 | 구현 상태 | 구현 근거 |
|---|---|---|
| 기존 수집·처리 코드를 Airflow DAG로 실행 | 구현 완료 | `spark_jobs.process_sample`을 DAG task에서 호출 |
| 코드를 수정하지 않고 입력값 변경 | 구현 완료 | JSONL 경로, 실행 이름, 출력 형식, partition, Spark master를 Param으로 입력 |
| 값을 바꾸어 한 번 더 실행 | 실행 대기 | 100건·1,000건 입력과 실행 설정 준비 완료 |
| DAG 코드 제출 | 준비 완료 | `dags/spark_parameterized_batch.py` |
| 실행 화면·로그·결과 제출 | 실행 대기 | Docker 권한이 있는 환경에서 두 DAG run 후 기록 |

현재 코드·테스트·실행 환경은 준비됐습니다. 이 개발 환경에서는 Docker socket 권한이 없어 실제 Airflow UI 실행 결과만 남아 있습니다.

## 2. 무엇을 자동화했는가

새로운 데이터 처리 로직을 DAG 내부에 다시 작성하지 않았습니다. Week 4에서 검증한 Spark CLI를 Airflow가 실행하고 결과를 확인하도록 구성했습니다.

```text
dags/spark_parameterized_batch.py
        │
        ▼
orchestration/spark_batch.py
        │  python -m spark_jobs.process_sample ...
        ▼
spark_jobs/process_sample.py
        │
        ├─ TextEvent v1 명시적 Schema parsing
        ├─ 계약·텍스트 품질 검사
        ├─ event_id 중복 제거
        ├─ Parquet 또는 JSONL 저장
        └─ report.json + run.log.jsonl 생성
```

이렇게 분리하면 Airflow는 실행 순서와 상태를 관리하고, 실제 데이터 처리 책임은 기존 Spark 코드가 유지합니다. 같은 Spark 작업은 Airflow 밖에서도 CLI로 실행할 수 있습니다.

## 3. DAG 구조

DAG ID는 `spark_parameterized_text_batch`이며 수동 trigger 방식입니다.

```text
prepare_parameters
        │
        ▼
run_existing_spark_job
        │
        ▼
verify_row_accounting
```

| task | 역할 | 실패 조건 |
|---|---|---|
| `prepare_parameters` | Param을 읽고 안전한 입력·출력 경로와 실행별 디렉터리 결정 | 파일 없음, 프로젝트 밖 경로, 허용하지 않은 확장자·형식 |
| `run_existing_spark_job` | 인자 배열로 기존 Spark CLI 실행 | Spark process 실패, 결과 report 미생성 |
| `verify_row_accounting` | report를 읽고 모든 입력 행이 설명되는지 검사 | 입력 0건, `input_rows != accounted_rows` |

DAG에는 retry 1회와 1분 간격을 설정했습니다. 과제의 필수 범위를 넘는 정기 schedule과 복잡한 backoff는 아직 적용하지 않았습니다.

## 4. 실행 시 바꿀 수 있는 값

| Param | 기본값 | 의미 |
|---|---|---|
| `input_file` | `sample/synthetic-events.jsonl` | 프로젝트 내부 `data/` 또는 `sample/`의 JSONL 입력 |
| `run_label` | `manual-sample` | 실행 목적과 입력 규모를 구분하는 이름 |
| `output_root` | `data/airflow-output` | 실행 결과를 저장할 프로젝트 내부 경로 |
| `output_format` | `parquet` | `parquet` 또는 `jsonl` |
| `partitions` | `2` | Spark 출력 partition 수, 1~64 |
| `spark_master` | `local[2]` | 과제용 컨테이너에서 사용할 Spark master |

Airflow의 DAG-level Param으로 타입, 범위와 일부 경로 형식을 먼저 검사합니다. 실행 helper에서도 실제 경로가 프로젝트 밖으로 빠져나가지 않는지 다시 검사합니다.

## 5. 파라미터를 바꾸는 두 번의 실행

### 5.1 입력 생성

```bash
python -m jobs.generate_synthetic_events \
  --count 100 \
  --output data/airflow-input/synthetic-100.jsonl

python -m jobs.generate_synthetic_events \
  --count 1000 \
  --output data/airflow-input/synthetic-1000.jsonl
```

합성 입력과 실행 결과가 위치하는 `data/`는 Git에서 제외됩니다.

### 5.2 첫 번째 실행 — 100건

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

### 5.3 두 번째 실행 — 1,000건

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

두 실행은 코드 변경 없이 `input_file`, `run_label`, `partitions`만 다르게 사용합니다.

## 6. 실행 환경과 방법

과제용 이미지는 Airflow 3.3.1에 Java 17과 PySpark 3.5.7을 추가합니다. `airflow standalone` 단일 컨테이너 구성은 로컬 시연용이며 운영 배포 구조는 아닙니다.

```bash
docker compose \
  -f infra/airflow/docker-compose.airflow.yml \
  up --build -d

docker compose \
  -f infra/airflow/docker-compose.airflow.yml \
  logs airflow
```

로그에 표시된 관리자 계정으로 `http://localhost:8082`에 접속한 뒤 `spark_parameterized_text_batch`에서 **Trigger DAG w/ config**를 선택합니다.

## 7. 결과와 중복 방지

결과는 `run_label`과 Airflow run ID별 디렉터리로 나누므로 두 실행이 서로 덮어쓰지 않습니다.

```text
data/airflow-output/<run_label>/<airflow_run_id>/
├── output/
│   ├── events/
│   └── rejected/
├── report.json
└── run.log.jsonl
```

`report.json`에는 입력, 계약 거부, 중복, 고유 출력과 전체 행 회계가 기록됩니다. 마지막 task는 다음 조건을 검사합니다.

```text
input_rows
= unique_valid_rows
+ duplicate_event_id_rows
+ contract_rejected_rows
= accounted_rows
```

## 8. 검증 상태

| 검증 | 결과 |
|---|---|
| DAG·helper Python 문법 검사 | 통과 |
| Param·경로·CLI·report 단위 테스트 | 4개 통과 |
| 비-Spark 전체 테스트 | 47개 통과 |
| Docker Compose 구성 검사 | 통과 |
| 합성 입력 행 수 | 100건·1,000건 확인 |
| Airflow 실제 두 번 실행 | Docker socket 권한으로 대기 |

실제 두 실행이 끝나면 [Airflow 과제 실행 검증](../../analysis/reports/airflow-assignment-validation.md)에 처리 건수와 시간을 기록합니다.

## 9. 발표 순서 제안

1. Week 4에서 만든 Spark batch를 이번 자동화 대상으로 선택한 이유
2. Airflow와 Spark 코드의 책임을 분리한 구조
3. 세 task의 역할과 실패 조건
4. UI에서 바꿀 수 있는 Param 설명
5. 100건과 1,000건 실행 화면 비교
6. `verify_row_accounting` 로그와 두 결과 디렉터리 확인
7. 향후 schedule, backfill, 알림과 외부 Spark cluster 확장 계획

## 10. 제출할 자료

- GitHub의 [`dags/spark_parameterized_batch.py`](../../dags/spark_parameterized_batch.py)
- Airflow Graph 또는 Grid 화면에서 두 run이 성공한 캡처
- 각 run의 `verify_row_accounting` task 로그
- 두 `report.json`의 주요 집계가 기록된 검증 문서
- GitHub에 비밀키·개인정보·`data/` 실행 산출물이 포함되지 않았다는 확인

## 11. 관련 문서

- [Airflow 실행 가이드](../guides/airflow-automation.md)
- [Week 4 Kafka·Spark 정리](week4.md)
- [Spark Standalone 실행 구조](../guides/spark-standalone.md)
- [TextEvent v1 데이터 계약](../architecture/data-contract.md)
- [Airflow 과제 실행 검증](../../analysis/reports/airflow-assignment-validation.md)
