# MinIO 전체 데이터 이전 및 자동 저장 검증

- 실행일: 2026-09-03 KST
- 대상: `data/` 아래의 정식 원본·처리·LLM·실행 보고서
- 저장 방식: 로컬 작업 파일을 checksum 검증 후 MinIO에 복사

## 1. 저장 경계

| Bucket | 저장 대상 | 대표 prefix |
|---|---|---|
| `news-raw` | 수집 원본과 Airflow 입력 | `raw/`, `airflow-input/` |
| `news-processed` | Spark 결과·검증·실험 산출물 | `airflow/<run>/<id>/output/`, `selected/`, `validation/` |
| `news-llm` | Batch 요청·응답·manifest | `requests/`, `responses/`, `airflow/` |
| `news-reports` | 실행 로그·보고서·검증 자료 | `airflow/<run>/<id>/`, `logs/`, `reports/` |
| `news-checkpoints` | 향후 Spark 재시작 상태 | `spark/` |

`.part`, `.tmp`, `.prefix`, `.footer`, `.sparse.parquet`, Spark `.crc`와 이전 작업
자체의 manifest·report는 대량 이전 대상에서 제외했다. 전자는 미완성 다운로드 또는
임시 파일이고, manifest·report는 실행이 끝난 뒤 별도로 업로드한다.

## 2. 전체 데이터 이전 결과

첫 실행에서 862개 파일, 40,617,977,648 bytes를 MinIO에 복사했다. 실패는 없었고
94.536초가 걸렸다. 이후 실행 자체가 만든 파일을 순회 대상에서 제외한 상태로 다시
검증한 결과는 다음과 같다.

| 항목 | 최초 전체 복사 | Airflow key 정리 | 최종 멱등 재실행 |
|---|---:|---:|---:|
| 대상 객체 | 862 | 869 | 869 |
| 업로드 | 862 | 54 | 0 |
| 변경 없음 | 0 | 815 | 869 |
| 실패 | 0 | 0 | 0 |
| 실제 전송량 | 40,617,977,648 B | 1,543,851 B | 0 B |
| 실행 시간 | 94.536초 | 31.404초 | 31.024초 |

최종 실행의 전송량이 0인 것은 객체 크기와 SHA-256 metadata가 일치하는 파일을
`unchanged`로 처리했기 때문이다. 중간에는 Airflow LLM 산출물을 `news-llm`으로 옮기고
Spark output과 report의 key·bucket을 통일했다. 안전상 기존 잘못된 prefix 객체는 자동
삭제하지 않았으므로 현재 용량 집계에는 작은 legacy 사본도 포함된다.

Airflow Spark 동기화와 전체 이전은 모두 `airflow/<run>/<id>/...` key를 사용한다.
`output/` 아래 실제 처리 데이터는 `news-processed`, 같은 run의 report·log는
`news-reports`로 분리해 이후 재실행에서 중복 key가 생성되지 않게 했다.

현재 bucket 전체 현황에는 위 이전 데이터 외에 사전 fixture와 Spark S3A 검증 결과가
함께 포함된다.

| Bucket | 객체 수 | 저장 크기 |
|---|---:|---:|
| `news-raw` | 394 | 39,905,778,505 B |
| `news-processed` | 199 | 636,244,330 B |
| `news-llm` | 315 | 74,358,209 B |
| `news-reports` | 44 | 4,059,713 B |
| `news-checkpoints` | 0 | 0 B |
| 합계 | 952 | 40,620,440,757 B (약 37.83 GiB) |

## 3. 새 파이프라인 자동 저장 검증

`PIPELINE_STORAGE_BACKEND=minio`일 때 공통 JSONL writer와 각 Job이 로컬 임시
파일을 완성한 후 해당 객체를 즉시 MinIO에 게시한다. 읽을 파일이 로컬에 없으면
일반 파일은 같은 bucket/key에서 검증 다운로드해 작업 캐시를 복구한다.

실제 Airflow DAG를 Reddit `2016-01-02`, 수집 100건, LLM 요청 준비 10건으로 실행했다.

```text
Reddit 수집 100건
→ news-raw/airflow-input/reddit-2016-01-02.jsonl
→ Spark 입력·고유 출력 100건, 오류 0건
→ news-processed에 Spark 결과 2개 객체
→ LLM 요청·manifest·preflight 3개 파일
→ news-llm/requests/airflow-minio-auto/
```

| 지표 | 결과 |
|---|---:|
| Reddit 수집 | 100건 |
| Spark 입력 / 고유 저장 | 100 / 100건 |
| Spark 오류 | 0건 |
| Spark 실행 시간 | 6.192초 |
| MinIO 처리 객체 / 크기 | 2개 / 140,396 bytes |
| LLM 요청 준비 | 10건 |
| 예상 입력 token | 3,491 |
| 최종 DAG 상태 | `success` |

외부 LLM API에는 제출하지 않고 `submit=false`로 요청 생성과 저장 경로만 확인했다.
원본 객체에는 `pipeline-path`와 SHA-256 metadata가 기록되었다.

## 4. 실행과 확인

```bash
python -m jobs.migrate_data_to_minio --dry-run
python -m jobs.migrate_data_to_minio

docker compose run --rm --entrypoint /bin/sh minio-init -c \
  'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc du --json local/news-raw'
```

대량 이전 상세 manifest는 `data/logs/minio-data-migration.jsonl`, 마지막 실행 요약은
`data/reports/minio-data-migration.json`에 남는다. 두 파일도 MinIO의 `news-reports`에
게시된다.

## 5. 운영 해석과 남은 범위

현재 구조에서 MinIO는 영속 객체 저장소이고 로컬 파일은 수집·Spark 실행을 위한
staging/cache다. 업로드는 파일 이동이 아니라 복사이므로 기존 로컬 데이터는 삭제하지
않았다. MinIO 컨테이너를 내려도 named volume `minio-data`가 남아 데이터는 유지되지만,
volume까지 삭제하면 복구할 별도 백업은 없다.

대용량 Parquet directory 전체를 로컬 없이 바로 읽는 경로는 Spark S3A로 검증했지만,
모든 Collector가 byte stream을 곧바로 MinIO에 쓰도록 바뀐 것은 아니다. 미완성 파일을
노출하지 않기 위해 로컬에서 atomic write를 완료한 다음 검증 업로드한다. 남은 독립
확장 항목은 `news-checkpoints`의 실제 Structured Streaming 재시작 검증이다.
