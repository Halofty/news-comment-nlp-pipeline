# MinIO 확장 구현 계획

## 1. 목표

로컬 파일에만 저장하던 수집 원본, Spark 처리 결과와 LLM 요청·응답을 S3 호환 object
storage 경계로 옮긴다. 이번 확장의 완료 기준은 서비스 기동이 아니라 기존 데이터 전체
복사, 새 파이프라인 산출물 자동 게시, Spark `s3a://` 읽기·쓰기와 Airflow 동기화다.

## 2. 구현 범위와 완료 조건

| 단계 | 구현 내용 | 완료 조건 |
|---:|---|---|
| 1 | Python S3 호환 adapter | endpoint·자격 증명·bucket을 환경 변수로 받고 파일을 업로드·다운로드 |
| 2 | 무결성·멱등성 | 객체 크기와 SHA-256 metadata를 검증하고 같은 파일 재업로드를 생략 |
| 3 | 공개 fixture 실제 검증 | `sample/synthetic-events.jsonl`을 `news-raw`에 저장하고 round-trip checksum 일치 |
| 4 | Spark S3A 연결 | `news-raw` fixture를 읽어 `news-processed`에 Parquet를 쓰고 행 수 대조 |
| 5 | Airflow 연결 | Spark 출력 디렉터리를 run별 prefix로 동기화하고 object 수·bytes를 결과에 포함 |
| 6 | 전체 데이터 이전 | 정식 `data/` 산출물을 bucket별로 복사하고 실패 0건·재실행 전송 0 확인 |
| 7 | 자동 게시 | 새 raw·processed·LLM 산출물이 실행 중 해당 bucket으로 게시됨을 DAG로 검증 |
| 8 | 문서 동기화 | README·구성 문서·로드맵에 실제 실행 결과와 확인 명령 반영 |
| 9 | Streaming checkpoint | S3A checkpoint로 3회 실행해 처리량 99·0·50과 최종 고유 149건 대조 |
| 10 | 저장소 재시작 | MinIO 컨테이너 재시작 전후 report·checkpoint·output 객체 보존 확인 |

## 3. Object key 규칙

```text
news-raw/
└── fixtures/text-events/synthetic-events.jsonl

news-processed/
├── fixtures/text-events/synthetic-events-parquet/
└── airflow/<run-label>/<airflow-run-id>/output/...

news-llm/
├── requests/...
├── responses/...
└── airflow/...

news-reports/
├── logs/...
└── reports/...

news-checkpoints/
└── spark/<consumer-or-job>/<checkpoint-version>/...
```

원본과 처리 결과를 다른 bucket에 저장하고, Airflow 출력에는 사람이 지정한 경로 대신
정규화된 run label과 run ID를 사용한다. 객체 key는 절대 경로와 `..` 경로 이동을
허용하지 않는다.

## 4. 무결성·재실행 정책

- 업로드 전에 로컬 SHA-256과 byte 크기를 계산한다.
- SHA-256은 객체 metadata의 `sha256`에 저장한다.
- 같은 bucket·key에 동일 크기와 SHA-256 객체가 있으면 업로드하지 않고
  `unchanged`로 처리한다.
- 값이 다르면 같은 key를 새 객체로 덮어쓰고 다시 `head_object`로 검증한다.
- 다운로드 후에도 로컬 SHA-256을 다시 계산한다.
- 디렉터리 동기화 결과는 `uploaded`, `unchanged`, `object_count`, `total_bytes`로 남긴다.

## 5. 안전 경계

- `.env`의 실제 MinIO 비밀번호는 출력하거나 Git에 추가하지 않는다.
- 로컬 HTTP endpoint는 개발 네트워크에서만 사용한다.
- MinIO 적재가 끝나도 기존 로컬 원본은 자동 삭제하지 않는다.
- 로컬 삭제는 object checksum·행 수·복구 시험을 마친 뒤 별도 승인으로 수행한다.
- checkpoint 경로는 topic과 stateful 처리 버전마다 분리하고 임의로 재사용하지 않는다.

## 6. 진행 현황

| 단계 | 상태 | 검증 자료 |
|---:|:---:|---|
| 1 | 완료 | `storage/object_store.py`, adapter 단위 테스트 3건 |
| 2 | 완료 | 첫 실행 `uploaded`, 같은 fixture 재실행 `unchanged` |
| 3 | 완료 | 994 bytes 업로드·다운로드 SHA-256 일치 |
| 4 | 완료 | Spark S3A 2행 읽기·Parquet 2행 쓰기, 누락 0 |
| 5 | 완료 | Airflow에서 100건 결과 2개 객체·128,906 bytes 동기화 |
| 6 | 완료 | 최초 862개·40,617,977,648 bytes 복사, 현재 정식 파일 869개 동기화 |
| 7 | 완료 | Reddit 100건→Spark 100건→LLM 요청 10건 DAG에서 bucket별 자동 저장 |
| 8 | 완료 | README·Date 7·로드맵과 전체 이전 검증 보고서 갱신 |
| 9 | 완료 | 같은 query ID로 99·0·50건 처리, 최종 149건·고유 ID 149건 |
| 10 | 완료 | MinIO 재시작 전후 checkpoint 43개·output 44개 객체 보존 |

마지막 멱등 재실행에서는 정식 파일 869개가 모두 `unchanged`, 실제 전송량 0 bytes로
확인됐다. checkpoint 실험 후 5개 bucket에는 fixture와 legacy key를 포함해 1,045개
객체, 40,621,229,862 bytes(약 37.83 GiB)가 저장되어 있다. 로컬 파일은 작업
staging/cache로 유지하며 자동 삭제하지 않는다.

## 7. AWS S3 후속 전환

MinIO에서 검증한 S3 API, bucket/key 구조와 Spark S3A 경계는 AWS S3 전환 기반으로
재사용한다. 후속 전환에서는 endpoint·path-style·static credential을 AWS region,
TLS, IAM role/default credential provider와 bucket policy로 바꾼다. 객체 복사 후에는
bucket별 object 수·byte·checksum, Parquet 행 수와 제한된 Kafka topic의 새 checkpoint
재시작 결과를 대조한다. 현재 단계는 전환 가능성을 설계한 것이며 AWS 자원 생성이나
데이터 업로드는 수행하지 않았다.
