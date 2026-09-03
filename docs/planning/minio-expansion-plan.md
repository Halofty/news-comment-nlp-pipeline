# MinIO 확장 구현 계획

## 1. 목표

로컬 파일에만 저장하던 수집 원본과 Spark 처리 결과를 S3 호환 object storage 경계로
옮길 수 있게 한다. 이번 확장의 완료 기준은 서비스 기동이 아니라 실제 업로드·검증,
Spark `s3a://` 읽기·쓰기와 Airflow 동기화까지다.

## 2. 구현 범위와 완료 조건

| 단계 | 구현 내용 | 완료 조건 |
|---:|---|---|
| 1 | Python S3 호환 adapter | endpoint·자격 증명·bucket을 환경 변수로 받고 파일을 업로드·다운로드 |
| 2 | 무결성·멱등성 | 객체 크기와 SHA-256 metadata를 검증하고 같은 파일 재업로드를 생략 |
| 3 | 공개 fixture 실제 검증 | `sample/synthetic-events.jsonl`을 `news-raw`에 저장하고 round-trip checksum 일치 |
| 4 | Spark S3A 연결 | `news-raw` fixture를 읽어 `news-processed`에 Parquet를 쓰고 행 수 대조 |
| 5 | Airflow 연결 | Spark 출력 디렉터리를 run별 prefix로 동기화하고 object 수·bytes를 결과에 포함 |
| 6 | 문서 동기화 | README·구성 문서·로드맵에 실제 실행 결과와 확인 명령 반영 |

## 3. Object key 규칙

```text
news-raw/
└── fixtures/text-events/synthetic-events.jsonl

news-processed/
├── fixtures/text-events/synthetic-events-parquet/
└── airflow/<run-label>/<airflow-run-id>/output/...

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
- `news-checkpoints` 전환은 원본·처리 데이터 경로가 안정된 뒤 별도로 검증한다.

## 6. 진행 현황

| 단계 | 상태 | 검증 자료 |
|---:|:---:|---|
| 1 | 진행 중 | Python adapter와 단위 테스트 |
| 2 | 대기 | 같은 fixture 2회 업로드 결과 비교 |
| 3 | 대기 | MinIO round-trip 보고서 |
| 4 | 대기 | Spark S3A 실행 보고서 |
| 5 | 대기 | Airflow DAG import 및 동기화 실행 결과 |
| 6 | 대기 | README·Date 7·로드맵 링크 검사 |

