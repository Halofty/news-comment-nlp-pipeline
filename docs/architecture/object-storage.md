# MinIO 로컬 Object Storage 설계

## 도입 목적

대용량 Reddit 원본과 일별 Parquet를 개발자 로컬 경로에만 묶어 두지 않고 S3
호환 경계로 관리하기 위해 MinIO를 도입합니다. AWS S3 비용과 계정 설정 없이
object key, bucket, endpoint와 접근 자격 증명 구조를 먼저 검증하는 목적입니다.

MinIO는 로컬 개발·교육 환경용이며 AWS S3의 가용성, IAM, 암호화와 운영 책임을
대체하지 않습니다.

## Bucket 책임

| Bucket | 저장 대상 | 예시 key |
|---|---|---|
| `news-raw` | 변경하지 않는 수집 원본 | `reddit/year=2012/month=01/RC_2012-01.parquet` |
| `news-processed` | 정제·통합된 일별 Parquet | `text-events/year=2012/month=01/day=01/part-*.parquet` |
| `news-llm` | Batch 요청·응답·manifest | `requests/economy-social/...`, `responses/...` |
| `news-reports` | 실행 로그·검증 보고서 | `logs/...`, `reports/...` |
| `news-checkpoints` | Spark·Airflow 재시작 상태 | `spark/reddit-google-v1/...` |

원본, 처리 결과와 checkpoint를 같은 prefix에 섞지 않습니다. PostgreSQL은 검색과
상태·멱등성 관리에 사용하고, 대용량 파일 본문은 object storage에 둡니다.

## 현재 구현 범위

- Compose의 단일 노드·단일 드라이브 MinIO
- S3 API `localhost:9000`, Console `localhost:9101`
- `minio-init`을 통한 bucket 5개 멱등 생성
- Docker volume을 통한 컨테이너 재시작 후 데이터 유지
- `.env` 기반 로컬 자격 증명
- `boto3` 기반 파일·디렉터리 업로드와 다운로드 adapter
- 크기·SHA-256 metadata 검증과 동일 객체 재업로드 생략
- Hadoop 3.3.4용 S3A connector를 포함한 Spark 이미지
- Spark `s3a://` 읽기·Parquet 쓰기 검증
- 기존 raw·processed·LLM·report 정식 파일의 bucket별 전체 복사
- 새 JSONL·수집 report·LLM 요청·응답의 실행 중 자동 게시
- Airflow Spark 출력의 run별 `news-processed` 동기화
- Structured Streaming의 `s3a://` 출력·checkpoint와 프로세스 재시작 복구

MinIO를 영속 저장소로 사용하고 로컬 파일시스템은 atomic write와 Spark 실행을 위한
staging/cache로 사용한다. 완성 파일만 checksum 검증 후 복사하며 로컬에 입력이 없으면
일반 파일은 MinIO에서 복구한다. 별도의 S3A round-trip Job으로 Spark의 직접
읽기·쓰기도 검증했다.

## 실행과 검증

```bash
docker compose up -d --wait minio minio-init
docker compose ps minio minio-init
docker compose logs minio-init
```

Console은 `http://localhost:9101`에서 확인합니다. 컨테이너 내부 Console 포트는
`9001`이지만 호스트에서 이미 사용 중인 포트와 충돌하지 않도록 `9101`로 연결했습니다.
실제 `.env` 비밀번호를 Git에
올리지 않습니다. `.env.example` 값은 로컬 개발용 예시일 뿐 운영 자격 증명으로
사용하지 않습니다.

## 검증 결과

- 공개 fixture 994 bytes 업로드·다운로드 SHA-256 일치
- 같은 bucket·key 재업로드 결과 `unchanged`
- Spark S3A 입력 2행·Parquet 출력 2행
- Airflow Spark 결과 100행, MinIO 2개 객체·128,906 bytes 동기화
- 기존 정식 파일 최초 862개·40,617,977,648 bytes 복사, 실패 0건
- 현재 정식 파일 재실행 869개 `unchanged`, 전송 0 bytes
- 현재 fixture·legacy·checkpoint 실험 포함 1,045개 객체·40,621,229,862 bytes
  (약 37.83 GiB)
- 새 DAG에서 raw 100건·processed 100건·LLM 요청 10건 자동 게시
- 같은 MinIO checkpoint로 실행별 99·0·50건 처리, 최종 149건·고유 ID 149건
- MinIO 컨테이너 재시작 전후 checkpoint 43개와 출력 44개 객체 보존

상세 수치와 명령은 [MinIO 통합 검증](../../analysis/reports/minio-integration-validation.md)에
정리했다. 전체 복사와 자동 게시 결과는
[MinIO 전체 데이터 이전 검증](../../analysis/reports/minio-data-migration-validation.md)에
별도로 정리했다. Streaming 재시작 결과는
[MinIO checkpoint 복구 검증](../../analysis/reports/minio-checkpoint-recovery-validation.md)에
있다.

## 다음 확장 단계

1. 대용량 Parquet dataset 복구는 Spark S3A를 기본 입력으로 연결합니다.
2. Kafka broker·Spark worker·MinIO 네트워크 장애 중단 복구를 추가 검증합니다.
3. 로컬 파일 삭제가 필요하면 object 크기·checksum·행 수 검증과 별도 승인을 먼저
   수행합니다.
4. 외부 운영이 필요해지면 동일한 bucket/key 경계를 AWS S3로 이전합니다.

## AWS S3 전환 가능성

MinIO는 S3 API와 Spark S3A를 사용하므로 bucket, object key, Parquet 구조와 대부분의
읽기·쓰기 코드는 AWS S3에서도 재사용할 수 있다. 전환은 단순한 이름 변경이 아니라
다음 운영 설정을 교체하는 작업이다.

- 로컬 endpoint와 root key를 제거하고 AWS region·IAM role 또는 기본 credential
  provider chain을 사용한다.
- S3A의 path-style access를 끄고 TLS endpoint, bucket policy와 최소 권한 IAM을
  설정한다.
- `news-raw`, `news-processed`, `news-llm`, `news-reports`, `news-checkpoints` 책임을
  S3 bucket 또는 prefix로 동일하게 유지한다.
- `mc mirror`, `aws s3 sync` 또는 별도 migration Job으로 복사한 뒤 객체 수·byte·
  checksum·Parquet 행 수를 대조한다.
- Streaming은 기존 checkpoint를 임의 변환하지 않고 새 S3 경로에서 제한된 topic으로
  재시작 검증 후 전환한다.

현재 코드는 MinIO 전용 환경 변수와 static key를 사용하므로 AWS 전환 시 storage
설정을 provider 중립 형태로 일반화해야 한다. 따라서 S3 전환은 가능한 후속 확장이며,
현재 AWS 운영 배포가 완료됐다는 의미는 아니다.

## 운영 시 주의사항

- 기본 개발 자격 증명을 외부 네트워크에 노출하지 않습니다.
- TLS와 별도 사용자를 구성하기 전에는 로컬 호스트 밖에서 접근하지 않습니다.
- MinIO volume은 백업이 아니므로 필요한 원본의 유일한 사본으로 사용하지 않습니다.
- 개발 중 `latest` image를 사용하되 재현 가능한 제출·배포 시 검증된 release tag나
  digest로 고정합니다.
- MinIO 장애가 발생해도 PostgreSQL 상태와 원본 다운로드 manifest로 재처리할 수
  있어야 합니다.

## 참고

- [MinIO single-node container 배포](https://min.io/docs/minio/container/operations/install-deploy-manage/deploy-minio-single-node-single-drive.html)
- [MinIO Console 고정 포트](https://min.io/docs/minio/container/administration/minio-console.html)
- [`mc mb --ignore-existing`](https://min.io/docs/minio/linux/reference/minio-mc/mc-mb.html)
