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
| `news-checkpoints` | Spark·Airflow 재시작 상태 | `spark/reddit-google-v1/...` |

원본, 처리 결과와 checkpoint를 같은 prefix에 섞지 않습니다. PostgreSQL은 검색과
상태·멱등성 관리에 사용하고, 대용량 파일 본문은 object storage에 둡니다.

## 현재 구현 범위

- Compose의 단일 노드·단일 드라이브 MinIO
- S3 API `localhost:9000`, Console `localhost:9001`
- `minio-init`을 통한 bucket 3개 멱등 생성
- Docker volume을 통한 컨테이너 재시작 후 데이터 유지
- `.env` 기반 로컬 자격 증명

아직 Spark와 애플리케이션의 실제 읽기·쓰기 경로는 로컬 파일시스템입니다.
MinIO 서비스가 추가됐다는 사실과 `s3a://` 연동 완료를 구분해야 합니다.

## 실행과 검증

```bash
docker compose up -d --wait minio minio-init
docker compose ps minio minio-init
docker compose logs minio-init
```

Console은 `http://localhost:9001`에서 확인합니다. 실제 `.env` 비밀번호를 Git에
올리지 않습니다. `.env.example` 값은 로컬 개발용 예시일 뿐 운영 자격 증명으로
사용하지 않습니다.

## 다음 연동 단계

1. 작은 fixture를 `news-raw`에 업로드하고 size·ETag를 검증합니다.
2. Python S3 adapter를 추가해 endpoint와 bucket을 환경 변수로 받습니다.
3. Spark에 호환되는 `hadoop-aws`와 AWS SDK 버전을 고정합니다.
4. path-style access와 로컬 HTTP endpoint로 `s3a://news-raw/...`를 읽습니다.
5. 2012년 1월 Parquet를 복사해 로컬 파일과 object의 행 수를 비교합니다.
6. 성공 후 2012년 전체 원본·처리 데이터를 단계적으로 이동합니다.
7. 로컬 파일 삭제는 object 크기·checksum·행 수 검증과 별도 승인 후에만 합니다.

Spark checkpoint는 일반 출력보다 일관성과 재시작 검증이 중요합니다. 원본과
처리 데이터 연동이 안정된 뒤 마지막으로 `news-checkpoints`를 적용합니다.

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
