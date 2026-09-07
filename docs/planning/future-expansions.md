# 후속 확장 계획

이 문서는 아직 구현하지 않은 기능을 현재 시스템 구성과 분리해 관리한다. 아래 항목은
가능성과 방향을 정리한 것이며 README와 시스템 구성도에서는 현재 기능으로 표시하지
않는다.

## 1. Kafka 실시간 경로의 운영 통합

현재 Kafka Producer, DLQ와 Spark Structured Streaming은 독립적으로 구현·검증됐지만
최종 Airflow 배치 DAG가 반드시 통과하는 단계는 아니다.

운영 통합이 필요하면 다음을 추가 검증한다.

- Collector가 로컬 staging과 Kafka 중 어떤 경로에 기록할지 실행 모드로 선택
- Kafka consumer lag, replay 시작 offset과 처리 완료 offset 기록
- broker·worker 중단 후 checkpoint와 PostgreSQL commit의 일치 검증
- batch와 streaming이 같은 `event_id`를 처리할 때 중복 저장 방지

## 2. MinIO에서 AWS S3로 전환

현재 MinIO의 bucket/key와 S3-compatible adapter를 유지한 채 endpoint와 인증 방식을
AWS S3로 교체할 수 있다. 실제 전환 전에는 다음이 필요하다.

- IAM role과 최소 권한 bucket policy
- server-side encryption, versioning, lifecycle, 비용 예산 설정
- Spark S3A credential provider와 multipart upload 검증
- MinIO→S3 checksum 비교 및 실패 시 rollback 절차
- local endpoint에 의존한 설정을 provider 중립 설정으로 변경

현재 AWS 자원을 생성하거나 데이터를 업로드한 상태는 아니다.

## 3. 기사 전문 수집

현재 뉴스 분석 텍스트는 제목이다. 전문을 추가하려면 URL 접근 가능성만으로 결정하지
않고 다음을 먼저 검증한다.

- robots.txt와 언론사 이용약관
- 저작권과 원문 보존 기간
- 본문 추출 성공률, 중복 기사와 syndicated article 처리
- 과대 문서·깨진 Unicode·광고 문구에 대한 품질 기준
- LLM token 예산과 제목/본문 가중치

## 4. PostgreSQL 대규모 적재

현재는 Driver chunk upsert를 사용한다. 데이터가 커지면 다음 순서로 비교한다.

1. JDBC staging table 적재
2. `COPY` 기반 bulk load
3. staging에서 최종 테이블로 `event_id` 기반 merge
4. 실패 batch rollback과 재실행 행 회계 검증

## 5. 수집과 장애 복구 확대

- Google News 100건 상한 도달 검색을 날짜·검색 조건으로 세분화
- Reddit 나머지 기간을 선정 subreddit 기준 UTC 일별 Parquet로 변환
- Kafka broker, Spark worker, PostgreSQL, MinIO의 장시간·복합 장애 실험
- 분산 MinIO 또는 운영 object storage의 backup·restore 검증

## 적용 원칙

각 항목은 코드, 자동 테스트, 실제 실행 기록이 모두 준비된 뒤에만 README의 현재 구현과
시스템 구성도에 편입한다.
