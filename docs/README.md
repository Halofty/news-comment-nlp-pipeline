# 프로젝트 문서 안내

| 분류 | 내용 |
|---|---|
| [`architecture/`](architecture/) | 전체 시스템, 기술 스택, 데이터 계약과 PostgreSQL·MinIO·LLM 설계 |
| [`guides/`](guides/) | 로컬 실행, ingestion, Spark와 Airflow 사용 방법 |
| [`briefings/`](briefings/) | 회차별 과제 설명과 발표 자료 |
| [`planning/`](planning/) | 로드맵, 피드백 반영, 장애·부하 및 Langfuse 계획 |
| [`security/`](security/) | 데이터·개인정보·비밀정보 처리 원칙 |
| [`reports/`](reports/) | 수집 데이터 검증 문서 |
| [`adr/`](adr/) | 주요 기술 결정과 근거 |

실행 결과와 공개 가능한 수치 검증은 저장소 최상위의 [`analysis/reports/`](../analysis/reports/)에서 관리합니다.

웹 뉴스 수집은 [`guides/web-news-collection.md`](guides/web-news-collection.md), 현재
2012년 수집·부하·복구 결과는 [`briefings/date6/date6.md`](briefings/date6/date6.md)에서
확인할 수 있습니다.

6차시 전체 흐름 점검과 GPT-5.6 Luna·Langfuse 보완 결과는
[`briefings/date7/date7.md`](briefings/date7/date7.md)에서 확인할 수 있습니다.
경제·사회 1월 일별 31건, quality gate와 월간 통합 분석의 실제 결과는
[`briefings/date7/economy-social-results-01-31.md`](briefings/date7/economy-social-results-01-31.md)에
정리했습니다.

OpenAI API 프로젝트·환경변수와 Langfuse Cloud Japan의 실제 구성·검증 기록은
[`briefings/date7/openai-langfuse-setup.md`](briefings/date7/openai-langfuse-setup.md)에
있습니다.

MinIO 도입 범위와 bucket 구조는 [`architecture/object-storage.md`](architecture/object-storage.md)에 정리했습니다.
실제 전체 데이터 복사와 자동 게시 수치는
[`../analysis/reports/minio-data-migration-validation.md`](../analysis/reports/minio-data-migration-validation.md)에서 확인할 수 있습니다.
실제 checksum·멱등 업로드, Spark S3A와 Airflow 동기화 결과는
[`analysis/reports/minio-integration-validation.md`](../analysis/reports/minio-integration-validation.md)에서 확인할 수 있습니다.
Spark와 MinIO 컨테이너 재시작의 checkpoint·출력 무결성 결과는
[`analysis/reports/minio-checkpoint-recovery-validation.md`](../analysis/reports/minio-checkpoint-recovery-validation.md)에 정리했습니다.
