# Airflow 과제 실행 검증

## 검증 대상

- DAG: `spark_parameterized_text_batch`
- 자동화 대상: 기존 `spark_jobs.process_sample`
- 검증일: 2026-08-27
- 실행 환경: `apache/airflow:3.3.1-python3.11`, Java 17, PySpark 3.5.7

## 사전 검증

| 항목 | 결과 |
|---|---|
| DAG·보조 모듈 Python 문법 검사 | 통과 |
| 파라미터·경로·CLI·결과 검사 단위 테스트 | 4개 통과 |
| Docker Compose 구성 검사 | 통과 |
| 합성 입력 생성 | 100건·1,000건 생성 및 행 수 확인 |

## 실제 DAG 실행 결과

실제 Airflow 실행 후 아래 표를 `report.json`과 task log를 근거로 갱신합니다. 실행 전 값을 성공으로 기록하지 않습니다.

| run label | 입력 | partitions | DAG 상태 | input/accounted | unique/rejected/duplicate | 실행 시간 |
|---|---:|---:|---|---|---|---|
| `assignment-100` | 100 | 2 | 실행 대기 | - | - | - |
| `assignment-1000` | 1,000 | 4 | 실행 대기 | - | - | - |

## 제출 체크리스트

- [x] 입력값을 받는 DAG 코드 작성
- [x] 기존 Spark 처리 코드와 연결
- [x] 100건·1,000건 입력 준비
- [ ] 서로 다른 입력값으로 DAG 두 번 성공
- [ ] 두 실행의 Airflow 화면 캡처
- [ ] task log와 집계 결과를 이 문서에 기록
- [ ] GitHub 업로드 후 4차시 채널에 링크 공유

실행 방법과 파라미터 예시는 [`docs/guides/airflow-automation.md`](../../docs/guides/airflow-automation.md)를 참고합니다.
