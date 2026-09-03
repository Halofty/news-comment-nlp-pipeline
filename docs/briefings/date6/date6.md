# Date 6 — 2012년 데이터 수집과 파이프라인 부하 실험

## 1. 결론

`2012-01`~`2016-02`의 Reddit 월별 Parquet 50개는 공식 Hugging Face 파일 크기 기준 총 **291,704,154,448 bytes, 271.67GiB**입니다. 현재 계획한 Reddit 원본 예산 약 300GB 안에 들어가므로 원본 단계에서 subreddit을 버릴 필요는 없습니다.

다만 원본을 `TextEvent v1` JSONL로 모두 풀면 두 날짜 표본 기준 약 **1.46TiB**로 추정됩니다. 따라서 다음 구조를 사용합니다.

```text
월별 원본 Parquet 50개 전체 보관                 약 271.67GiB
→ 월 단위 Spark 읽기
→ 날짜·subreddit·품질 필터
→ 선택 데이터만 UTC 연/월/일 partitioned Parquet로 저장
→ 임시 JSONL은 월 처리 완료 후 보관하지 않음
```

핵심 원칙은 **원본은 전체 보존하고 분석·가공 계층만 subreddit으로 제한**하는 것입니다.

저장 경로는 다음과 같이 Hive 스타일의 일 단위 파티션으로 고정합니다.

```text
data/selected/reddit/
└── year=2016/
    └── month=01/
        ├── day=01/
        │   └── part-*.parquet
        ├── day=02/
        │   └── part-*.parquet
        └── ...
```

`created_utc`를 UTC 날짜로 변환해 파티션을 결정합니다. 월별 원본 파일은
다운로드·재개 단위이며, 실제 분석 데이터의 조회·재처리 단위는 일(day)입니다.

## 2. 원본 월 파일 용량 검증

아래 `월 파일` 크기는 일별 표본을 월 단위로 확대한 추정값이 아니다. Hugging Face에
등록된 `RC_YYYY-MM.parquet` 파일 자체의 byte 크기이며, 내려받은 2012년 파일 12개도
카탈로그와 byte 단위로 일치했다. `월평균`과 `일평균`만 이 실측 합계에서 계산한
파생값이다.

| 지표 | 값 |
|---|---:|
| 기간 | 2012-01-01~2016-02-29 |
| 기간 일수 | 1,521일 |
| 월 파일 | 50개 |
| 총 압축 Parquet 실측 합계 | 291,704,154,448 bytes (291.70GB, 271.67GiB) |
| 월평균 파생값 | 5.83GB (5.43GiB) |
| 일평균 파생값 | 약 191.78MB (182.9MiB) |
| 최소 월 | 2012-02, 2.13GiB |
| 최대 월 | 2016-01, 9.12GiB |

계산식은 `전체 bytes ÷ 50개월`과 `전체 bytes ÷ 1,521일`이다. 즉, 하루 크기를
한 달 크기로 간주하지 않는다. 월평균은 목표한 6GB와 비슷하지만 Reddit 사용량이
증가하므로 모든 달이 6GB 이하는 아니다. 특히 2016-01과 2016-02는 각각 약
9.12GiB와 8.81GiB다.

2012년 로컬 다운로드 결과로도 다음과 같이 교차 검증했다.

| 검증 항목 | 실제 값 |
|---|---:|
| 2012-01 원본 월 파일 | 2,299,573,066 bytes, 15,060,640행 |
| 2012-02 원본 월 파일 | 2,289,042,111 bytes, 14,751,099행 |
| 2012년 12개 월 파일 합계 | 36,560,787,920 bytes (36.56GB, 34.05GiB) |

50개 파일별 정확한 크기는 [`reddit-monthly-source-files.csv`](reddit-monthly-source-files.csv)에 있습니다. 출처는 [Hugging Face의 Pushshift Reddit Comments 데이터셋](https://huggingface.co/datasets/fddemarco/pushshift-reddit-comments) 공식 파일 API입니다.

## 3. subreddit 전수 목록을 계산한 범위

현재 확보한 다음 두 날짜를 전수 조사했습니다.

| 날짜 | 이벤트 | JSONL 크기 |
|---|---:|---:|
| 2016-01-01 | 1,452,563건 | 851,436,307 bytes |
| 2016-02-01 | 1,915,934건 | 1,211,910,818 bytes |
| 합계 | 3,368,497건 | 2,063,347,125 bytes |

대소문자가 다른 동일 이름을 `casefold()`로 통합한 결과 **29,173개 subreddit**이 확인됐습니다. 모든 이름, 날짜별 행 수, 실제 JSONL bytes와 기간 추정치는 [`subreddit-inventory.csv`](subreddit-inventory.csv)에 기록했습니다.

여기서 subreddit별 용량은 **두 날짜 JSONL에서 직접 측정한 정확한 값**입니다. `projected_period_gib`는 두 날짜 평균을 1,521일로 확장한 추정치이며 2012~2016 전체를 실제로 스캔한 값은 아닙니다.

## 4. 상위 subreddit

| 순위 | subreddit | 2일 댓글 | 일평균 JSONL | 50개월 단순 추정 |
|---:|---|---:|---:|---:|
| 1 | AskReddit | 277,210 | 73.60MiB | 109.32GiB |
| 2 | CFB | 71,108 | 16.51MiB | 24.52GiB |
| 3 | leagueoflegends | 50,272 | 13.47MiB | 20.00GiB |
| 4 | politics | 40,242 | 12.73MiB | 18.91GiB |
| 5 | funny | 44,916 | 11.19MiB | 16.62GiB |
| 6 | news | 34,372 | 10.46MiB | 15.54GiB |
| 7 | videos | 37,215 | 10.32MiB | 15.32GiB |
| 8 | hockey | 42,487 | 10.06MiB | 14.94GiB |
| 9 | todayilearned | 32,103 | 9.00MiB | 13.36GiB |
| 10 | worldnews | 28,057 | 8.72MiB | 12.96GiB |

CFB·hockey처럼 날짜와 시즌에 크게 좌우되는 커뮤니티가 있으므로 단순 용량 순으로 타기팅하면 분석 주제와 관계없는 데이터가 많이 포함됩니다.

## 5. 확정 분석 subreddit 21개

21개 목록을 [`config/subreddits-analysis.txt`](../../../config/subreddits-analysis.txt)에 확정했습니다. Collector는 `--subreddit-file`로 이 파일을 받고, 로컬 Parquet에서는 날짜와 subreddit 조건을 함께 predicate pushdown합니다.

### 5.1 핵심 뉴스·정책 집합

다음 커뮤니티는 뉴스 보도와 정치·경제·기술·과학·환경 토픽을 비교하기 위한 핵심 집합입니다.

```text
politics, news, worldnews, PoliticalDiscussion, NeutralPolitics,
geopolitics, Economics, business, technology, Futurology,
science, environment, climate, energy, TrueReddit, changemyview,
explainlikeimfive, todayilearned, dataisbeautiful, InternetIsBeautiful
```

- 후보: 20개
- 두 날짜 기준 일평균 JSONL: 약 56.16MiB
- 50개월 단순 추정: 약 83.41GiB

### 5.2 일반 대중 반응 확장 확정

핵심 집합에 `AskReddit`을 추가해 뉴스 링크 반응뿐 아니라 일반 담론과 질문형 반응을 더 넓게 봅니다. 따라서 최종 allowlist는 21개입니다.

- 후보: 21개
- 두 날짜 기준 일평균 JSONL: 약 129.75MiB
- 50개월 단순 추정: 약 192.73GiB

300GB를 반드시 채우기 위해 관련성이 낮은 대형 subreddit을 추가하지 않습니다. 300GB는 목표 사용량이 아니라 상한으로 보고, 실제 토픽 분석 가치가 있는 커뮤니티만 선택합니다.

### 5.3 두 날짜 실제 필터 검증

| 날짜 | 전체 이벤트 | 21개 선택 | 선택 JSONL | byte 유지율 |
|---|---:|---:|---:|---:|
| 2016-01-01 | 1,452,563 | 195,464 | 115,439,904 bytes | 13.56% |
| 2016-02-01 | 1,915,934 | 260,883 | 156,669,663 bytes | 12.93% |
| 합계 | 3,368,497 | 456,347 | 272,109,567 bytes | 13.19% |

실제 결과는 `data/selected/`에 저장했으며 원본 전체 JSONL은 유지했습니다. 두 날짜의 선택 데이터 일평균은 약 129.75MiB로 사전 추정과 일치합니다.

```bash
python -m collectors.reddit \
  --month 2016-01 \
  --input-parquet data/raw/reddit-parquet/RC_2016-01.sparse.parquet \
  --start-date 2016-01-01 --end-date 2016-01-01 \
  --subreddit-file config/subreddits-analysis.txt \
  --limit 0 \
  --output data/selected/reddit-2016-01-01-selected.jsonl
```

## 6. 600GB 디스크 운영안

| 구역 | 계획 |
|---|---:|
| Reddit 압축 원본 | 약 271.67GiB |
| 가공 데이터·Spark 출력 | 최대 약 200GiB 권장 |
| 다운로드 임시 파일·checkpoint·여유 공간 | 최소 약 100GiB 유지 |

전체 JSONL을 한 번에 만들지 않습니다. 월별 Parquet를 직접 Spark로 읽고 선택 subreddit만 Parquet로 저장합니다. 처리에 사용한 임시 파일은 해당 월의 행 회계와 출력 검증이 끝난 뒤 제거합니다.

## 7. 당시 기술 확장 범위

1. 월별 파일 50개의 checksum·다운로드 상태 manifest를 만든다.
2. 중단 후 재개 가능한 월 단위 downloader 구현 완료
3. 다운로드 중 최소 100GiB 여유 공간을 검사한다.
4. Spark에서 `subreddit` allowlist를 파라미터로 받고 UTC 일 단위로 저장한다.
5. 한 달을 먼저 처리해 압축 Parquet 출력 비율과 처리 시간을 측정한다.
6. 실제 전체 기간 집계 후 추정치를 정확한 subreddit별 행 수·용량으로 교체한다.

### 전체 원본 다운로드 실행 상태

- 시작 시각: 2026-08-31 12:43 KST
- 범위: `2012-01`~`2016-02`, 총 50개월
- 예상 크기: 291,704,154,448 bytes (271.67GiB)
- 현재 상태: 2012년 범위 완료 후 중단
- 완료 범위: `2012-01`~`2012-12`, 12개월
- 완료 원본: 36,560,787,920 bytes (34.05GiB)
- 재개 지점: `2013-01`의 `.part` 파일 336,523,264 bytes부터
- 원본 경로: `data/raw/reddit-archive/data/RC_YYYY-MM.parquet`
- 진행 로그: `data/logs/reddit-archive-download.jsonl`
- 안전장치: 월별 정확한 byte 검증, HTTP Range 재개, 100GiB 잔여 공간 유지

실행 중인 파일은 `.parquet.part` 확장자를 사용합니다. 파일 크기가 카탈로그와
일치한 경우에만 `.parquet`으로 변경되므로 미완료 파일을 분석 입력으로 잘못
사용하지 않습니다.

## 8. 산출물

| 파일 | 내용 |
|---|---|
| [`reddit-monthly-source-files.csv`](reddit-monthly-source-files.csv) | 공식 원본 50개월의 파일별 bytes·GiB |
| [`subreddit-inventory.csv`](subreddit-inventory.csv) | 두 날짜에 등장한 29,173개 subreddit 전체 목록과 용량 |
| [`subreddit-profile-summary.json`](subreddit-profile-summary.json) | 전체·후보 집합 집계와 추정 조건 |
| [`config/subreddits-analysis.txt`](../../../config/subreddits-analysis.txt) | 확정된 21개 subreddit allowlist |
| [`jobs/profile_subreddit_storage.py`](../../../jobs/profile_subreddit_storage.py) | subreddit별 행·JSONL byte 재현 스크립트 |
| [`jobs/catalog_reddit_months.py`](../../../jobs/catalog_reddit_months.py) | Hugging Face 월 파일 카탈로그 생성 스크립트 |
| [`jobs/download_reddit_archive.py`](../../../jobs/download_reddit_archive.py) | 50개월 원본의 재개·크기 검증·디스크 보호 다운로드 |
| [`jobs/filter_reddit_events.py`](../../../jobs/filter_reddit_events.py) | 기존 TextEvent JSONL에 동일 allowlist 적용 |
| [`spark_jobs/filter_reddit_archive.py`](../../../spark_jobs/filter_reddit_archive.py) | 월별 원본을 21개 subreddit의 UTC 일 단위 Parquet로 변환 |

## 9. 2012년 데이터 수집 판단 과정과 결과

### 9.1 기간을 2012년으로 맞춘 이유

Reddit 원본은 `2012-01`부터 제공되므로 뉴스와 댓글의 시계열을 직접 비교하려면
뉴스도 같은 날짜 범위로 맞춰야 합니다. GDELT DOC API는 이 과거 기간을 현재
수집 방식으로 안정적으로 조회할 수 없어 뉴스 수집 경로에서 제외했습니다.

Google News는 2012년 검색 결과를 반환하는 것을 실제 요청으로 확인했습니다.
다만 완전한 원본 아카이브가 아니라 결과 수가 제한되는 검색 색인이므로 다음과
같이 수집 단위를 잘게 나눴습니다.

```text
하루 × 4개 주제군
→ Google News RSS 요청
→ URL 기준 중복 제거
→ 요청 기간 밖 결과 제거
→ year=YYYY/month=MM/day=DD/events.jsonl
```

주제군은 분석 결과를 미리 고정하는 라벨이 아니라 뉴스 후보를 넓게 찾기 위한
검색 조건입니다.

| 주제군 | 검색어 |
|---|---|
| 정치 | government, election, president, parliament |
| 경제 | economy, economic, market, business |
| 기술 | technology, internet, digital |
| 환경 | climate, energy, environment |

Google News 화면을 Scrapy로 직접 크롤링하면 동적 화면·리디렉션·차단과 selector
변경에 취약하면서도 검색 색인의 완전성은 개선되지 않습니다. 따라서 Google
News에서는 RSS를 후보 발견에 사용하고, 기사 본문이나 더 완전한 목록이 필요할
때 상위 언론사의 공식 아카이브를 Scrapy로 수집하는 방식으로 분리했습니다.

### 9.2 Reddit 2012년 원본 수집 결과

| 항목 | 결과 |
|---|---:|
| 기간 | 2012-01-01~2012-12-31 |
| 월별 Parquet | 12개 |
| Parquet metadata 기준 행 | 239,814,057건 |
| 압축 원본 크기 | 36,560,787,920 bytes (34.05GiB) |
| 파일 크기 검증 | 12개 모두 공식 카탈로그와 일치 |
| 다음 재개 지점 | 2013-01 `.part` 336,523,264 bytes |

원본은 월별 Parquet로 보존하고, 분석용 21개 subreddit 결과는 이후 Spark에서
UTC 일 단위 Parquet로 변환합니다. 이번 단계에서는 원본 다운로드가 목적이므로
239,814,057건을 Spark 처리·저장 건수로 표현하지 않습니다.

### 9.3 Google News 2012년 수집 결과

| 항목 | 1월 | 2~12월 | 2012년 합계 |
|---|---:|---:|---:|
| 날짜 | 31일 | 335일 | 366일 |
| RSS 요청 | 124회 | 1,340회 | 1,464회 |
| RSS 반환 건수 | 4,823건 | 51,793건 | 56,616건 |
| URL 중복 제거 후 후보 | 2,558건 | 26,639건 | 29,197건 |
| 날짜 검증 후 저장 | 2,410건 | 26,584건 | 28,994건 |
| 일별 파일 | 31개 | 335개 | 366개 |
| 저장 크기 | 2,988,801 bytes | 33,147,894 bytes | 36,136,695 bytes (34.46MiB) |
| 100건 제한 도달 요청 | 0회 | 3회 | 3회 |

`RSS 반환 건수`는 검색어가 겹쳐 같은 URL이 여러 번 포함된 입력량이고,
`날짜 검증 후 저장`은 URL 중복과 기간 밖 검색 결과를 제거한 최종 건수입니다.
100건에 도달한 3개 요청은 누락 가능성이 있으므로 완전한 뉴스 전수 자료가 아닌
검색 후보 데이터라는 한계를 유지합니다.

## 10. 데이터 수집 결과

### 10.1 현재 입력량과 정상 실행

현재 정상 실행의 기준은 **Google News 2012년 1월 전체(31일)**로 잡았습니다.

| 측정 항목 | 결과 |
|---|---:|
| 입력 날짜 | 31일 |
| 입력 요청 | 124회 |
| 입력 응답 항목 | 4,823건 |
| 중복 제거 처리 | 2,558건 |
| 최종 저장 | 2,410건 |
| 실행 시간 | 177.563초 (약 2분 58초) |
| 저장 처리량 | 약 13.57건/초 |
| 치명적 오류 | 0건 |
| 결과 제한 경고 | 0건 |

시작 시각은 모든 이벤트에 기록된 `collected_at`, 종료 시각은 최종 보고서의
파일 수정 시각으로 계산했습니다. 결과는 31개 일별 JSONL 파티션에 저장했습니다.

### 10.2 날짜 범위를 확대한 부하 실행

코드를 바꾸지 않고 날짜만 **2012년 2월 1일~12월 31일**로 확장했습니다.
외부 서비스에 순간 부하를 만들지 않도록 요청 사이에는 1초 간격을 유지했습니다.

| 측정 항목 | 기준: 1월 | 확대: 2~12월 | 변화 |
|---|---:|---:|---:|
| 입력 날짜 | 31일 | 335일 | 10.81배 |
| 입력 요청 | 124회 | 1,340회 | 10.81배 |
| 입력 응답 항목 | 4,823건 | 51,793건 | 10.74배 |
| 중복 제거 처리 | 2,558건 | 26,639건 | 10.41배 |
| 최종 저장 | 2,410건 | 26,584건 | 11.03배 |
| 실행 시간 | 177.563초 | 1,915.851초 | 10.79배 |
| 저장 처리량 | 13.57건/초 | 13.88건/초 | 유사 |
| 치명적 오류 | 0건 | 0건 | 변화 없음 |
| 100건 제한 경고 | 0건 | 3건 | 3건 증가 |

입력량과 실행 시간이 거의 비례했고 저장 처리량은 유지됐습니다. 따라서 현재
범위에서는 처리량 저하나 프로세스 실패보다 Google News 검색 결과의 100건 제한이
먼저 관찰된 확장 한계입니다. 제한에 도달한 3개 요청은 보고서에 남겼으며, 해당
날짜·주제군에는 검색 결과 누락 가능성이 있습니다.

### 10.3 실행 명령과 근거 파일

```bash
# 기준 실행
python -m collectors.google_news \
  --start-date 2012-01-01 --end-date 2012-01-31 \
  --output-root data/raw/google-news \
  --report data/reports/google-news-2012-01.json \
  --request-delay 1

# 확대 실행
python -m collectors.google_news \
  --start-date 2012-02-01 --end-date 2012-12-31 \
  --output-root data/raw/google-news \
  --report data/reports/google-news-2012-02-to-12.json \
  --request-delay 1
```

실제 대용량 원본과 생성 데이터는 `.gitignore`로 제외합니다. GitHub에는 수집기,
이 문서와 재현 명령을 제출하며 원본 파일은 올리지 않습니다. 필수 3·4의 장애
재현과 복구 검증은 아래 11절에서 이 기준값과 비교합니다.

## 11. 필수 3·4 — 장애 재현과 복구 검증

외부 서비스에는 장애나 부하를 가하지 않았습니다. 이미 저장한 2012년 1월
Reddit 원본과 Google News 일별 파일만 사용했으며, 실험 출력과 PostgreSQL도
모두 로컬 환경에 격리했습니다.

### 11.1 실험 입력과 처리 규칙

| 입력 | 원본 행 | 처리 규칙 | 처리 후 행 |
|---|---:|---|---:|
| Reddit 2012-01 | 15,060,640건 | 확정 21개 subreddit, 빈 값·삭제 댓글 제외 | 2,933,375건 |
| Google News 2012-01 | 2,410건 | 저장된 날짜 범위 데이터 사용 | 2,410건 |
| 합계 | 15,063,050건 | `event_id`를 공통 키로 사용 | 2,935,785건 |

결과는 `year/month/day/source_name` 파티션의 Zstandard Parquet로 저장합니다.
Google News를 다시 호출하지 않으므로 외부 API에 실험 부하가 전달되지 않습니다.

### 11.2 장애 A — 처리 작업 저장 직전 강제 실패

`--inject-failure-before-write`를 지정해 모든 필터와 변환, 건수 계산을 마친 뒤
Parquet 저장 직전에 `InjectedProcessingFailure`를 발생시켰습니다.

| 확인 항목 | 장애 실행 결과 |
|---|---:|
| 입력 행 | 15,063,050건 |
| 처리 완료 행 | 2,935,785건 |
| 장애 발생 위치 | 처리 완료 후, 출력 저장 전 |
| 실행 시간 | 8.847초 |
| 최종 출력 경로 생성 | 아니요 |
| 부분 저장 데이터 | 0건 |

최종 출력이 없는 것을 확인했으므로 실패 결과를 정상 데이터로 오인하거나 다음
작업이 부분 데이터를 읽을 가능성이 없습니다.

```bash
python -m jobs.january_processing_experiment \
  --reddit-input data/raw/reddit-archive/data/RC_2012-01.parquet \
  --google-news-glob 'data/raw/google-news/year=2012/month=01/day=*/events.jsonl' \
  --output-root data/experiments/week5/january-recovery \
  --subreddit-file config/subreddits-analysis.txt \
  --report data/reports/week5-processing-failure.json \
  --inject-failure-before-write
```

#### 처리 장애 복구

입력과 코드는 그대로 두고 장애 주입 옵션만 제거해 다시 실행했습니다.

| 확인 항목 | 복구 결과 |
|---|---:|
| 처리 행 | 2,935,785건 |
| 저장 행 | 2,935,785건 |
| 고유 `event_id` | 2,935,785건 |
| 누락 | 0건 |
| 중복 | 0건 |
| Parquet 파일 | 79개 |
| 저장 크기 | 360,388,006 bytes (약 343.69MiB) |
| 실행 시간 | 28.846초 |

처리 건수, 저장 건수와 고유 ID 수가 모두 같으므로 강제 중단 후 복구 과정에서
데이터가 빠지거나 중복되지 않았습니다.

### 11.3 장애 B — 로컬 PostgreSQL 연결 실패

전체 처리 결과에서 Reddit과 Google News를 각각 100건씩 뽑아 DB 장애 실험에
사용했습니다. 대용량 DB 성능 실험이 아니라 연결 실패·트랜잭션·멱등 복구를
확인하는 목적이므로 200건으로 제한했습니다.

먼저 PostgreSQL이 열려 있지 않은 `127.0.0.1:55432`로 연결해 다음 오류를
재현했습니다.

```text
OperationalError: connection to server at 127.0.0.1,
port 55432 failed: Connection refused
```

| 확인 항목 | 장애 결과 |
|---|---:|
| 적재 시도 | 200건 |
| 연결 실패 시간 | 0.002초 |
| 트랜잭션 시작 | 아니요 |
| 장애 후 적재 | 0건 |

#### DB 연결 복구와 중복 실행 검증

DSN의 포트를 정상 PostgreSQL `5432`로 되돌린 뒤 같은 200건을 하나의
트랜잭션으로 upsert했습니다. 이어서 동일 데이터를 한 번 더 적재했습니다.

| 검증 시점 | 전체 행 | 고유 ID | 누락 | 중복 |
|---|---:|---:|---:|---:|
| 장애 직후 | 0건 | 0건 | 200건 | 0건 |
| 정상 포트 복구 후 | 200건 | 200건 | 0건 | 0건 |
| 동일 배치 재실행 후 | 200건 | 200건 | 0건 | 0건 |

독립적인 `psql` 조회에서도 `reddit=100/고유 ID 100`,
`web_news=100/고유 ID 100`으로 확인됐습니다. `event_id` 기본 키와 upsert 때문에
동일 배치를 다시 실행해도 행 수가 400건으로 늘어나지 않았습니다.

```bash
python -m jobs.postgres_recovery_experiment \
  --input-root data/experiments/week5/january-recovery \
  --wrong-dsn 'postgresql://USER:PASSWORD@127.0.0.1:55432/news_pipeline' \
  --correct-dsn 'postgresql://USER:PASSWORD@127.0.0.1:5432/news_pipeline' \
  --per-source 100 \
  --report data/reports/week5-postgres-recovery.json
```

실제 비밀번호는 `.env`로 관리하고 GitHub 문서와 명령에는 포함하지 않습니다.

### 11.4 실행 중 추가로 발견한 실제 장애

첫 Spark 시도에서는 `local[*]`로 모든 CPU 코어를 사용하면서 전체 ID 전역 정렬을
수행해 `java.lang.OutOfMemoryError: Java heap space`가 발생했습니다. 계획한 강제
실패가 아니라 실제 자원 장애였으므로 다음과 같이 조정한 후 통제된 실험을 다시
실행했습니다.

- 로컬 동시 실행을 `local[4]`로 제한
- Spark driver heap을 8GiB로 설정
- source-prefixed 안정 ID 특성상 불필요했던 저장 전 전역 정렬 제거
- shuffle partition을 64개로 설정

조정 후 동일 입력 전체를 28.846초에 저장하고 누락·중복 0건을 확인했습니다.

### 11.5 공개 가능한 결과 근거

| 파일 | 내용 |
|---|---|
| [`results/processing-failure.json`](results/processing-failure.json) | 저장 직전 강제 실패 결과 |
| [`results/processing-recovery.json`](results/processing-recovery.json) | 처리 복구와 전체 행 회계 |
| [`results/postgres-recovery.json`](results/postgres-recovery.json) | DB 연결 실패·복구·중복 실행 결과 |
| [`jobs/january_processing_experiment.py`](../../../jobs/january_processing_experiment.py) | Spark 장애 주입 및 복구 검증 코드 |
| [`jobs/postgres_recovery_experiment.py`](../../../jobs/postgres_recovery_experiment.py) | PostgreSQL 연결 실패와 멱등 upsert 검증 코드 |
