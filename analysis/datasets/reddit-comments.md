# Pushshift Reddit 댓글 데이터셋 명세

- 카탈로그 ID: `pushshift-reddit-comments`
- 제공 위치: Hugging Face `fddemarco/pushshift-reddit-comments`
- 데이터 페이지: <https://huggingface.co/datasets/fddemarco/pushshift-reddit-comments>
- 원천 표기: Pushshift Reddit comments archive
- 확인 기준일: 2026-08-23
- 공통 계약: [`../../docs/architecture/data-contract.md`](../../docs/architecture/data-contract.md)
- 표본 profile: [`../reports/reddit-sample-profile.json`](../reports/reddit-sample-profile.json)

## 원본 범위와 형식

Hugging Face 데이터 페이지는 이 저장소를 Parquet 형식, 단일 `train` split, 약 18.5억 행 규모로 표시합니다. 월별 파일은 `data/RC_YYYY-MM.parquet` 이름으로 제공됩니다. 이 숫자는 외부 저장소의 현재 표시값이며 프로젝트가 전체 데이터를 내려받거나 검증했다는 뜻이 아닙니다.

현재 Collector는 Hugging Face `datasets`의 streaming 모드로 지정한 월별 Parquet를 순차적으로 읽습니다. 프로젝트에서 실제 검증한 범위는 `2016-01` 파일의 앞부분에서 얻은 유효 댓글 100건이며, 전체 월간·전체 Reddit 분포를 대표하지 않습니다.

## 이용 조건과 개인정보 판단

확인일 현재 Hugging Face 데이터 카드에는 명시적인 라이선스 식별자가 표시되지 않습니다. 따라서 데이터가 공개적으로 접근 가능하다는 사실을 자유로운 재배포·상업적 이용 허가로 해석하지 않습니다. 원천 플랫폼의 현재 정책과 적용 법률도 별도로 검토해야 합니다.

프로젝트는 보수적으로 다음 원칙을 적용합니다.

- 원문 Parquet와 변환된 댓글 원문은 `data/`에 저장하고 Git에 커밋하지 않습니다.
- `author`와 `subreddit_id`를 출력 이벤트로 복사하지 않습니다.
- 사용자 식별, 프로파일링, 광고 타기팅과 신원 추론에 사용하지 않습니다.
- 삭제·제거 표시 댓글은 분석에서 제외합니다.
- 삭제 요청이나 원천 콘텐츠 제거를 반영하는 운영 절차는 프로덕션 사용 전에 별도로 구현합니다.
- 공개 문서에는 원문이나 원본 댓글 ID 대신 집계값만 기록합니다.

Reddit의 현재 정책은 사용자 삭제와 콘텐츠 제거를 존중하고 데이터 보유·삭제 절차를 갖출 것을 요구할 수 있습니다. 이 프로젝트의 Hugging Face 미러 접근이 Reddit 공식 API 사용과 동일하다고 단정하지 않으며, 실제 배포·공개·상업 사용 전 별도의 이용 가능성 검토가 필요합니다.

## 원본 필드와 매핑

| 원본 필드 | 사용 | `TextEvent v1` | 처리 |
|---|:---:|---|---|
| `id` | O | `event_id` | `sha256("reddit:" + id)`로 비식별 ID 생성 |
| `body` | O | `text` | 앞뒤 공백 제거 후 분석 텍스트로 사용 |
| `created_utc` | O | `event_time` | Unix timestamp를 UTC ISO-8601로 변환 |
| `subreddit` | O | `community` | 커뮤니티 필터 및 분석 그룹에 사용 |
| `score` | O | `engagement` | 음수를 포함한 정수 값을 보존 |
| `link_id` | O | `metadata.link_id` | 연결 게시물의 생명주기 값으로 보존 |
| `controversiality` | O | `metadata.controversiality` | 정수 값으로 보존 |
| `author` | X | 없음 | 사용자 식별 가능성이 있어 제외 |
| `subreddit_id` | X | 없음 | 분석에 불필요해 제외 |

`id`, `body`, `created_utc`가 없거나 본문이 빈 값, `[deleted]`, `[removed]`이면 이벤트를 만들지 않습니다. Reddit 언어는 현재 별도 감지를 하지 않아 `unknown`으로 저장합니다.

## 수집 명령

검증에 사용한 명령은 다음과 같습니다.

```bash
python3 -m collectors.reddit \
  --month 2016-01 \
  --limit 100 \
  --output data/validation/reddit-2016-01-100.jsonl
```

커뮤니티를 제한할 때는 `--subreddit`을 반복합니다. 재현 정보에는 월, 필터 목록, 유효 이벤트 제한, Collector 코드 revision과 실행 시각을 함께 기록해야 합니다.

## 확인된 품질 특성

- 100건 모두 `TextEvent v1` JSON Schema와 Python 검증을 통과했습니다.
- 필수 필드 결측과 중복 `event_id`는 각각 0건이었습니다.
- `score`는 음수가 가능하므로 0 이상으로 강제하지 않습니다.
- 표본은 4초 구간과 파일 앞부분에 집중되어 대표성이 없습니다.
- 원본에는 삭제 표시, 매우 긴 텍스트, URL, 비정상 Unicode와 개인정보성 문자열이 포함될 수 있습니다.

정확한 집계값과 해석 한계는 profile과 [`../quality/validation-summary.md`](../quality/validation-summary.md)에 기록합니다.
