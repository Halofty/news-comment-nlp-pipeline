"""기사 전문 수집 확장을 위한 자리표시자 모듈.

TODO: MVP 이후 선택적 확장 단계에서 구현합니다.

예정된 책임:
- GDELT 이벤트의 원문 URL 요청
- robots 정책, 이용약관과 요청 속도 제한 확인
- HTML에서 기사 본문과 게시 시각 추출
- 본문 정제, 길이 제한과 content hash 생성
- 추출 성공 시 ``text_scope=full_text``로 변환
- 접근 제한 또는 추출 실패 시 ``text_scope=title_only`` 유지

현재 MVP에서는 이 모듈을 import하거나 실행하지 않으며 GDELT 제목만 분석합니다.
"""
