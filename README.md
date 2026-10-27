# 등록 전문직 정보 수집

Victoria Building Authority의 전문직 검색 페이지에서 등록 정보를 찾아 Google Sheets에 정리하는 인턴 업무 저장소입니다.

Seeka Technology 인턴 기간(2025.01–05)에 작성한 코드입니다.

**읽는 순서:** [검색 링크](scripts/practitioner_link_01.py) → [등록 상세](scripts/practitioner_detail_01.py) → [결과 처리](scripts/report.py)

## 처리 구조

| 단계 | 코드 | 역할 |
| --- | --- | --- |
| 검색 결과 수집 | [practitioner_link_01.py](scripts/practitioner_link_01.py) 및 번호별 파일 | 검색 조건별 결과 링크 수집 |
| 상세 정보 수집 | [practitioner_detail_01.py](scripts/practitioner_detail_01.py) 및 번호별 파일 | 등록 정보 상세 조회·적재 |
| 결과 처리 | [report.py](scripts/report.py), [clear.py](scripts/clear.py) | 수집 결과 후처리 |
| 작업 관리 | [process_handler.py](scripts/process_handler.py) | 작업 진행 관리 |

번호별 파일은 수집 범위를 나눈 동일 업무의 구성입니다. 먼저 링크 수집과 상세 수집의 관계를 보면 전체 흐름을 이해하기 쉽습니다.

## 실행 준비

Python, Chrome·ChromeDriver, Google Sheets 접근 권한과 서비스 계정이 필요합니다.

```bash
python -m pip install -r requirements.txt
```

시트 연결과 단계별 실행 순서는 [workflow 기록](.github/workflows/scrapping.yaml)과 [시트 연결 코드](scripts/google_form_package.py)를 참고하세요. 상세 수집에는 링크 수집 결과가 필요합니다.

## 현재 자동화 설정

현재 workflow 파일은 전체가 주석 처리돼 있습니다. 과거 설정에는 달력 날짜를 기준으로 실행하는 `0 15 */3 * *` 스케줄이 남아 있습니다. 현재 실행 여부나 최신 데이터 보유량을 뜻하지 않습니다.

## 인증과 저장 대상

[.env.example](.env.example)을 참고해 셸 환경 변수로 설정합니다. 파일은 자동으로 로드되지 않습니다.

| 변수 | 준비할 값 |
| --- | --- |
| `SERVICE_ACCOUNT_KEY` | 테스트 시트에 접근할 서비스 계정 JSON |
| `SPREADSHEET_URL` | 본인이 관리하는 HTTPS Google Sheets URL |

서비스 계정에 테스트 시트를 공유하고, 실행할 스크립트의 워크시트·입력 열을 먼저 준비합니다. 실행하면 실제 외부 수집과 시트 쓰기가 발생합니다.

시트 생성·초기화 함수는 기존 데이터를 지울 수 있습니다. 번호가 붙은 파일을 모두 순서대로 실행하는 방식이 아니며, 분할 구간과 시트 이름을 확인한 파일만 테스트 시트에서 실행합니다.

## 코드와 자료의 범위

업무 시트·인증 파일·수집 원본은 실행 예제로 제공하지 않습니다. 생성 결과와 인증 파일은 버전 관리에서 제외하며, 제외 규칙은 과거 이력을 제거하지 않습니다.

이번 정리에서 실행 코드와 보조 모듈을 `scripts/`로 옮겼습니다. 번호별 작업 구간과 기존 처리 로직은 유지합니다. 의존성과 설정 예시는 루트에 두며, 실행 명령의 기준 경로는 저장소 루트입니다.

링크 수집은 루트의 `refined_db.csv`를 읽습니다. 이 파일은 네 자리 우편번호 목록이며 수집한 전문직 개인정보 결과물이 아닙니다.

설정과 입력 시트를 준비한 뒤 시작 단계만 실행하는 예:

```bash
python scripts/practitioner_link_01.py
```

시트 주소는 `https://docs.google.com/spreadsheets/d/…` 형식이어야 합니다. 누락된 설정·잘못된 URL·유효하지 않은 서비스 계정 JSON은 연결 전에 거부하며, 오류 메시지에 JSON 원문을 넣지 않습니다.

외부 수집 없이 설정·인증 처리를 검사하려면 의존성을 설치한 뒤 `python -m unittest discover -s tests`를 실행합니다.
