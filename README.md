# 등록 전문직 정보 수집

Victoria Building Authority의 전문직 검색 페이지에서 등록 정보를 찾아 Google Sheets에 정리하는 인턴 업무 저장소입니다.

## 수행 배경

Seeka Technology 소프트웨어 개발 인턴 업무로 진행했습니다. 인턴 기간은 2025년 1월–5월이며, 원격근무로 데이터 수집·처리 업무를 수행했습니다.

## 처리 구조

| 단계 | 코드 | 역할 |
| --- | --- | --- |
| 검색 결과 수집 | [practitioner_link_01.py](practitioner_link_01.py) 및 번호별 파일 | 검색 조건별 결과 링크 수집 |
| 상세 정보 수집 | [practitioner_detail_01.py](practitioner_detail_01.py) 및 번호별 파일 | 등록 정보 상세 조회·적재 |
| 결과 처리 | [report.py](report.py), [clear.py](clear.py) | 수집 결과 후처리 |
| 작업 관리 | [process_handler.py](process_handler.py) | 작업 진행 관리 |

번호별 파일은 수집 범위를 나눈 동일 업무의 구성입니다. 먼저 링크 수집과 상세 수집의 관계를 보면 전체 흐름을 이해하기 쉽습니다.

## 실행 준비

Python, Chrome·ChromeDriver, Google Sheets 접근 권한과 서비스 계정이 필요합니다.

```bash
python -m pip install -r requirements.txt
```

시트 연결과 단계별 실행 순서는 [workflow 기록](.github/workflows/scrapping.yaml)과 [시트 연결 코드](google_form_package.py)를 참고하세요. 상세 수집에는 링크 수집 결과가 필요합니다.

## 현재 자동화 설정

현재 workflow 파일은 전체가 주석 처리돼 있습니다. 과거 설정에는 달력 날짜를 기준으로 실행하는 `0 15 */3 * *` 스케줄이 남아 있습니다. 현재 실행 여부나 최신 데이터 보유량을 뜻하지 않습니다.


## 파일을 읽는 순서

| 시작 파일 | 역할 |
| --- | --- |
| [practitioner_link_01.py](<practitioner_link_01.py>) | 우편번호별 링크 수집 |
| [practitioner_detail_01.py](<practitioner_detail_01.py>) | 상세 정보 수집 |
| [report.py](<report.py>) | 집계 |
| [clear.py](<clear.py>) | 시트 정리 |

전체 파일과 분할 스크립트는 [구조 안내](docs/STRUCTURE.md)에 정리했습니다.

## 인증과 저장 대상

`SERVICE_ACCOUNT_KEY`에는 Google 서비스 계정 JSON을, `SPREADSHEET_URL`에는 본인이 관리하는 테스트 시트 URL을 환경 변수로 설정합니다. `.env.example`은 변수 목록이며 자동으로 로드되지 않습니다. 셸에서 환경 변수를 설정한 후 실행해야 합니다. 기존 업무 시트 주소와 인증 파일 저장을 제거했으므로 새 저장 대상을 먼저 지정해야 합니다.

시트 생성·초기화 함수는 기존 데이터를 지울 수 있습니다. 번호가 붙은 파일을 모두 순서대로 실행하는 방식이 아니며, 분할 구간과 시트 이름을 확인한 파일만 테스트 시트에서 실행합니다.

## 저장소 관리

실행 결과·인증 파일은 버전 관리에서 제외합니다. 공개 소스와 수집 데이터의 공개 범위는 구분하며, 인턴 업무 데이터·시트 접근 권한은 포함하지 않습니다. 보안 관련 내용은 [관리 기준](SECURITY.md)을 참고하세요.
