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
