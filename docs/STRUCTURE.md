# 파일 구조 안내

우편번호별 검색 결과에서 전문직 등록 정보의 링크와 상세 내용을 수집하는 업무 코드입니다.

실행 파일 이름은 기존 작업 구간·워크플로와 연결되어 있어 유지했습니다. `docs/`에서 역할과 시작점을 찾을 수 있습니다.

| 구분 | 파일 |
| --- | --- |
| `clear.py` | [clear.py](<../clear.py>) |
| `google_form_package.py` | [google_form_package.py](<../google_form_package.py>) |
| `practitioner_detail_*.py` | [practitioner_detail_01.py](<../practitioner_detail_01.py>), [practitioner_detail_02.py](<../practitioner_detail_02.py>), [practitioner_detail_03.py](<../practitioner_detail_03.py>), [practitioner_detail_04.py](<../practitioner_detail_04.py>), [practitioner_detail_05.py](<../practitioner_detail_05.py>), [practitioner_detail_06.py](<../practitioner_detail_06.py>), [practitioner_detail_07.py](<../practitioner_detail_07.py>), [practitioner_detail_08.py](<../practitioner_detail_08.py>), [practitioner_detail_09.py](<../practitioner_detail_09.py>), [practitioner_detail_10.py](<../practitioner_detail_10.py>), [practitioner_detail_11.py](<../practitioner_detail_11.py>), [practitioner_detail_12.py](<../practitioner_detail_12.py>), [practitioner_detail_13.py](<../practitioner_detail_13.py>), [practitioner_detail_14.py](<../practitioner_detail_14.py>), [practitioner_detail_15.py](<../practitioner_detail_15.py>), [practitioner_detail_16.py](<../practitioner_detail_16.py>), [practitioner_detail_17.py](<../practitioner_detail_17.py>), [practitioner_detail_18.py](<../practitioner_detail_18.py>), [practitioner_detail_19.py](<../practitioner_detail_19.py>), [practitioner_detail_20.py](<../practitioner_detail_20.py>) |
| `practitioner_link_*.py` | [practitioner_link_01.py](<../practitioner_link_01.py>), [practitioner_link_02.py](<../practitioner_link_02.py>), [practitioner_link_03.py](<../practitioner_link_03.py>), [practitioner_link_04.py](<../practitioner_link_04.py>), [practitioner_link_05.py](<../practitioner_link_05.py>), [practitioner_link_06.py](<../practitioner_link_06.py>), [practitioner_link_07.py](<../practitioner_link_07.py>), [practitioner_link_08.py](<../practitioner_link_08.py>), [practitioner_link_09.py](<../practitioner_link_09.py>), [practitioner_link_10.py](<../practitioner_link_10.py>), [practitioner_link_11.py](<../practitioner_link_11.py>), [practitioner_link_12.py](<../practitioner_link_12.py>), [practitioner_link_13.py](<../practitioner_link_13.py>), [practitioner_link_14.py](<../practitioner_link_14.py>), [practitioner_link_15.py](<../practitioner_link_15.py>), [practitioner_link_16.py](<../practitioner_link_16.py>), [practitioner_link_17.py](<../practitioner_link_17.py>), [practitioner_link_18.py](<../practitioner_link_18.py>), [practitioner_link_19.py](<../practitioner_link_19.py>), [practitioner_link_20.py](<../practitioner_link_20.py>) |
| `process_handler.py` | [process_handler.py](<../process_handler.py>) |
| `report.py` | [report.py](<../report.py>) |

`google_form_package.py`는 Google Sheets 인증·브라우저 설정을, `process_handler.py`는 진행 상태 기록을 담당합니다.

`refined_db.csv`는 우편번호 검색 입력입니다. 현재 파일의 열과 각 분할 파일의 시작·종료 인덱스를 확인해야 합니다. 수집한 개인별 상세 정보와 같은 결과 데이터와는 구분합니다.

## 실행 조건

루트 디렉터리에서 `requirements.txt`를 설치합니다. 브라우저 수집은 Chrome과 호환 드라이버가 필요하고, 시트 기록에는 테스트 시트와 쓰기 권한이 필요합니다. 외부 사이트·시트에 연결하는 실행은 문서 정리 검증 과정에서 수행하지 않았습니다.
