# clear.py
import json
from google_form_package import Sheet


def main():
    web_sheet = Sheet()
    progress_sheet = web_sheet.get_worksheet("Progress")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "UrlNum": 1})]], range_name="A1")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 0})]], range_name="A2")

if __name__ == "__main__":
    main()