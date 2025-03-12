# report.py
import re
import time

import gspread
from pyasn1_modules.rfc5280 import postal_code
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import quote

from google_form_package import Sheet
from process_handler import ProcessHandler

web_sheet = Sheet()
driver = web_sheet.set_driver()
wait = WebDriverWait(driver, 10)

def set_detail_sheet(worksheet):
    worksheet.clear()
    headers = ["Suburb", "Total VBA per suburb"]
    worksheet.append_row(headers)
    print("Reset the Report sheet.")
    return worksheet

def extract(sheet):
    retries = 3
    delay = 5
    for attempt in range(retries):
        try:
            sheet_header = sheet.row_values(1)
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(
                    f"Read quota error when fetching header. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch worksheet header after multiple attempts.")

    try:
        suburb_idx = sheet_header.index("Suburb") + 1
        postcode_idx = sheet_header.index("Postcode") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return []

    for attempt in range(3):
        try:
            all_rows = sheet.get_all_values()[1:]
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(
                    f"Read quota error when fetching all values. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch all values after 3 attempts.")
    base_list = []
    for row_num, row in enumerate(all_rows, start=2):
        suburb = row[suburb_idx - 1] if len(row) >= suburb_idx else ""
        postcode = row[postcode_idx - 1] if len(row) >= postcode_idx else ""
        if not suburb:
            break
        mixed = {suburb : postcode}
        base_list.append(mixed)
    return base_list

def extract_detail(sheet):
    retries = 3
    delay = 5
    for attempt in range(retries):
        try:
            sheet_header = sheet.row_values(1)
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(
                    f"Read quota error when fetching header. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch worksheet header after multiple attempts.")

    try:
        postcode_idx = sheet_header.index("postcode") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return []

    for attempt in range(3):
        try:
            all_rows = sheet.get_all_values()[1:]
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(
                    f"Read quota error when fetching all values. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch all values after 3 attempts.")
    base_list = []
    for row_num, row in enumerate(all_rows, start=2):
        postcode = row[postcode_idx - 1] if len(row) >= postcode_idx else ""
        if not postcode:
            break
        base_list.append(postcode)
    return base_list


def main():
    detail_sheet = web_sheet.get_worksheet("PractitionerDetail")
    report_sheet = web_sheet.get_worksheet("Report")
    base_sheet = web_sheet.get_worksheet("VIC Suburbs - Tracking")

    set_detail_sheet(report_sheet)

    base = extract(base_sheet)
    detail_postcodes = extract_detail(detail_sheet)

    postcode_counts = {}
    for pc in detail_postcodes:
        if pc:
            postcode_counts[pc] = postcode_counts.get(pc, 0) + 1

    suburb_to_postcodes = {}
    for record in base:
        for suburb, pc in record.items():
            if suburb in suburb_to_postcodes:
                suburb_to_postcodes[suburb].add(pc)
            else:
                suburb_to_postcodes[suburb] = {pc}

    report_data = []
    for suburb, postcodes in suburb_to_postcodes.items():
        total_count = sum(postcode_counts.get(pc, 0) for pc in postcodes)
        report_data.append([suburb, total_count])

    retries = 5
    delay = 30
    for attempt in range(retries):
        try:
            report_sheet.update(values=report_data, range_name="A2", value_input_option="USER_ENTERED")
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"Batch update quota error on attempt {attempt + 1}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                raise

    else:
            raise Exception("Failed to append row after multiple attempts.")

    print("Saved every data into the Report Sheet successfully.")

if __name__ == "__main__":
    main()