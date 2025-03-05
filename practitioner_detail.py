# practitioner_detail.py
import re
import time

import gspread
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

def append_row_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.append_row(data, value_input_option="USER_ENTERED")
            return
        except gspread.exceptions.APIError as e:
            if any(code in str(e) for code in ["500", "502", "503", "504", "429"]):
                print(f"Error occurred. Retry after {delay} seconds ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"Failed to append rows {data} after {retries} attempts.")
                return

def set_detail_sheet(worksheet):
    worksheet.clear()
    headers = ["Name", "Business address", "Contact Details", "Building practitioner registration",
               "Limitations", "Conditions", "Status", "Registration number", "Commenced", "Anniversary",
               "Expires", "Date registration was suspended, cancelled or surrendered (if applicable)",
               "Reason for Suspension or Cancellation (if applicable)", "Director Name", "Partnership details"," lat", "long"]
    worksheet.append_row(headers)
    return worksheet

def wait_for_page_load(wait_driver, timeout=15):
    try:
        WebDriverWait(wait_driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")

def extract(sheet):
    retries = 3
    delay = 5
    for attempt in range(retries):
        try:
            sheet_header = sheet.row_values(1)
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"Read quota error when fetching header. Retrying in {delay} seconds... (Attempt {attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch worksheet header after multiple attempts.")

    try:
        link_idx = sheet_header.index("Link") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return []

    for attempt in range(3):
        try:
            all_rows = sheet.get_all_values()[1:]
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"Read quota error when fetching all values. Retrying in {delay} seconds... (Attempt {attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch all values after 3 attempts.")
    link_list = []
    for row_num, row in enumerate(all_rows, start=2):
        link = row[link_idx - 1] if len(row) >= link_idx else ""
        if not link:
            break
        link_list.append(link)
    return link_list

def find_element(element_driver, tag):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            element = WebDriverWait(element_driver, 10).until(
                EC.presence_of_element_located((By.XPATH, tag))
            )
            return element
        except (NoSuchElementException, TimeoutException):
            if attempt < max_retries - 1:
                print(f"retry {attempt + 1}/{max_retries} ...")
                time.sleep(1)
            else:
                print("Failed to find element.")
                return "N/A"

def find_elements(element_driver, tag):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            elements = WebDriverWait(element_driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, tag))
            )
            return elements
        except (NoSuchElementException, TimeoutException):
            if attempt < max_retries - 1:
                print(f"retry {attempt + 1}/{max_retries} ...")
                time.sleep(1)
            else:
                print("Failed to find element.")
                return "N/A"

def main():
    detail_sheet = web_sheet.get_worksheet("PractitionerDetail")
    link_sheet = web_sheet.get_worksheet("PractitionerLink")
    progress_sheet = web_sheet.get_worksheet("Progress")
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "RowNum": 0}, "A2")
    progress = ph.load_progress()
    if progress["progress"] == "setting":
        set_detail_sheet(detail_sheet)
    link_list = extract(link_sheet)
    if progress["progress"] != "finished":
        detail_sheet.update([["Running Scrapping"]], "R1")
        while progress["RowNum"] < len(link_list):
            progress["progress"] = "processing"
            driver.get(link_list[progress["RowNum"]])
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait_for_page_load(driver)
            print(f"current page: row {progress["RowNum"]}")
            name = find_element(driver, ".//lightning-layout-item[contains(@class, 'summary-view-responsive-style practitioner-name-style')]").text
            details = find_elements(driver,
                                       ".//lightning-layout-item[contains(@class, 'detail-value-responsive-style')]")
            raw_address = details[0].text
            address = raw_address.replace("\n", ", ")
            contact = details[1].text
            raw_limitations = details[2].text
            limitations = raw_limitations.replace("\n", ", ")
            raw_conditions = details[3].text
            conditions = raw_conditions.replace("\n", ", ")
            status = details[4].text
            registration_number = details[5].text
            commenced = details[6].text
            anniversary = details[7].text
            expires = details[8].text
            date_scs = details[9].text
            raw_reason_scs = details[10].text
            reason_scs = raw_reason_scs.replace("\n", ", ")
            raw_director_name = details[11].text
            director_name = raw_director_name.replace("\n", ", ")
            partnership = find_element(driver, "//p[contains(@class, 'sub-header-text-style') and contains(text(), 'Partnership details')]/following-sibling::div//span").text
            maps_url = f"https://www.google.com/maps/search/?api=1&query={quote(address)}"
            driver.get(maps_url)
            WebDriverWait(driver, 30).until(lambda d: "@" in d.current_url)
            map_url = driver.current_url
            pattern = r"@(-?\d+\.\d+),(-?\d+\.\d+)"
            match = re.search(pattern, map_url)
            if match:
                va_lat, va_long = match.groups()
            else:
                va_lat = "No lat given"
                va_long = "No long given"
            
            updates = [name,
                       address,
                       contact,
                       limitations,
                       conditions,
                       status,
                       registration_number,
                       commenced,
                       anniversary,
                       expires,
                       date_scs,
                       reason_scs,
                       director_name,
                       partnership,
                       va_lat,
                       va_long
                       ]
            append_row_with_retry(detail_sheet, updates)
            progress["RowNum"] += 1
            ph.save_progress(progress)
    
        progress["progress"] = "finished"
        progress["RowNum"] = 0
        ph.save_progress(progress)
        detail_sheet.update([["Finished Scrapping"]], "R1")
        driver.quit()
        print("Saved every data into the Google Sheet successfully.")
    else:
        print("Finished already")

if __name__ == "__main__":
    main()
