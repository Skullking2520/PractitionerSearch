# practitioner_link.py
import time

import gspread
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from google_form_package import Sheet
from process_handler import ProcessHandler

web_sheet = Sheet()
driver = web_sheet.set_driver()
wait = WebDriverWait(driver, 10)
URL = "https://bams.vba.vic.gov.au/bams/s/practitioner-search"

def set_link_sheet(worksheet):
    worksheet.clear()
    headers = ["Name", "Link"]
    worksheet.append_row(headers)
    return worksheet

def wait_for_page_load(base_driver, timeout=15):
    try:
        WebDriverWait(base_driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")

def batch_append_multiple_rows(worksheet, rows_list, retries=3, delay=10):
    for attempt in range(retries):
        try:
            worksheet.append_rows(rows_list, value_input_option='USER_ENTERED')
            return
        except gspread.exceptions.APIError as e:
            if any(code in str(e) for code in ["500", "502", "503", "504"]):
                print(f"API Error ({e}). Retry after {delay} seconds... (attempt {attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    print("Failed to append multiple rows.")

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
    link_sheet = web_sheet.get_worksheet("PractitionerLink")
    progress_sheet = web_sheet.get_worksheet("Progress")
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "UrlNum": 1}, "A1")
    progress = ph.load_progress()
    set_link_sheet(link_sheet)
    link_sheet.update([["Running Scrapping"]], "C1")

    progress["progress"] = "processing"
    driver.get(URL)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    wait_for_page_load(driver)
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Search']")))
    search_button.click()
    page_buttons = find_elements(driver, ".//button[contains(@kx-type, 'underline')]")
    page_numbers = []
    links = []

    for page_button in page_buttons:
        page_numbers.append(page_button.text)

    print("Starting scrapping link")

    while str(progress["UrlNum"]) in page_numbers:
        page_buttons = find_elements(driver, ".//button[contains(@kx-type, 'underline')]")
        for page_button in page_buttons:
            if page_button.text == str(progress["UrlNum"]):
                page_button.click()
                break
        wait_for_page_load(driver)
        practitioners = find_elements(driver, "//lightning-layout-item[contains(@class, 'search-result-style')]")

        print(f"Page: {progress["UrlNum"]}")

        for practitioner in practitioners:
            head = find_element(practitioner, ".//a[contains(@class, 'search-result-name-text-style')]")
            if head != "N/A":
                name = head.text
                link = head.get_attribute("href")
            else:
                name = "Failed to load practitioner"
                link = "Failed to load practitioner link"
            links.append([name, link])

        batch_append_multiple_rows(link_sheet, links)
        links = []
        progress["UrlNum"] += 1

    progress["progress"] = "finished"
    progress["UrlNum"] = 1
    ph.save_progress(progress)

    link_sheet.update([["Finished Scrapping"]], "C1")
    print("Scraping link finished")
    driver.quit()

if __name__ == "__main__":
    main()