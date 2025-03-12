# practitioner_link_12.py
import time
import csv
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


def set_postcode():
    f = open('refined_db.csv', 'r', encoding='utf-8')
    rdr = csv.reader(f)
    postcode = []
    for line in rdr:
        postcode.append(line)
    f.close()
    return postcode


def set_link_sheet(worksheet):
    worksheet.clear()
    headers = ["postcode", "Name", "Link"]
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


def batch_append_multiple_rows(worksheet, rows_list, retries=10, delay=60):
    for attempt in range(retries):
        try:
            worksheet.append_rows(rows_list, value_input_option='USER_ENTERED')
            return
        except gspread.exceptions.APIError as e:
            if any(code in str(e) for code in ["429", "500", "502", "503", "504"]):
                print(f"API Error ({e}). Retry after {delay} seconds... (attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    print("Failed to append multiple rows.")


def find_element(element_driver, tag):
    max_retries = 1
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
    max_retries = 1
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
                return []


def main():
    link_sheet = web_sheet.get_worksheet("PractitionerLink")
    progress_sheet = web_sheet.get_worksheet("Progress")
    postcode_list = set_postcode()
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "UrlNum": 11}, "L1")
    progress = ph.load_progress()
    if progress["progress"] == "setting":
        set_link_sheet(link_sheet)
    link_sheet.update([["Running Scrapping"]], "D1")
    if progress["progress"] != "finished":
        progress["progress"] = "processing"
        append_list = []
        while progress["UrlNum"] < len(postcode_list):
            postcode = postcode_list[progress["UrlNum"]]
            driver.get(URL)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait_for_page_load(driver)
            postcode_input = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//input[@name='postcode']"))
            )
            postcode_input.clear()
            postcode_input.send_keys(postcode[0])
            search_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Search']")))
            search_button.click()
            page_buttons = find_elements(driver, ".//button[contains(@kx-type, 'underline')]")
            page_numbers = []
            links = []
            if not page_buttons:
                page_numbers = [str(1)]
            else:
                for page_button in page_buttons:
                    page_numbers.append(page_button.text)

            print(f"Starting {postcode[0]}")
            pagenum = 1
            post_list = []
            while str(pagenum) in page_numbers:
                page_buttons = find_elements(driver, ".//button[contains(@kx-type, 'underline')]")
                if page_buttons:
                    for page_button in page_buttons:
                        if page_button.text == str(pagenum):
                            page_button.click()
                            break
                    wait_for_page_load(driver)
                    practitioners = find_elements(driver,
                                                  "//lightning-layout-item[contains(@class, 'search-result-style')]")

                    print(f"Page: {pagenum}")
                    if practitioners:
                        for practitioner in practitioners:
                            head = find_element(practitioner, ".//a[contains(@class, 'search-result-name-text-style')]")
                            if head != "N/A":
                                name = head.text
                                link = head.get_attribute("href")
                            else:
                                name = "Failed to load practitioner"
                                link = "Failed to load practitioner link"
                            links.append([postcode[0], name, link])

                        post_list.append(links)
                        links = []
                        pagenum += 1
                    else:
                        pagenum += 1
                else:
                    wait_for_page_load(driver)
                    practitioners = find_elements(driver,
                                                  "//lightning-layout-item[contains(@class, 'search-result-style')]")

                    print(f"Page: {pagenum}")

                    if practitioners:
                        for practitioner in practitioners:
                            head = find_element(practitioner, ".//a[contains(@class, 'search-result-name-text-style')]")
                            if head != "N/A":
                                name = head.text
                                link = head.get_attribute("href")
                            else:
                                name = "Failed to load practitioner"
                                link = "Failed to load practitioner link"
                            links.append([postcode[0], name, link])

                        post_list.append(links)
                        break
                    else:
                        break
            append_list.append(post_list)

            if len(append_list) >= 20:
                update = [row for post in append_list for page in post for row in page]
                batch_append_multiple_rows(link_sheet, update)
                ph.save_progress(progress)
                append_list = []
            progress["UrlNum"] += 20


        if append_list:
            update = [row for post in append_list for page in post for row in page]
            batch_append_multiple_rows(link_sheet, update)

        progress["progress"] = "finished"
        ph.save_progress(progress)

        link_sheet.update([["Finished Scrapping"]], "D1")
        print("Scraping link finished")
        driver.quit()
    else:
        print("Finished already")


if __name__ == "__main__":
    main()
