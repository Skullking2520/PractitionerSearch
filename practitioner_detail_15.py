# practitioner_detail_15.py
import re
import time
from urllib.parse import quote

import gspread
from requests.exceptions import ConnectionError
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from google_form_package import Sheet
from process_handler import ProcessHandler

web_sheet = Sheet()
driver = web_sheet.set_driver()
driver.set_page_load_timeout(180)
wait = WebDriverWait(driver, 10)


def append_row_with_retry(worksheet, data, retries=3, delay=60):
    for attempt in range(retries):
        try:
            worksheet.append_row(data, value_input_option="USER_ENTERED")
            return
        except Exception:
            print(f"Error occurred. Retry after {delay} seconds ({attempt + 1}/{retries})")
            time.sleep(delay)
            delay *= 2
    print(f"Failed to append rows {data} after {retries} attempts.")
    return


def set_detail_sheet(worksheet):
    worksheet.clear()
    headers = ["Name", "Category", "Business address", "Contact Details",
               "Limitations", "Conditions", "Status", "Registration number", "Commenced", "Anniversary",
               "Expires", "Date registration was suspended, cancelled or surrendered (if applicable)",
               "Reason for Suspension or Cancellation (if applicable)", "Director Name", "Partnership details", " lat",
               "long", "postcode"]
    worksheet.append_row(headers)
    print("Reset the PractitionerDetail sheet.")
    return worksheet


def wait_for_page_load(wait_driver, timeout=180):
    try:
        WebDriverWait(wait_driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")


def extract(sheet):
    retries = 10
    delay = 60
    for attempt in range(retries):
        try:
            sheet_header = sheet.row_values(1)
            break
        except Exception:
            print(
                f"Read quota error when fetching header. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
            time.sleep(delay)
            delay *= 2
    else:
        raise Exception("Failed to fetch worksheet header after multiple attempts.")

    try:
        link_idx = sheet_header.index("Link") + 1
        postcode_idx = sheet_header.index("postcode") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return []

    for attempt in range(3):
        try:
            all_rows = sheet.get_all_values()[1:]
            break
        except Exception:
            print(
                f"Read quota error when fetching all values. Retrying in {delay} seconds... (Attempt {attempt + 1}/3)")
            time.sleep(delay)
            delay *= 2
    else:
        raise Exception("Failed to fetch all values after 3 attempts.")
    link_list = []
    for row_num, row in enumerate(all_rows, start=2):
        link = row[link_idx - 1] if len(row) >= link_idx else ""
        postcode = row[postcode_idx - 1] if len(row) >= postcode_idx else ""
        if not link:
            break
        mixed = {link: postcode}
        link_list.append(mixed)
    return link_list

def load_to_seen_data():
    retries = 10
    delay = 60

    for attempt in range(retries):
        try:
            detail_sheet = web_sheet.get_worksheet("PractitionerDetail")
            break
        except Exception:
            print(
                f"load_to_seen_data: Error in get_worksheet. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
            delay *= 2
    else:
        raise Exception("load_to_seen_data: Failed to get PracticeDetail sheet after multiple attempts.")

    delay = 60
    for attempt in range(retries):
        try:
            detail_header = detail_sheet.row_values(1)
            break
        except Exception:
            print(
                f"load_to_seen_data: Error 429 in header read. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
            delay *= 2
    else:
        raise Exception("load_to_seen_data: Failed to retrieve worksheet header after multiple attempts.")

    try:
        detail_name_idx = detail_header.index("Name") + 1
        detail_address_idx = detail_header.index("Business address") + 1
    except ValueError as e:
        print("load_to_seen_data: Requested header row not found.", e)
        return

    delay = 60
    for attempt in range(retries):
        try:
            all_rows = detail_sheet.get_all_values()[1:]
            break
        except Exception:
            print(
                f"load_to_seen_data: 429 error in full row read. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
            delay *= 2
    else:
        raise Exception("load_to_seen_data: Failed to retrieve entire row after multiple attempts.")

    dup_list = []
    for row_num, row in enumerate(all_rows, start=2):
        detail_name = row[detail_name_idx - 1] if len(row) >= detail_name_idx else ""
        detail_address = row[detail_address_idx - 1] if len(row) >= detail_address_idx else ""
        detail = {detail_name: detail_address}
        dup_list.append(detail)
    return dup_list


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
                print("Failed to find elements.")
                return "N/A"


def update_category(name, address, new_category):
    retries = 10
    delay = 60

    for attempt in range(retries):
        try:
            detail_sheet = web_sheet.get_worksheet("PractitionerDetail")
            break
        except Exception as e:
            print(
                f"update_category: APIError in row_values: {e}. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
    else:
        print("update_category: Failed to get the Practice Detail sheet after multiple attempts.")
        return

    for attempt in range(retries):
        try:
            header = detail_sheet.row_values(1)
            break
        except Exception as e:
            print(
                f"update_category: APIError in row_values: {e}. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
    else:
        print("update_category: Failed to retrieve header after multiple attempts.")
        return

    try:
        name_idx = header.index("Name") + 1
        address_idx = header.index("Business address") + 1
        category_idx = header.index("Category") + 1
    except ValueError as e:
        print("No required header:", e)
        return

    for attempt in range(retries):
        try:
            all_rows = detail_sheet.get_all_values()[1:]
            break
        except Exception as e:
            print(
                f"update_category: APIError in update_cell: {e}. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
    else:
        print("update_category: update_cell failed after multiple attempts.")
        return

    for row_num, row in enumerate(all_rows, start=2):
        row_name = row[name_idx - 1] if len(row) >= name_idx else ""
        row_address = row[address_idx - 1] if len(row) >= address_idx else ""
        if row_name == name and row_address == address:
            current_category = row[category_idx - 1] if len(row) >= category_idx else ""
            categories = [cat.strip() for cat in current_category.split(",") if cat.strip()] if current_category else []
            if new_category in categories:
                print("Category exists.")
                return
            categories.append(new_category)
            updated_category = ", ".join(categories)

            for attempt in range(retries):
                try:
                    detail_sheet.update_cell(row_num, category_idx, updated_category)
                    print(f"Row {row_num}'s category has been updated to '{updated_category}'.")
                    return
                except Exception as e:
                    print(
                        f"update_category: APIError in update_cell: {e}. Retry after {delay} seconds... ({attempt + 1}/{retries})")
                    time.sleep(delay)
            else:
                print("update_category: update_cell failed after multiple attempts.")
                return

    print("Could not find matching name and address.")


def timer(start, end):
    return (end - start) < 14401

def main():
    start = time.time()
    retries = 10
    delay = 60
    for attempt in range(retries):
        try:
            detail_sheet = web_sheet.get_worksheet("PractitionerDetail")
            break
        except Exception as e:
            print(
                f"load_to_seen_data: Error in get_worksheet. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
            delay *= 2
    else:
        raise Exception("load_to_seen_data: Failed to get the PracticeDetail sheet after multiple attempts.")
    delay = 60
    for attempt in range(retries):
        try:
            link_sheet = web_sheet.get_worksheet("PractitionerLink")
            break
        except Exception as e:
            print(
                f"load_to_seen_data: Error in get_worksheet. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
            delay *= 2
    else:
        raise Exception("load_to_seen_data: Failed to get the PractitionerLink sheet after multiple attempts.")
    delay = 60
    for attempt in range(retries):
        try:
            progress_sheet = web_sheet.get_worksheet("Progress")
            break
        except Exception as e:
            print(
                f"load_to_seen_data: Error in get_worksheet. Retry after {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
            delay *= 2
    else:
        raise Exception("load_to_seen_data: Failed to get the Progress sheet after multiple attempts.")
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "RowNum": 14}, "O2")
    progress = ph.load_progress()
    link_list = extract(link_sheet)
    if progress["progress"] != "finished":
        detail_sheet.update([["Running Scrapping"]], "S1")
        while progress["RowNum"] < len(link_list) and timer(start, time.time()):
            progress["progress"] = "processing"
            current_link_dict = link_list[progress["RowNum"]]
            seen_data = load_to_seen_data()
            current_link = list(current_link_dict.keys())[0]
            driver.get(current_link)
            wait_for_page_load(driver)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait_for_page_load(driver)
            print(f"current page: row {progress['RowNum']}")
            name = find_element(driver,
                                ".//lightning-layout-item[contains(@class, 'summary-view-responsive-style practitioner-name-style')]")
            if name != "N/A":
                name = name.text
            category = find_element(driver, "//c-practitioner-detail//p[@class='sub-header-text-style']")
            if category != "N/A":
                category = category.text
            details = find_elements(driver,
                                    ".//lightning-layout-item[contains(@class, 'detail-value-responsive-style')]")
            if details == "N/A":
                progress["RowNum"] += 1
                print(f"Current row: {progress["RowNum"]}, Failed to find details. Processing to next row.")
                continue
            fields = [
                ("address", True),
                ("contact", False),
                ("limitations", True),
                ("conditions", True),
                ("status", False),
                ("registration_number", False),
                ("commenced", False),
                ("anniversary", False),
                ("expires", False),
                ("date_scs", False),
                ("reason_scs", True),
                ("director_name", True),
            ]

            data = {}
            for i, (field, replace_newline) in enumerate(fields):
                if i < len(details):
                    text = details[i].text
                    data[field] = text.replace("\n", ", ") if replace_newline else text
                else:
                    data[field] = "N/A"

            address = data["address"]
            contact = data["contact"]
            limitations = data["limitations"]
            conditions = data["conditions"]
            status = data["status"]
            registration_number = data["registration_number"]
            commenced = data["commenced"]
            anniversary = data["anniversary"]
            expires = data["expires"]
            date_scs = data["date_scs"]
            reason_scs = data["reason_scs"]
            director_name = data["director_name"]

            partnership_element = find_element(driver,
                                               "//p[contains(@class, 'sub-header-text-style') and contains(text(), 'Partnership details')]/following-sibling::div//span")
            partnership = partnership_element.text if partnership_element != "N/A" else ""
            if address and address != "VAC":
                maps_url = f"https://www.google.com/maps/search/?api=1&query={quote(address)}"
                driver.get(maps_url)
                try:
                    WebDriverWait(driver, 30).until(lambda d: "@" in d.current_url)
                except TimeoutException:
                    print("lat/long not in url.")
                    va_lat, va_long = "No lat given", "No long given"
                else:
                    map_url = driver.current_url
                    pattern = r"@(-?\d+\.\d+),(-?\d+\.\d+)"
                    match = re.search(pattern, map_url)
                    if match:
                        va_lat, va_long = match.groups()
                    else:
                        va_lat = "No lat given"
                        va_long = "No long given"
            else:
                va_lat = "No lat given"
                va_long = "No long given"

            update = any(seen.get(name) == address for seen in seen_data)

            current_dict = link_list[progress["RowNum"]]
            postcode = list(current_dict.values())[0]

            if not update:
                updates = [name,
                           category,
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
                           va_long,
                           postcode
                           ]
                append_row_with_retry(detail_sheet, updates)
            else:
                update_category(name, address, category)
            progress["RowNum"] += 20
            ph.save_progress(progress)

        if not progress["RowNum"] < len(link_list):
            progress["progress"] = "finished"
        ph.save_progress(progress)
        driver.quit()
        print("Saved every data into the Google Sheet successfully.")
    else:
        print("Finished already")


if __name__ == "__main__":
    main()
