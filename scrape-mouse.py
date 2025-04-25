from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
import pyautogui
import time
import csv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

driver = setup_driver()

url = "https://edocs.deq.nc.gov/WaterResources/Browse.aspx?id=2148507&dbid=0&repo=WaterResources"

def wait_for_element(by, value, timeout=20):
    try:
        return WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
    except TimeoutException:
        logging.error(f"Timeout waiting for element: {by}={value}")
        return None

def scroll_and_extract():
    documents = set()
    last_count = 0
    no_new_docs_count = 0
    max_attempts = 1000

    left_component = wait_for_element(By.CSS_SELECTOR, "div.left-component")
    if not left_component:
        logging.error("Could not find left component")
        return list(documents)

    actions = ActionChains(driver)
    actions.move_to_element(left_component).perform()

    for attempt in range(max_attempts):
        # Extract current visible rows
        rows = driver.find_elements(By.CSS_SELECTOR, "tr.ui-widget-content")
        new_docs = set(extract_row_data(row) for row in rows if row.is_displayed())
        documents.update(new_docs)
        
        current_count = len(documents)
        if attempt % 10 == 0:  # Log every 10 attempts to reduce console output
            logging.info(f"Attempt {attempt + 1}: Extracted {current_count} documents")
        
        if current_count == last_count:
            no_new_docs_count += 1
            if no_new_docs_count >= 10:
                logging.info("No new documents found after multiple attempts. Ending extraction.")
                break
        else:
            no_new_docs_count = 0
        
        last_count = current_count

        # Scroll using mouse wheel
        pyautogui.scroll(-600)  # Increased scroll length
        
        # Short wait after scrolling
        time.sleep(0.5)  # Reduced wait time
    
    return list(documents)

def extract_row_data(row):
    try:
        columns = row.find_elements(By.TAG_NAME, "td")
        name_element = columns[1].find_element(By.CSS_SELECTOR, "span.EntryNameColumn a")
        name = name_element.text.strip()
        link = name_element.get_attribute('href')
        return (name, link) + tuple(col.text.strip() for col in columns[2:])
    except (NoSuchElementException, StaleElementReferenceException):
        return None

def main():
    try:
        driver.get(url)
        logging.info("Navigated to the Laserfiche WebLink page")

        # Wait for the page to load completely
        time.sleep(5)  # Reduced initial wait time

        # Move mouse to the left component
        left_component = wait_for_element(By.CSS_SELECTOR, "div.left-component")
        if left_component:
            actions = ActionChains(driver)
            actions.move_to_element(left_component).perform()
            logging.info("Moved mouse to left component")
        
        # Give user time to manually position the mouse if needed
        print("Please ensure the mouse is over the left component. Script will continue in 3 seconds...")
        time.sleep(3)

        documents = scroll_and_extract()

        with open('well_documents.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Document Name', 'Link', 'Current Status', 'ID #', 'Version', 'Facility/Project Name', 'County', 'Document Date', 'Document Types', 'Page count', 'Linked'])
            writer.writerows(documents)

        logging.info(f"Extracted {len(documents)} documents and saved to well_documents.csv")

    except Exception as e:
        logging.exception(f"An error occurred: {e}")

    finally:
        driver.quit()
        logging.info("Browser closed")

if __name__ == "__main__":
    main()