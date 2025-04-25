import asyncio
import csv
from playwright.async_api import async_playwright
import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

INPUT_CSV = "well_documents.csv"
OUTPUT_CSV = "extracted_well_data-2022.csv"
BASE_URL = "https://edocs.deq.nc.gov/"

# List of counties to include
COUNTIES_TO_SCRAPE = ['Hoke', 'Robeson', 'Bladen', 'Sampson', 'Cumberland']

# Specify the output columns in the desired order
OUTPUT_COLUMNS = [
    "Well ID", "well types", "year of completion", "county", "Lat", "Long", 
    "Ground elevation ft", "well depth ft", "static water level below ground ft", 
    "yield gpm", "borehole diameter, inches", "screen depth ft", "aquifer name", 
    "number of clay layers above the screened zone", "material type in screened zone", "PFAS level"
]

async def scrape_and_parse_page(page, url):
    try:
        await page.goto(url, wait_until="networkidle")
        content = await page.content()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        metadata_table = soup.find('table', {'id': 'metadataTable'})
        if not metadata_table:
            logging.error(f"Metadata table not found in {url}")
            return None
        
        data = {}
        for row in metadata_table.find_all('tr', class_='fieldPane'):
            field = row.find('td', class_='ng-star-inserted')
            if field:
                field_name = field.div.text.strip()
                value_div = row.find_all('td', class_='ng-star-inserted')[1].div
                if value_div:
                    field_value = value_div.text.strip()
                    data[field_name] = field_value
        
        logging.info(f"Successfully scraped and parsed: {url}")
        return data
    except Exception as e:
        logging.error(f"Error scraping {url}: {e}")
        return None

def map_data_to_output_columns(scraped_data, original_row):
    mapped_data = {col: "" for col in OUTPUT_COLUMNS}
    
    # Map the data from the original CSV row and scraped data to the output columns
    mapped_data.update({
        "Well ID": original_row.get("ID #", ""),
        "well types": scraped_data.get("Well Type", ""),
        "year of completion": scraped_data.get("Date Well Completed", "")[:4] if scraped_data.get("Date Well Completed") else "",
        "county": original_row.get("County", ""),
        "Lat": scraped_data.get("Latitude", ""),
        "Long": scraped_data.get("Longitude", ""),
        "Ground elevation ft": scraped_data.get("Ground Elevation", "").split()[0] if scraped_data.get("Ground Elevation") else "",
        "well depth ft": scraped_data.get("Total Depth", "").split()[0] if scraped_data.get("Total Depth") else "",
        "static water level below ground ft": scraped_data.get("Static Water Level", "").split()[0] if scraped_data.get("Static Water Level") else "",
        "yield gpm": scraped_data.get("Yield", "").split()[0] if scraped_data.get("Yield") else "",
        "borehole diameter, inches": scraped_data.get("Borehole Diameter", "").split()[0] if scraped_data.get("Borehole Diameter") else "",
        "screen depth ft": scraped_data.get("Screen Depth", ""),
        "aquifer name": scraped_data.get("Aquifer", ""),
        "number of clay layers above the screened zone": "",  # Do Manually
        "material type in screened zone": scraped_data.get("Screen Material", ""),
        "PFAS level": ""  # Get from other document
    })
    
    return mapped_data

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context()
        page = await context.new_page()

        all_data = []

        with open(INPUT_CSV, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'County' in row and row['County'] in COUNTIES_TO_SCRAPE:
                    if 'Link' in row:
                        url = urljoin(BASE_URL, row['Link'].lstrip('/'))
                        scraped_data = await scrape_and_parse_page(page, url)
                        if scraped_data:
                            mapped_data = map_data_to_output_columns(scraped_data, row)
                            all_data.append(mapped_data)
                        await asyncio.sleep(1)  # Be nice to the server

        await browser.close()

        # Save all data to CSV
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader()
            for data in all_data:
                writer.writerow(data)

        logging.info(f"Scraped and saved data for {len(all_data)} documents to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())