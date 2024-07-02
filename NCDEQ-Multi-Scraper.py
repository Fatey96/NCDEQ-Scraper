import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def scrape_and_save(url):
    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the main tag, then the content section, and the first div
    main_content = soup.find('main')
    content_section = main_content.find('section', id='content')
    first_div = content_section.find('div')

    # Find the table with id "wellcons"
    table = first_div.find('table', id='wellcons')

    # Initialize lists to store the data
    column1 = []
    column2 = []

    # Iterate through each row in the table
    for row in table.find_all('tr')[1:]:  # Skip the header row
        cols = row.find_all('td')
        if len(cols) == 2:
            column1.append(cols[0].text.strip())
            column2.append(cols[1].text.strip())

    # Get the well name (3rd row, 2nd column)
    well_name = column2[2] if len(column2) > 2 else "Unknown_Well"

    # Clean the well name to make it suitable for a filename
    well_name = re.sub(r'[^\w\-_\. ]', '_', well_name)

    # Create a DataFrame
    df = pd.DataFrame({
        'Field': column1,
        'Data': column2
    })

    # Save the DataFrame as a CSV file
    filename = f"{well_name}.csv"
    df.to_csv(filename, index=False)
    print(f"Data for {well_name} has been scraped and saved to '{filename}'")

# List of URLs to scrape
urls = [
    "https://www.ncwater.org/?page=736&id=U**40Y1&inactive=n&countyname=CUMBERLAND&aquifer=&station=&search=&net=&tl=1&jmp=",
    # Add more URLs here
]

# Scrape data from each URL
for url in urls:
    scrape_and_save(url)