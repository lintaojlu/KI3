import requests
from bs4 import BeautifulSoup
import pandas as pd

# Send an HTTP request to the Wikipedia page
url = 'https://en.wikipedia.org/wiki/Lists_of_cities_by_country'
response = requests.get(url)

# Parse the HTML content of the page
soup = BeautifulSoup(response.content, 'html.parser')

# Find all links to individual country pages
country_links = soup.select('div.div-col.columns.column-width ul li a')
print(country_links)

# Loop through each country link
for link in country_links:
    # Extract the country name from the link text
    country_name = link.text.strip()

    # Build the URL for the country page
    country_url = 'https://en.wikipedia.org' + link['href']

    # Send an HTTP request to the country page
    response = requests.get(country_url)

    # Parse the HTML content of the country page
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table containing the list of cities
    table = soup.find('table', {'class': 'wikitable sortable'})

    # Read the table into a DataFrame object
    df = pd.read_html(str(table))[0]

    # Save the DataFrame object to a CSV file
    df.to_csv(country_name + '_cities.csv', index=False)
