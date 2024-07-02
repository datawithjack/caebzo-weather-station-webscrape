import requests
from bs4 import BeautifulSoup
import pandas as pd

URL = 'https://cabezo.bergfex.at/'
response = requests.get(URL)

# Explicitly set the encoding to UTF-8
response.encoding = 'utf-8'

soup = BeautifulSoup(response.content, 'lxml')

# CSS selectors based on the provided XPaths
selectors = {
    'WindSpeedTime': 'body > header > v:nth-child(9) > span',
    'WindSpeed': 'html > body > header > v:nth-of-type(1) > span',
    'WindGust': 'body > header > v:nth-child(5)',
    'Temperature': 'body > header > v.small.mobile-hidden'
}

data = {}
for key, selector in selectors.items():
    element = soup.select_one(selector)
    text = element.text.strip() if element else 'Not Found'
    data[key] = text

# Convert the dictionary to a DataFrame
df = pd.DataFrame([data])
print(df)  # check output

# Clean up the Temperature field
df['Temperature'] = df['Temperature'].str.replace('Â', '').str.replace('°C', '').astype(float)

# Write the DataFrame to a CSV file with UTF-8 encoding
csv_file_path = 'wind_data.csv'
df.to_csv(csv_file_path, index=False, encoding='utf-8')
print(f'Data successfully written to {csv_file_path}')
