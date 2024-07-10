import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime

# Replace 'your_url' with the actual URL
url = 'https://cabezo.bergfex.at/'
response = requests.get(url)

if response.status_code == 200:
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table by its ID or class (update this if needed)
    table = soup.find('table', id='table')  # or use class name if applicable, e.g., soup.find('table', class_='table-class')

    if table:
        # Extract table rows
        rows = table.find_all('tr')

        if rows:
            # Define the column names based on the header row
            columns = ['time', 'wind_speed_kts', 'wind_bft', 'wind_kmh', 'gusts_speed_kts', 'gusts_bft', 'gusts_kmh', 'direction', 'temperature']

            # Initialize a list to store the data
            data = []

            # Iterate over the rows and extract the data
            for row in rows[1:]:  # Skip the header row
                cells = row.find_all('td')
                if len(cells) > 0:
                    print(f"Row content: {[cell.text.strip() for cell in cells]}")  # Debugging statement

                    time = cells[0].text.strip() if len(cells) > 0 else None

                    wind_text = cells[1].text.strip() if len(cells) > 1 else None
                    gusts_text = cells[3].text.strip() if len(cells) > 3 else None
                    direction = cells[4].text.strip() if len(cells) > 4 else None
                    temperature = cells[5].text.strip() if len(cells) > 5 else None

                    # Initialize variables with default values
                    wind_speed_kts = wind_bft = wind_kmh = gusts_speed_kts = gusts_bft = gusts_kmh = None

                    # Split wind_text safely
                    if wind_text:
                        wind_parts = wind_text.split()
                        if len(wind_parts) > 0:
                            wind_speed_kts = wind_parts[0]
                        if len(wind_parts) > 2:
                            wind_kmh_part = wind_parts[-1].strip('()').split(',')
                            if len(wind_kmh_part) > 1:
                                wind_kmh = wind_kmh_part[1].strip().split()[0]

                        wind_bft = cells[1].get('class', [''])[0] if cells[1].get('class', ['']) else ''

                    if gusts_text:
                        gusts_parts = gusts_text.split()
                        if len(gusts_parts) > 0:
                            gusts_speed_kts = gusts_parts[0]
                        if len(gusts_parts) > 2:
                            gusts_kmh_part = gusts_parts[-1].strip('()').split(',')
                            if len(gusts_kmh_part) > 1:
                                gusts_kmh = gusts_kmh_part[1].strip().split()[0]

                        gusts_bft = cells[2].get('class', [''])[0] if cells[2].get('class', ['']) else ''

                    # Clean temperature field
                    if temperature:
                        temperature = re.sub(r'[^\d.]+', '', temperature)

                    # Append the row data to the list
                    data.append([time, wind_speed_kts, wind_bft, wind_kmh, gusts_speed_kts, gusts_bft, gusts_kmh, direction, temperature])

            # Convert the data into a Pandas DataFrame
            df = pd.DataFrame(data, columns=columns)

            # Clean the data
            current_date = datetime.now().strftime('%d/%m/%Y')
            df['time'] = df['time'].apply(lambda x: f"{current_date} {x}")

            # Drop columns with '_kmh' in the name
            df = df.drop(columns=[col for col in df.columns if col.endswith('_kmh')])

            # Create new columns
            df['wind_speed_kts_text'] = df['wind_speed_kts'].apply(lambda x: f"{x} kts" if pd.notna(x) else None)
            df['gusts_speed_kts_text'] = df['gusts_speed_kts'].apply(lambda x: f"{x} kts" if pd.notna(x) else None)

            # Drop the last row
            df = df.iloc[:-1]

            # Export the DataFrame to a CSV file
            csv_file_path = 'weather_data.csv'
            df.to_csv(csv_file_path, index=False)

            print(f"Data has been exported to {csv_file_path}")
        else:
            print("No rows found in the table.")
    else:
        print("Table not found on the page.")
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
