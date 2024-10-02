import requests
import os

download_columns = "transit_timestamp, station_complex_id, sum(ridership) as ridership, sum(transfers) as transfers"
download_limit = 10**7  # 10 million records
hourly_ridership_csv_path = f"""https://data.ny.gov/resource/wujg-7c2s.csv?\
$limit={download_limit}&\
$select={download_columns}&\
$group=transit_timestamp, station_complex_id&\
$where=transit_mode='subway'
"""

data_folder = "../../data"
file_name = "ny_mta_hourly_ridership.csv"

file_path = os.path.join(data_folder, file_name)

response = requests.get(hourly_ridership_csv_path, stream=True)

os.makedirs(data_folder, exist_ok=True)
if response.status_code == 200:
    with open(file_path, "wb") as file:
        for data in response.iter_content(chunk_size=65536):
            file.write(data)
    print(f"Data downloaded and saved as '{file_path}'")
else:
    print(f"Error: Received status code {response.status_code}")
