from datetime import datetime
from pathlib import Path
import requests
import csv

# Define the directory path
directory_path = Path('./Output')

# Check if the directory exists
if not directory_path.exists():
    # Create the directory if it doesn't exist
    directory_path.mkdir(parents=True)
    # print(f"Directory '{directory_path}' created.")

# API_URL = "https://intranet.cytrion.com/trading-api"
API_URL = "http://127.0.0.1:9009"

if __name__ == "__main__":
    report_url = f"{API_URL}/get-report3"
    resp = requests.get(report_url)
    # print(resp.status_code)
    # print(resp.ok)
    response = resp.json()
    data = response.get('data')
    data_date = response.get('date')
    data_date = datetime.strptime(data_date, '%Y-%m-%d')
    file_name = f"initialSeriesStockOIInc-{data_date.strftime('%d%b%Y').upper()}.csv"

    file_path = directory_path / file_name
    # path = path if isinstance(path, Path) else Path(path)
    with open(file_path, "w", newline='') as f:
        f_csv = csv.writer(f)
        keys = list(data[0].keys())
        f_csv.writerow(keys)
        for x in data:
            f_csv.writerow(
                [
                    x[keys[0]],
                    x[keys[1]],
                    x[keys[2]],
                    x[keys[3]],
                    x[keys[4]],
                    x[keys[5]],
                    x[keys[6]],
                    x[keys[7]],
                    x[keys[8]],
                    x[keys[9]],
                    x[keys[10]],
                    x[keys[11]],
                    x[keys[12]],
                    x[keys[13]],
                    x[keys[14]],
                    x[keys[15]],

                ]
            )
