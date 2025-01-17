from datetime import datetime
from pathlib import Path
import requests
import csv

directory_path = Path('./Output')

# Check if the directory exists
if not directory_path.exists():
    # Create the directory if it doesn't exist
    directory_path.mkdir(parents=True)

# API_URL = "https://intranet.cytrion.com/trading-api"
API_URL = "http://127.0.0.1:9009"

if __name__ == "__main__":
    report_url = f"{API_URL}/get-report7"
    resp = requests.get(report_url)
    
    if resp.ok:
        response = resp.json()
        data = response.get('data')
        data_date = response.get('date')
        data_date = datetime.strptime(data_date, '%Y-%m-%d')
        file_name = f"daily_high_low_range_report-{data_date.strftime('%d%b%Y').upper()}.csv"

        file_path = directory_path / file_name
        
        with open(file_path, "w", newline='') as f:
            f_csv = csv.writer(f)
            f_csv.writerow([
                'TICKER',
                'NSE CODE',
                'GOOGLE CODE',
                'LAST PRICE',
                'ATM STRIKE',
                'ATM CE VALUE',
                'ATM PE VALUE',
                'UPPER BAND CONS',
                'LOWER BAND CONS',
                'COMBINED PREMIUM',
                '% PREMIUM']
            )
            
            for x in data:
                
                # combined_premium = round(x.get("ATM CE VALUE", 0) + x.get("ATM PE VALUE", 0) , 2)
                
                combined_premium = round((x.get("ATM CE VALUE") or 0) + (x.get("ATM PE VALUE") or 0),2)
                
                last_price = x.get("LAST PRICE", 0)
                # percent_premium = (combined_premium / last_price)
                # percent_premium = round((combined_premium / last_price) , 2)
                
                percent_premium = f"{round((combined_premium / last_price) *100, 2)}%"
                
                
                f_csv.writerow([
                    x['TICKER'],
                    x['NSE CODE'],
                    x['GOOGLE CODE'],
                    x.get("LAST PRICE", 0),
                    x.get("ATM STRIKE", 0),
                    x.get("ATM CE VALUE", 0),
                    x.get("ATM PE VALUE", 0),
                    x.get("UPPER BAND CONS", 0),
                    x.get("LOWER BAND CONS", 0),
                    combined_premium,
                    percent_premium
                ])
