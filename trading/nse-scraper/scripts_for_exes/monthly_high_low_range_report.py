# from datetime import datetime
# from pathlib import Path
# import requests
# import csv

# # Define the directory path
# directory_path = Path('./Output')

# # Check if the directory exists
# if not directory_path.exists():
#     # Create the directory if it doesn't exist
#     directory_path.mkdir(parents=True)

# API_URL = "https://intranet.cytrion.com/trading-api"

# if __name__ == "__main__":
#     report_url = f"{API_URL}/get-report6"
#     resp = requests.get(report_url)
    
#     if resp.ok:
#         response = resp.json()
#         data = response.get('data')
#         data_date = response.get('date')
#         data_date = datetime.strptime(data_date, '%Y-%m-%d')
#         file_name = f"monthly_high_low_range_report-{data_date.strftime('%d%b%Y').upper()}.csv"

#         file_path = directory_path / file_name
        
#         with open(file_path, "w", newline='') as f:
#             f_csv = csv.writer(f)
#             f_csv.writerow([
#                 'TICKER',
#                 'NSE CODE',
#                 'GOOGLE CODE',
#                 'LAST PRICE',
#                 'ATM STRIKE',
#                 'ATM CE VALUE',
#                 'ATM PE VALUE',
#                 'UPPER BAND CONS',
#                 'LOWER BAND CONS',
#                 'COMBINED PREMIUM',
#                 '% PREMIUM']
#             )
            
#             for x in data:
                
#                 atm_ce_value = x.get("ATM CE VALUE", 0) 
#                 atm_pe_value = x.get("ATM PE VALUE", 0) 
#                 last_price = x.get("LAST PRICE", 0) 

                
#                 combined_premium = round(atm_ce_value + atm_pe_value, 2)

#                 percent_premium = round((combined_premium / last_price), 2) 
                
#                 f_csv.writerow([
#                     x['TICKER'],
#                     x['NSE CODE'],
#                     x['GOOGLE CODE'],
#                     last_price,
#                     x.get("ATM STRIKE", 0),
#                     atm_ce_value,
#                     atm_pe_value,
#                     x.get("UPPER BAND CONS", 0),
#                     x.get("LOWER BAND CONS", 0),
#                     combined_premium,
#                     percent_premium
#                 ])



# from datetime import datetime
# from pathlib import Path
# import requests
# import csv

# # Define the directory path
# directory_path = Path('./Output')

# # Check if the directory exists
# if not directory_path.exists():
#     # Create the directory if it doesn't exist
#     directory_path.mkdir(parents=True)

# API_URL = "https://intranet.cytrion.com/trading-api"

# if __name__ == "__main__":
#     report_url = f"{API_URL}/get-report6"
#     resp = requests.get(report_url)
    
#     if resp.ok:
#         response = resp.json()
#         data = response.get('data')
#         data_date = response.get('date')
#         data_date = datetime.strptime(data_date, '%Y-%m-%d')
#         file_name = f"monthly_high_low_range_report-{data_date.strftime('%d%b%Y').upper()}.csv"

#         file_path = directory_path / file_name
        
#         with open(file_path, "w", newline='') as f:
#             f_csv = csv.writer(f)
#             f_csv.writerow([
#                 'TICKER',
#                 'NSE CODE',
#                 'GOOGLE CODE',
#                 'LAST PRICE',
#                 'ATM STRIKE',
#                 'ATM CE VALUE',
#                 'ATM PE VALUE',
#                 'UPPER BAND CONS',
#                 'LOWER BAND CONS',
#                 'COMBINED PREMIUM',
#                 '% PREMIUM']
#             )
            
#             for x in data:
#                 # Handle None values by defaulting to 0
#                 atm_ce_value = x.get("ATM CE VALUE", 0) 
#                 atm_pe_value = x.get("ATM PE VALUE", 0) 
#                 last_price = x.get("LAST PRICE", 0) 

#                 combined_premium = round(atm_ce_value + atm_pe_value, 2)

#                 percent_premium = f"{round((combined_premium / last_price) * 100, 2)}%"
                
#                 f_csv.writerow([
#                     x['TICKER'],
#                     x['NSE CODE'],
#                     x['GOOGLE CODE'],
#                     last_price,
#                     x.get("ATM STRIKE", 0),
#                     atm_ce_value,
#                     atm_pe_value,
#                     x.get("UPPER BAND CONS", 0),
#                     x.get("LOWER BAND CONS", 0),
#                     combined_premium,
#                     percent_premium
#                 ])

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

API_URL = "https://intranet.cytrion.com/trading-api"

if __name__ == "__main__":
    report_url = f"{API_URL}/get-report6"
    resp = requests.get(report_url)
    
    if resp.ok:
        response = resp.json()
        data = response.get('data')
        data_date = response.get('date')
        data_date = datetime.strptime(data_date, '%Y-%m-%d')
        file_name = f"monthly_high_low_range_report-{data_date.strftime('%d%b%Y').upper()}.csv"

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
                # Ensure no None values
                atm_ce_value = x.get("ATM CE VALUE", 0) or 0
                atm_pe_value = x.get("ATM PE VALUE", 0) or 0
                last_price = x.get("LAST PRICE", 0) or 0

                # Calculate combined premium
                combined_premium = round(float(atm_ce_value) + float(atm_pe_value), 2)

                # Calculate percent premium, avoid division by zero
                percent_premium = f"{round((combined_premium / last_price) * 100, 2)}%" if last_price else "0.00%"
                
                f_csv.writerow([
                    x.get('TICKER', ''),
                    x.get('NSE CODE', ''),
                    x.get('GOOGLE CODE', ''),
                    last_price,
                    x.get("ATM STRIKE", 0),
                    atm_ce_value,
                    atm_pe_value,
                    x.get("UPPER BAND CONS", 0),
                    x.get("LOWER BAND CONS", 0),
                    combined_premium,
                    percent_premium
                ])
