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

API_URL = "https://intranet.cytrion.com/trading-api"

if __name__ == "__main__":
    report_url = f"{API_URL}/get-report2"
    resp = requests.get(report_url)
    # print(resp.status_code)
    # print(resp.ok)
    response = resp.json()
    data = response.get('data')
    data_date = response.get('date')
    data_date = datetime.strptime(data_date, '%Y-%m-%d')
    file_name = f"recommendations-{data_date.strftime('%d%b%Y').upper()}.csv"

    file_path = directory_path / file_name
    # path = path if isinstance(path, Path) else Path(path)
    with open(file_path, "w", newline='') as f:
        f_csv = csv.writer(f)
        f_csv.writerow([
            "Symbol",
            'Lot Size',
            'Future OI',
            'Change (Future OI)',
            '# of lots change (Future OI)',
            '% Change in Future_OI',
            'PE OI',
            'Change (PE OI)',
            '# of lots change (PE OI)',
            '% Change (PE OI)',
            'CE OI',
            'Change (CE OI)',
            '# of lots change (CE OI)',
            '% Change (CE OI)',
            'VWAP',
            'CLOSE PRICE',
            'Change CE / PE Ratio (OI)',
            'Delivery %',
            '% Change in Price',
            '% Change in Price From Low',
            '% Change in Price From High',
            'Decision (Based on Options',
            'Decision (Based on Futures',
            'Decision (Based on Price']
        )
        for x in data:
            f_csv.writerow(
                [
                    x['Symbol'],
                    x['Lot Size'],
                    x["Future OI"],
                    x["Change (Future OI)"],
                    x["# of lots change (Future OI)"],
                    x["% Change in Future_OI"],
                    x["PE OI"],
                    x["Change (PE OI)"],
                    x["# of lots change (PE OI)"],
                    x["% Change (PE OI)"],
                    x["CE OI"],
                    x["Change (CE OI)"],
                    x["# of lots change (CE OI)"],
                    x["% Change (CE OI)"],
                    x["VWAP"],
                    x["CLOSE PRICE"],
                    x["Change CE / PE Ratio (OI)"],
                    x["Delivery %"],
                    x["% Change in Price"],
                    x["% Change in Price From Low"],
                    x["% Change in Price From High"],
                    x["Decision (Based on Options"],
                    x["Decision (Based on Futures"],
                    x["Decision (Based on Price"]
                ]
            )



# from datetime import datetime
# from pathlib import Path
# import requests
# import csv

# def synthetic_generator_data_report(data_date=None, upload_to_remote_server=False):
#     """
#     Fetches data from the trading API and generates a CSV report.
#     """
#     try:
#         # Define directory path
#         directory_path = Path('./Output').resolve()
#         if not directory_path.exists():
#             directory_path.mkdir(parents=True)
#             print(f"Directory '{directory_path}' created.")

#         # Define API details
#         API_URL = "https://intranet.cytrion.com/trading-api"
#         report_url = f"{API_URL}/get-report2"
#         print(f"Fetching data from API: {report_url}")

#         # Make API request
#         resp = requests.get(report_url)
#         if resp.status_code != 200:
#             print(f"Error: Failed to fetch data. HTTP Status Code: {resp.status_code}")
#             return

#         response = resp.json()
#         print("API Response received.")

#         # Extract data and date from response
#         data = response.get('data', [])
#         data_date = response.get('date')
#         if not data or not data_date:
#             print("Error: No data or date in the API response.")
#             return

#         data_date = datetime.strptime(data_date, '%Y-%m-%d')
#         file_name = f"recommendations-{data_date.strftime('%d%b%Y').upper()}.csv"
#         file_path = directory_path / file_name
#         print(f"Data will be saved to: {file_path}")

#         # Write data to CSV
#         with open(file_path, "w", newline='') as f:
#             f_csv = csv.writer(f)
#             f_csv.writerow([
#                 "Symbol", "Lot Size", "Future OI", "Change (Future OI)", "# of lots change (Future OI)",
#                 "% Change in Future_OI", "PE OI", "Change (PE OI)", "# of lots change (PE OI)",
#                 "% Change (PE OI)", "CE OI", "Change (CE OI)", "# of lots change (CE OI)",
#                 "% Change (CE OI)", "VWAP", "CLOSE PRICE", "Change CE / PE Ratio (OI)",
#                 "Delivery %", "% Change in Price", "% Change in Price From Low",
#                 "% Change in Price From High", "Decision (Based on Options",
#                 "Decision (Based on Futures", "Decision (Based on Price"
#             ])

#             for x in data:
#                 f_csv.writerow([
#                     x.get('Symbol', ''),
#                     x.get('Lot Size', ''),
#                     x.get("Future OI", ''),
#                     x.get("Change (Future OI)", ''),
#                     x.get("# of lots change (Future OI)", ''),
#                     x.get("% Change in Future_OI", ''),
#                     x.get("PE OI", ''),
#                     x.get("Change (PE OI)", ''),
#                     x.get("# of lots change (PE OI)", ''),
#                     x.get("% Change (PE OI)", ''),
#                     x.get("CE OI", ''),
#                     x.get("Change (CE OI)", ''),
#                     x.get("# of lots change (CE OI)", ''),
#                     x.get("% Change (CE OI)", ''),
#                     x.get("VWAP", ''),
#                     x.get("CLOSE PRICE", ''),
#                     x.get("Change CE / PE Ratio (OI)", ''),
#                     x.get("Delivery %", ''),
#                     x.get("% Change in Price", ''),
#                     x.get("% Change in Price From Low", ''),
#                     x.get("% Change in Price From High", ''),
#                     x.get("Decision (Based on Options", ''),
#                     x.get("Decision (Based on Futures", ''),
#                     x.get("Decision (Based on Price", '')
#                 ])
#         print(f"Data successfully written to {file_path}")
    
#     except Exception as e:
#         print(f"An error occurred: {e}")

# # Call the function for testing
# if __name__ == "__main__":
#     synthetic_generator_data_report()
