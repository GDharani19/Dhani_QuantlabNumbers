
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
#     # print(f"Directory '{directory_path}' created.")

# API_URL = "https://intranet.cytrion.com/trading-api"

# if __name__ == "__main__":
#     report_url = f"{API_URL}"
#     resp = requests.get(report_url)
#     # print(resp.status_code)
#     # print(resp.ok)
#     response = resp.json()
#     data = response.get('data')
#     data_date = response.get('date')
#     data_date = datetime.strptime(data_date, '%Y-%m-%d')
#     file_name = f"recommendations-{data_date.strftime('%d%b%Y').upper()}.csv"

#     file_path = directory_path / file_name
#     # path = path if isinstance(path, Path) else Path(path)
#     with open(file_path, "w", newline='') as f:
#         f_csv = csv.writer(f)
#         f_csv.writerow([
#             "Symbol",
#             'Lot Size',
#             'Future OI',
#             'Change (Future OI)',
#             '# of lots change (Future OI)',
#             '% Change in Future_OI',
#             'PE OI',
#             'Change (PE OI)',
#             '# of lots change (PE OI)',
#             '% Change (PE OI)',
#             'CE OI',
#             'Change (CE OI)',
#             '# of lots change (CE OI)',
#             '% Change (CE OI)',
#             'VWAP',
#             'CLOSE PRICE',
#             'Change CE / PE Ratio (OI)',
#             'Delivery %',
#             '% Change in Price',
#             '% Change in Price From Low',
#             '% Change in Price From High',
#             'Decision (Based on Options',
#             'Decision (Based on Futures',
#             'Decision (Based on Price']
#         )
#         for x in data:
#             f_csv.writerow(
#                 [
#                     x['Symbol'],
#                     x['Lot Size'],
#                     x["Future OI"],
#                     x["Change (Future OI)"],
#                     x["# of lots change (Future OI)"],
#                     x["% Change in Future_OI"],
#                     x["PE OI"],
#                     x["Change (PE OI)"],
#                     x["# of lots change (PE OI)"],
#                     x["% Change (PE OI)"],
#                     x["CE OI"],
#                     x["Change (CE OI)"],
#                     x["# of lots change (CE OI)"],
#                     x["% Change (CE OI)"],
#                     x["VWAP"],
#                     x["CLOSE PRICE"],
#                     x["Change CE / PE Ratio (OI)"],
#                     x["Delivery %"],
#                     x["% Change in Price"],
#                     x["% Change in Price From Low"],
#                     x["% Change in Price From High"],
#                     x["Decision (Based on Options"],
#                     x["Decision (Based on Futures"],
#                     x["Decision (Based on Price"]
#                 ]
#             )


from datetime import datetime
from pathlib import Path
import requests
import csv

def synthetic_generator_data_report(data_date=None, upload_to_remote_server=False):
    """
    Fetches data from the trading API and generates a CSV report.
    """
    try:
        # Define directory path
        directory_path = Path('./Output').resolve()
        if not directory_path.exists():
            directory_path.mkdir(parents=True)
            print(f"Directory '{directory_path}' created.")

        # Define API details
        API_URL = "http://127.0.0.1:9009"
        report_url = f"{API_URL}/get-report2"
        print(f"Fetching data from API: {report_url}")

        # Make API request
        resp = requests.get(report_url)
        if resp.status_code != 200:
            print(f"Error: Failed to fetch data. HTTP Status Code: {resp.status_code}")
            return

        response = resp.json()
        print("API Response received.")

        # Extract data and date from response
        data = response.get('data', [])
        data_date = response.get('date')
        if not data or not data_date:
            print("Error: No data or date in the API response.")
            return

        data_date = datetime.strptime(data_date, '%Y-%m-%d')
        file_name = f"recommendations-{data_date.strftime('%d%b%Y').upper()}.csv"
        file_path = directory_path / file_name
        print(f"Data will be saved to: {file_path}")

        # Write data to CSV
        with open(file_path, "w", newline='') as f:
            f_csv = csv.writer(f)
            f_csv.writerow([
                "Symbol", "Lot Size", "Future OI", "Change (Future OI)", "# of lots change (Future OI)",
                "% Change in Future_OI", "PE OI", "Change (PE OI)", "# of lots change (PE OI)",
                "% Change (PE OI)", "CE OI", "Change (CE OI)", "# of lots change (CE OI)",
                "% Change (CE OI)", "VWAP", "CLOSE PRICE", "Change CE / PE Ratio (OI)",
                "Delivery %", "% Change in Price", "% Change in Price From Low",
                "% Change in Price From High", "Decision (Based on Options",
                "Decision (Based on Futures", "Decision (Based on Price"
            ])

            for x in data:
                f_csv.writerow([
                    x.get('Symbol', ''),
                    x.get('Lot Size', ''),
                    x.get("Future OI", ''),
                    x.get("Change (Future OI)", ''),
                    x.get("# of lots change (Future OI)", ''),
                    x.get("% Change in Future_OI", ''),
                    x.get("PE OI", ''),
                    x.get("Change (PE OI)", ''),
                    x.get("# of lots change (PE OI)", ''),
                    x.get("% Change (PE OI)", ''),
                    x.get("CE OI", ''),
                    x.get("Change (CE OI)", ''),
                    x.get("# of lots change (CE OI)", ''),
                    x.get("% Change (CE OI)", ''),
                    x.get("VWAP", ''),
                    x.get("CLOSE PRICE", ''),
                    x.get("Change CE / PE Ratio (OI)", ''),
                    x.get("Delivery %", ''),
                    x.get("% Change in Price", ''),
                    x.get("% Change in Price From Low", ''),
                    x.get("% Change in Price From High", ''),
                    x.get("Decision (Based on Options", ''),
                    x.get("Decision (Based on Futures", ''),
                    x.get("Decision (Based on Price", '')
                ])
        print(f"Data successfully written to {file_path}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function for testing
if __name__ == "__main__":
    synthetic_generator_data_report()


# from datetime import datetime
# from pathlib import Path
# import requests
# import csv

# def synthetic_generator_data_report(data_date=None, upload_to_remote_server=False):
#     """
#     Fetches today's data from the trading API and generates a CSV report.
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
#         api_date = response.get('date')
#         if not data or not api_date:
#             print("Error: No data or date in the API response.")
#             return

#         # Use today's date
#         today_date = datetime.today().date()
#         api_date_obj = datetime.strptime(api_date, '%Y-%m-%d').date()

#         if api_date_obj != today_date:
#             print(f"Warning: The API data is not for today ({today_date}). Received date: {api_date_obj}")
#             return

#         file_name = f"recommendations-{today_date.strftime('%d%b%Y').upper()}.csv"
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
#         today_date = datetime.today().strftime('%Y-%m-%d')
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
#         api_date_str = response.get('date')
#         if not data or not api_date_str:
#             print("Error: No data or date in the API response.")
#             return

#         # Parse the API date
#         api_date_obj = datetime.strptime(api_date_str, '%Y-%m-%d')
#         if api_date_str != today_date:
#             print(f"Warning: The API data is not for today ({today_date}). Received date: {api_date_str}")
#             # Attempt to fetch today's data if supported by the API
#             print(f"Retrying with explicit date parameter: {today_date}...")
#             resp = requests.get(f"{report_url}?date={today_date}")
#             if resp.status_code == 200:
#                 response = resp.json()
#                 data = response.get('data', [])
#                 api_date_str = response.get('date')
#                 api_date_obj = datetime.strptime(api_date_str, '%Y-%m-%d')
#                 if api_date_str == today_date:
#                     print("Successfully fetched today's data.")
#                 else:
#                     print(f"Today's data is still unavailable. Using data for {api_date_str}.")

#         # Prepare file path
#         file_name = f"recommendations-{api_date_obj.strftime('%d%b%Y').upper()}.csv"
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
#         print("API Full Response:", response)  # Debugging API response

#         # Extract data and date from response
#         data = response.get('data', [])
#         data_date = response.get('date')
#         print("Extracted Data from API:", data)  # Debugging extracted data
#         print("Data Date from API:", data_date)

#         if not data or not data_date:
#             print("Error: No data or date in the API response.")
#             return

#         # Convert date to datetime for comparison
#         api_data_date = datetime.strptime(data_date, '%Y-%m-%d')
#         today = datetime.now()
#         if api_data_date.date() != today.date():
#             print(f"Warning: The API data is not for today ({today.date()}). Received date: {api_data_date.date()}")
#             # Retry with today's date
#             print(f"Retrying with explicit date parameter: {today.strftime('%Y-%m-%d')}...")
#             resp = requests.get(report_url, params={'date': today.strftime('%Y-%m-%d')})
#             if resp.status_code != 200:
#                 print(f"Error: Failed to fetch today's data. HTTP Status Code: {resp.status_code}")
#                 return
            
#             response = resp.json()
#             print("Retry API Full Response:", response)  # Debugging retry response
#             data = response.get('data', [])
#             data_date = response.get('date')
#             print("Retry Extracted Data:", data)
#             print("Retry Data Date:", data_date)
            
#             if not data or not data_date:
#                 print("Error: No data available for today.")
#                 return

#         # Save data to CSV
#         file_name = f"recommendations-{api_data_date.strftime('%d%b%Y').upper()}.csv"
#         file_path = directory_path / file_name
#         print(f"Data will be saved to: {file_path}")

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




from datetime import datetime
from pathlib import Path
import requests
import csv
import mysql.connector

def get_last_updated_date_from_db():
    """
    Fetches the last_updated_date from the testdb database.
    """
    try:
        # MySQL connection details
        db_config = {
            'host': 'localhost',  # Replace with your MySQL host
            'user': 'root',  # Replace with your MySQL username
            'password': 'root',  # Replace with your MySQL password
            'database': 'testdb'  # Replace with your database name
        }
        
        # Connect to the MySQL database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Query to fetch the last_updated_date from your table
        cursor.execute("SELECT last_updated_date FROM your_table_name ORDER BY last_updated_date DESC LIMIT 1;")
        result = cursor.fetchone()
        
        if result:
            # Convert result to a datetime object
            last_updated_date = result[0]
            return last_updated_date
        else:
            print("No last_updated_date found in the database.")
            return None
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

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
#         print("API Full Response:", response)  # Debugging API response

#         # Extract data and date from response
#         data = response.get('data', [])
#         data_date = response.get('date')
#         print("Extracted Data from API:", data)  # Debugging extracted data
#         print("Data Date from API:", data_date)

#         if not data or not data_date:
#             print("Error: No data or date in the API response.")
#             return

#         # Fetch the last updated date from the database
#         db_last_updated_date = get_last_updated_date_from_db()
#         if db_last_updated_date:
#             print(f"Last updated date from DB: {db_last_updated_date}")
#         else:
#             print("Using the date from the API as fallback.")
#             db_last_updated_date = data_date

#         # Convert date to datetime for comparison
#         api_data_date = datetime.strptime(db_last_updated_date, '%Y-%m-%d')
#         today = datetime.now()
#         if api_data_date.date() != today.date():
#             print(f"Warning: The API data is not for today ({today.date()}). Received date: {api_data_date.date()}")
#             # Retry with today's date
#             print(f"Retrying with explicit date parameter: {today.strftime('%Y-%m-%d')}...")
#             resp = requests.get(report_url, params={'date': today.strftime('%Y-%m-%d')})
#             if resp.status_code != 200:
#                 print(f"Error: Failed to fetch today's data. HTTP Status Code: {resp.status_code}")
#                 return
            
#             response = resp.json()
#             print("Retry API Full Response:", response)  # Debugging retry response
#             data = response.get('data', [])
#             data_date = response.get('date')
#             print("Retry Extracted Data:", data)
#             print("Retry Data Date:", data_date)
            
#             if not data or not data_date:
#                 print("Error: No data available for today.")
#                 return

#         # Save data to CSV
#         file_name = f"recommendations-{api_data_date.strftime('%d%b%Y').upper()}.csv"
#         file_path = directory_path / file_name
#         print(f"Data will be saved to: {file_path}")

#         with open(file_path, "w", newline='') as f:
#             f_csv = csv.writer(f)
#             f_csv.writerow([  # CSV headers
#                 "Symbol", "Lot Size", "Future OI", "Change (Future OI)", "# of lots change (Future OI)",
#                 "% Change in Future_OI", "PE OI", "Change (PE OI)", "# of lots change (PE OI)",
#                 "% Change (PE OI)", "CE OI", "Change (CE OI)", "# of lots change (CE OI)",
#                 "% Change (CE OI)", "VWAP", "CLOSE PRICE", "Change CE / PE Ratio (OI)",
#                 "Delivery %", "% Change in Price", "% Change in Price From Low",
#                 "% Change in Price From High", "Decision (Based on Options",
#                 "Decision (Based on Futures", "Decision (Based on Price"
#             ])

#             for x in data:
#                 f_csv.writerow([  # Write data to CSV
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




# import requests
# import csv
# from pathlib import Path
# from datetime import datetime

# # def get_last_updated_date_from_db():
# #     # This is a placeholder function for getting the last updated date from your DB.
# #     # For debugging purposes, returning a dummy value.
# #     return "2025-01-10"  # Replace with actual DB call logic

# def synthetic_generator_data_report(data_date=None, upload_to_remote_server=False):
#     """
#     Fetches data from the trading API and generates a CSV report.
#     """
#     try:
#         # Step 1: Define directory path
#         directory_path = Path('./Output').resolve()
#         print(f"Resolving directory path: {directory_path}")
        
#         # if not directory_path.exists():
#         #     directory_path.mkdir(parents=True)
#         #     print(f"Directory '{directory_path}' created.")
#         # else:
#         #     print(f"Directory '{directory_path}' already exists.")

#         # Step 2: Define API details
#         # API_URL = "https://intranet.cytrion.com/trading-api"
#         API_URL = "http://127.0.0.1:9009"
#         report_url = f"{API_URL}/get-report2"
#         # print(f"Fetching data from API: {report_url}")

#         # Step 3: Make API request
#         resp = requests.get(report_url)
#         if resp.status_code != 200:
#             print(f"Error: Failed to fetch data. HTTP Status Code: {resp.status_code}")
#             return

#         response = resp.json()
#         # print("API Full Response:", response)  # Debugging API response

#         # Step 4: Extract data and date from response
#         data = response.get('data', [])
#         data_date = response.get('date')
#         print("Extracted Data from API:", data)  # Debugging extracted data
#         print("Data Date from API:", data_date)

#         if not data or not data_date:
#             print("Error: No data or date in the API response.")
#             return

#         # Step 5: Fetch the last updated date from the database
#         db_last_updated_date = get_last_updated_date_from_db()
#         print(f"Last updated date from DB: {db_last_updated_date}")

#         # Step 6: Date comparison between API data and DB data
#         api_data_date = datetime.strptime(db_last_updated_date, '%Y-%m-%d')
#         today = datetime.now()
#         print(f"Today's date: {today.date()}")
        
#         if api_data_date.date() != today.date():
#             print(f"Warning: The API data is not for today ({today.date()}). Received date: {api_data_date.date()}")
#             print(f"Retrying with today's date: {today.strftime('%Y-%m-%d')}...")

#             # Step 7: Retry with today's date
#             resp = requests.get(report_url, params={'date': today.strftime('%Y-%m-%d')})
#             if resp.status_code != 200:
#                 print(f"Error: Failed to fetch today's data. HTTP Status Code: {resp.status_code}")
#                 return

#             response = resp.json()
#             print("Retry API Full Response:", response)  # Debugging retry response
#             data = response.get('data', [])
#             data_date = response.get('date')
#             # print("Retry Extracted Data:", data)
#             print("Retry Data Date:", data_date)

#             if not data or not data_date:
#                 print("Error: No data available for today.")
#                 return

#         # Step 8: Save data to CSV
#         file_name = f"recommendations-{api_data_date.strftime('%d%m%Y').upper()}.csv"
#         file_path = directory_path / file_name
#         print(f"Data will be saved to: {file_path}")

#         with open(file_path, "w", newline='') as f:
#             f_csv = csv.writer(f)
#             f_csv.writerow([  # CSV headers
#                 "Symbol", "Lot Size", "Future OI", "Change (Future OI)", "# of lots change (Future OI)",
#                 "% Change in Future_OI", "PE OI", "Change (PE OI)", "# of lots change (PE OI)",
#                 "% Change (PE OI)", "CE OI", "Change (CE OI)", "# of lots change (CE OI)",
#                 "% Change (CE OI)", "VWAP", "CLOSE PRICE", "Change CE / PE Ratio (OI)",
#                 "Delivery %", "% Change in Price", "% Change in Price From Low",
#                 "% Change in Price From High", "Decision (Based on Options",
#                 "Decision (Based on Futures", "Decision (Based on Price"
#             ])

#             # Step 9: Writing data to CSV
#             for x in data:
#                 f_csv.writerow([  # Write data to CSV
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



# import requests
# import csv
# from pathlib import Path
# from datetime import datetime

# def get_last_updated_date_from_db():
#     # Simulating database date retrieval (you should replace this with actual DB code)
#     return "2025-01-10"  # Example, should return the last updated date from the DB

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
#         # print(f"Fetching data from API: {report_url}")  # Commented out print

#         # Make API request
#         resp = requests.get(report_url)
#         if resp.status_code != 200:
#             print(f"Error: Failed to fetch data. HTTP Status Code: {resp.status_code}")
#             return

#         response = resp.json()
#         # print("API Full Response:", response)  # Commented out print

#         # Extract data and date from response
#         data = response.get('data', [])
#         data_date = response.get('date')
#         # print("Extracted Data from API:", data)  # Commented out print
#         # print("Data Date from API:", data_date)  # Commented out print

#         if not data or not data_date:
#             print("Error: No data or date in the API response.")
#             return

#         # Fetch the last updated date from the database
#         db_last_updated_date = get_last_updated_date_from_db()
#         if db_last_updated_date:
#             print(f"Last updated date from DB: {db_last_updated_date}")
#         else:
#             print("Using the date from the API as fallback.")
#             db_last_updated_date = data_date

#         # Convert date to datetime for comparison
#         api_data_date = datetime.strptime(db_last_updated_date, '%Y-%m-%d')
#         today = datetime.now()
#         if api_data_date.date() != today.date():
#             print(f"Warning: The API data is not for today ({today.date()}). Received date: {api_data_date.date()}")
#             # Retry with today's date
#             print(f"Retrying with explicit date parameter: {today.strftime('%Y-%m-%d')}...")  # Commented out print
#             resp = requests.get(report_url, params={'date': today.strftime('%Y-%m-%d')})
#             if resp.status_code != 200:
#                 print(f"Error: Failed to fetch today's data. HTTP Status Code: {resp.status_code}")
#                 return
            
#             response = resp.json()
#             # print("Retry API Full Response:", response)  # Commented out print
#             data = response.get('data', [])
#             data_date = response.get('date')
#             # print("Retry Extracted Data:", data)  # Commented out print
#             # print("Retry Data Date:", data_date)  # Commented out print
            
#             if not data or not data_date:
#                 print("Error: No data available for today.")
#                 return

#         # Save data to CSV
#         file_name = f"recommendations-{api_data_date.strftime('%d%b%Y').upper()}.csv"
#         file_path = directory_path / file_name
#         print(f"Data will be saved to: {file_path}")

#         with open(file_path, "w", newline='') as f:
#             f_csv = csv.writer(f)
#             f_csv.writerow([  # CSV headers
#                 "Symbol", "Lot Size", "Future OI", "Change (Future OI)", "# of lots change (Future OI)",
#                 "% Change in Future_OI", "PE OI", "Change (PE OI)", "# of lots change (PE OI)",
#                 "% Change (PE OI)", "CE OI", "Change (CE OI)", "# of lots change (CE OI)",
#                 "% Change (CE OI)", "VWAP", "CLOSE PRICE", "Change CE / PE Ratio (OI)",
#                 "Delivery %", "% Change in Price", "% Change in Price From Low",
#                 "% Change in Price From High", "Decision (Based on Options",
#                 "Decision (Based on Futures", "Decision (Based on Price"
#             ])

#             for x in data:
#                 f_csv.writerow([  # Write data to CSV
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



# import requests
# import csv
# from pathlib import Path
# from datetime import datetime

# def get_last_updated_date_from_db():
#     # Simulating database date retrieval (you should replace this with actual DB code)
#     return "2025-01-10"  # Example, should return the last updated date from the DB

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
#         print(f"Fetching data from API: {report_url}")  # Ensure this is correct

#         # Make API request
#         resp = requests.get(report_url)
#         if resp.status_code != 200:
#             print(f"Error: Failed to fetch data. HTTP Status Code: {resp.status_code}")
#             return

#         response = resp.json()
#         print("API Full Response:", response)  # Print API response to debug

#         # Extract data and date from response
#         data = response.get('data', [])
#         data_date = response.get('date')
#         print("Extracted Data from API:", data)  # Print the extracted data for debugging
#         print("Data Date from API:", data_date)  # Print the data date

#         if not data or not data_date:
#             print("Error: No data or date in the API response.")
#             return

#         # Fetch the last updated date from the database
#         db_last_updated_date = get_last_updated_date_from_db()
#         if db_last_updated_date:
#             print(f"Last updated date from DB: {db_last_updated_date}")
#         else:
#             print("Using the date from the API as fallback.")
#             db_last_updated_date = data_date

#         # Convert date to datetime for comparison
#         api_data_date = datetime.strptime(db_last_updated_date, '%Y-%m-%d')
#         today = datetime.now()
#         if api_data_date.date() != today.date():
#             print(f"Warning: The API data is not for today ({today.date()}). Received date: {api_data_date.date()}")
#             # Retry with today's date
#             print(f"Retrying with explicit date parameter: {today.strftime('%Y-%m-%d')}...")
#             resp = requests.get(report_url, params={'date': today.strftime('%Y-%m-%d')})
#             if resp.status_code != 200:
#                 print(f"Error: Failed to fetch today's data. HTTP Status Code: {resp.status_code}")
#                 return
            
#             response = resp.json()
#             data = response.get('data', [])
#             data_date = response.get('date')
#             print("Retry API Full Response:", response)  # Print the retry response for debugging
#             print("Retry Extracted Data:", data)  # Print the retry extracted data
#             print("Retry Data Date:", data_date)  # Print the retry data date
            
#             if not data or not data_date:
#                 print("Error: No data available for today.")
#                 return

#         # Save data to CSV
#         file_name = f"recommendations-{api_data_date.strftime('%d%b%Y').upper()}.csv"
#         file_path = directory_path / file_name
#         print(f"Data will be saved to: {file_path}")

#         with open(file_path, "w", newline='') as f:
#             f_csv = csv.writer(f)
#             f_csv.writerow([  # CSV headers
#                 "Symbol", "Lot Size", "Future OI", "Change (Future OI)", "# of lots change (Future OI)",
#                 "% Change in Future_OI", "PE OI", "Change (PE OI)", "# of lots change (PE OI)",
#                 "% Change (PE OI)", "CE OI", "Change (CE OI)", "# of lots change (CE OI)",
#                 "% Change (CE OI)", "VWAP", "CLOSE PRICE", "Change CE / PE Ratio (OI)",
#                 "Delivery %", "% Change in Price", "% Change in Price From Low",
#                 "% Change in Price From High", "Decision (Based on Options",
#                 "Decision (Based on Futures", "Decision (Based on Price"
#             ])

#             # Check if data is available before writing to CSV
#             if data:
#                 for x in data:
#                     f_csv.writerow([  # Write data to CSV
#                         x.get('Symbol', ''),
#                         x.get('Lot Size', ''),
#                         x.get("Future OI", ''),
#                         x.get("Change (Future OI)", ''),
#                         x.get("# of lots change (Future OI)", ''),
#                         x.get("% Change in Future_OI", ''),
#                         x.get("PE OI", ''),
#                         x.get("Change (PE OI)", ''),
#                         x.get("# of lots change (PE OI)", ''),
#                         x.get("% Change (PE OI)", ''),
#                         x.get("CE OI", ''),
#                         x.get("Change (CE OI)", ''),
#                         x.get("# of lots change (CE OI)", ''),
#                         x.get("% Change (CE OI)", ''),
#                         x.get("VWAP", ''),
#                         x.get("CLOSE PRICE", ''),
#                         x.get("Change CE / PE Ratio (OI)", ''),
#                         x.get("Delivery %", ''),
#                         x.get("% Change in Price", ''),
#                         x.get("% Change in Price From Low", ''),
#                         x.get("% Change in Price From High", ''),
#                         x.get("Decision (Based on Options", ''),
#                         x.get("Decision (Based on Futures", ''),
#                         x.get("Decision (Based on Price", '')
#                     ])
#                 print(f"Data successfully written to {file_path}")
#             else:
#                 print("Error: No data to write to CSV.")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# # Call the function for testing
# if __name__ == "__main__":
#     synthetic_generator_data_report()


# import requests
# import csv
# from pathlib import Path
# from datetime import datetime

# def get_last_updated_date_from_db():
#     # Simulating database date retrieval (you should replace this with actual DB code)
#     return "2025-01-13"  # Example, should return the last updated date from the DB

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
#         # API_URL = "https://intranet.cytrion.com/trading-api"
#         API_URL = "http://127.0.0.1:9009"
#         report_url = f"{API_URL}/get-report2"
#         print(f"Fetching data from API: {report_url}")  # Ensure this is correct

#         # Explicitly use the 10th date for the request
#         desired_date = "2025-01-13"  # Set the desired date here
#         print(f"Requesting data for the date: {desired_date}")

#         # Make API request with explicit date parameter
#         resp = requests.get(report_url, params={'date': desired_date})
#         if resp.status_code != 200:
#             print(f"Error: Failed to fetch data. HTTP Status Code: {resp.status_code}")
#             return

#         response = resp.json()
#         print("API Full Response:", response)  # Print API response to debug

#         # Extract data and date from response
#         data = response.get('data', [])
#         data_date = response.get('date')
#         print("Extracted Data from API:", data)  # Print the extracted data for debugging
#         print("Data Date from API:", data_date)  # Print the data date

#         if not data or not data_date:
#             print("Error: No data or date in the API response.")
#             return

#         # Fetch the last updated date from the database
#         db_last_updated_date = get_last_updated_date_from_db()
#         print(f"Last updated date from DB: {db_last_updated_date}")

#         # Convert date to datetime for comparison
#         api_data_date = datetime.strptime(db_last_updated_date, '%Y-%m-%d')
#         if api_data_date.date() != datetime.strptime(desired_date, '%Y-%m-%d').date():
#             print(f"Warning: The API data is not for the desired date ({desired_date}). Received date: {api_data_date.date()}")

#         # Save data to CSV
#         file_name = f"recommendations-{api_data_date.strftime('%d%b%Y').upper()}.csv"
#         file_path = directory_path / file_name
#         print(f"Data will be saved to: {file_path}")

#         with open(file_path, "w", newline='') as f:
#             f_csv = csv.writer(f)
#             f_csv.writerow([  # CSV headers
#                 "Symbol", "Lot Size", "Future OI", "Change (Future OI)", "# of lots change (Future OI)",
#                 "% Change in Future_OI", "PE OI", "Change (PE OI)", "# of lots change (PE OI)",
#                 "% Change (PE OI)", "CE OI", "Change (CE OI)", "# of lots change (CE OI)",
#                 "% Change (CE OI)", "VWAP", "CLOSE PRICE", "Change CE / PE Ratio (OI)",
#                 "Delivery %", "% Change in Price", "% Change in Price From Low",
#                 "% Change in Price From High", "Decision (Based on Options",
#                 "Decision (Based on Futures", "Decision (Based on Price"
#             ])

#             for x in data:
#                 f_csv.writerow([  # Write data to CSV
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
