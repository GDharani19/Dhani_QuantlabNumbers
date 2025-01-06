import datetime
import requests
import pandas as pd
from io import StringIO
from pymongo import MongoClient
from config import settings

DATA_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

mongo_client = MongoClient(settings.MONGO_URI)
db = mongo_client[settings.MONGO_DATABASE]
collection = db["ticker_info"]

response = requests.get(DATA_URL)
if response.status_code == 200:
    # Parse the CSV file content using pandas
    csv_content = response.content.decode('utf-8')
    data = pd.read_csv(StringIO(csv_content))

    # Display the first few rows of the DataFrame
    print("Original Data:")
    print(data.head())

    filtered_data = data[
        (data['EXCH_ID'].isin(['NSE', 'BSE'])) &
        (data['INSTRUMENT'] == 'FUTIDX') &
        (data['UNDERLYING_SYMBOL'].isin(
            ['BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTY', 'SENSEX'])) &
        (data['EXPIRY_FLAG'] == 'M')
    ]

    # Display the filtered data
    print("Filtered Data:")
    filtered_data = filtered_data[['SECURITY_ID', 'UNDERLYING_SYMBOL', 'EXCH_ID',
                                   'SYMBOL_NAME', 'DISPLAY_NAME', 'LOT_SIZE', 'SM_EXPIRY_DATE']].reset_index(drop=True)

    filtered_data['EXPIRY_MONTH'] = filtered_data['DISPLAY_NAME'].str.split().str[1]
    sorted_data = filtered_data.sort_values(
        by=['UNDERLYING_SYMBOL', 'SM_EXPIRY_DATE']).reset_index(drop=True)
    sorted_data["deleted_at"] = None
    sorted_data = sorted_data.to_dict('records')

    lot_sizes = {k['SECURITY_ID']: k for k in sorted_data}

    old_data = collection.find({'deleted_at': None, "SM_EXPIRY_DATE": {
                               '$gte': datetime.datetime.now(datetime.timezone.utc)}})
    for x in old_data:
        if x['SECURITY_ID'] in lot_sizes.keys():
            if x['LOT_SIZE'] != lot_sizes.get(x['SECURITY_ID'])['LOT_SIZE']:
                x['deleted_at'] = datetime.datetime.now(datetime.timezone.utc)
                collection.update_one({'_id': x['_id']}, x)
            else:
                del lot_sizes[x['SECURITY_ID']]
                                                                                                                               
    print(lot_sizes.values())
    if lot_sizes:
        collection.insert_many(lot_sizes.values())

else:
    print(f"Failed to download the file. Status code: {response.status_code}")


# x = [
#     {'SECURITY_ID': 35013, 'UNDERLYING_SYMBOL': 'BANKNIFTY', 'SYMBOL_NAME': 'BANKNIFTY-Nov2024-FUT',
#      'DISPLAY_NAME': 'BANKNIFTY NOV FUT', 'LOT_SIZE': 15.0, 'SM_EXPIRY_DATE': '2024-11-27', 'deleted_at': None},
#     {'SECURITY_ID': 35025, 'UNDERLYING_SYMBOL': 'BANKNIFTY', 'SYMBOL_NAME': 'BANKNIFTY-Dec2024-FUT',
#      'DISPLAY_NAME': 'BANKNIFTY DEC FUT', 'LOT_SIZE': 15.0, 'SM_EXPIRY_DATE': '2024-12-24', 'deleted_at': None},
#     {'SECURITY_ID': 35012, 'UNDERLYING_SYMBOL': 'BANKNIFTY', 'SYMBOL_NAME': 'BANKNIFTY-Jan2025-FUT',
#      'DISPLAY_NAME': 'BANKNIFTY JAN FUT', 'LOT_SIZE': 15.0, 'SM_EXPIRY_DATE': '2025-01-29', 'deleted_at': None},
#     {'SECURITY_ID': 35426, 'UNDERLYING_SYMBOL': 'FINNIFTY', 'SYMBOL_NAME': 'FINNIFTY-Dec2024-FUT',
#      'DISPLAY_NAME': 'FINNIFTY DEC FUT', 'LOT_SIZE': 25.0, 'SM_EXPIRY_DATE': '2024-12-31', 'deleted_at': None},
#     {'SECURITY_ID': 35239, 'UNDERLYING_SYMBOL': 'FINNIFTY', 'SYMBOL_NAME': 'FINNIFTY-Jan2025-FUT',
#      'DISPLAY_NAME': 'FINNIFTY JAN FUT', 'LOT_SIZE': 25.0, 'SM_EXPIRY_DATE': '2025-01-28', 'deleted_at': None},
#     {'SECURITY_ID': 35525, 'UNDERLYING_SYMBOL': 'FINNIFTY', 'SYMBOL_NAME': 'FINNIFTY-Feb2025-FUT',
#      'DISPLAY_NAME': 'FINNIFTY FEB FUT', 'LOT_SIZE': 65.0, 'SM_EXPIRY_DATE': '2025-02-25', 'deleted_at': None},
#     {'SECURITY_ID': 35004, 'UNDERLYING_SYMBOL': 'MIDCPNIFTY', 'SYMBOL_NAME': 'MIDCPNIFTY-Dec2024-FUT',
#      'DISPLAY_NAME': 'MIDCPNIFTY DEC FUT', 'LOT_SIZE': 50.0, 'SM_EXPIRY_DATE': '2024-12-30', 'deleted_at': None},
#     {'SECURITY_ID': 35007, 'UNDERLYING_SYMBOL': 'MIDCPNIFTY', 'SYMBOL_NAME': 'MIDCPNIFTY-Jan2025-FUT',
#      'DISPLAY_NAME': 'MIDCPNIFTY JAN FUT', 'LOT_SIZE': 50.0, 'SM_EXPIRY_DATE': '2025-01-27', 'deleted_at': None},
#     {'SECURITY_ID': 43292, 'UNDERLYING_SYMBOL': 'MIDCPNIFTY', 'SYMBOL_NAME': 'MIDCPNIFTY-Feb2025-FUT',
#      'DISPLAY_NAME': 'MIDCPNIFTY FEB FUT', 'LOT_SIZE': 120.0, 'SM_EXPIRY_DATE': '2025-02-24', 'deleted_at': None},
#     {'SECURITY_ID': 35089, 'UNDERLYING_SYMBOL': 'NIFTY', 'SYMBOL_NAME': 'NIFTY-Nov2024-FUT',
#      'DISPLAY_NAME': 'NIFTY NOV FUT', 'LOT_SIZE': 25.0, 'SM_EXPIRY_DATE': '2024-11-28', 'deleted_at': None},
#     {'SECURITY_ID': 35005, 'UNDERLYING_SYMBOL': 'NIFTY', 'SYMBOL_NAME': 'NIFTY-Dec2024-FUT',
#      'DISPLAY_NAME': 'NIFTY DEC FUT', 'LOT_SIZE': 25.0, 'SM_EXPIRY_DATE': '2024-12-26', 'deleted_at': None},
#     {'SECURITY_ID': 35006, 'UNDERLYING_SYMBOL': 'NIFTY', 'SYMBOL_NAME': 'NIFTY-Jan2025-FUT',
#      'DISPLAY_NAME': 'NIFTY JAN FUT', 'LOT_SIZE': 25.0, 'SM_EXPIRY_DATE': '2025-01-30', 'deleted_at': None},
#     {'SECURITY_ID': 1126233, 'UNDERLYING_SYMBOL': 'SENSEX', 'SYMBOL_NAME': 'BSXFUT',
#      'DISPLAY_NAME': 'SENSEX NOV  FUT', 'LOT_SIZE': 10.0, 'SM_EXPIRY_DATE': '2024-11-29', 'deleted_at': None},
#     {'SECURITY_ID': 1146913, 'UNDERLYING_SYMBOL': 'SENSEX', 'SYMBOL_NAME': 'BSXFUT',
#      'DISPLAY_NAME': 'SENSEX DEC  FUT', 'LOT_SIZE': 10.0, 'SM_EXPIRY_DATE': '2024-12-27', 'deleted_at': None},
#     {'SECURITY_ID': 1164591, 'UNDERLYING_SYMBOL': 'SENSEX', 'SYMBOL_NAME': 'BSXFUT',
#      'DISPLAY_NAME': 'SENSEX JAN  FUT', 'LOT_SIZE': 10.0, 'SM_EXPIRY_DATE': '2025-01-31', 'deleted_at': None}]
