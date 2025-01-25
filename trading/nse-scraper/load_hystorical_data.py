# # import random
# # from pathlib import Path

# # import pandas as pd
# # from datetime import datetime

# # from sqlalchemy import text
# # from db import get_db
# # from main_old import NSE
# # import time

# # from models.last_updated_date import LastUpdatedDate


# # def add_hystorical_data_of_a_date(data_date: datetime):
# #     x = NSE()
# #     try:
# #         data = x.equity_cm_bhavcopy_final(data_date)
# #         print("equity_cm_bhavcopy_final:", data)
# #         x.add_equity_udiff_bhavcopy_to_db(data, verify_in_db=False)
# #     except RuntimeError as e:
# #         print(e)
# #     try:
# #         data = x.equity_fo_bhavcopy_final(data_date)
# #         print("equity_cm_bhavcopy_final:", data)
# #         x.add_fo_udiff_bhavcopy_to_db(data, verify_in_db=False)
# #     except RuntimeError as e:
# #         print(e)
# #     try:
# #         data = x.securities_bhavdata(data_date)
# #         print("securities_bhavdata:", data)
# #         x.add_securities_bhavdata_to_db(data, verify_in_db=False)
# #     except RuntimeError as e:
# #         print(e)
# #     try:
# #         data = x.equity_block(data_date)
# #         print("equity_block:", data)
# #         x.add_equity_block_to_db(data, verify_in_db=False)
# #     except RuntimeError as e:
# #         print(e)
# #     try:
# #         data = x.equity_bulk(data_date)
# #         print("equity_bulk:", data)
# #         x.add_equity_bulk_to_db(data, verify_in_db=False)
# #     except RuntimeError as e:
# #         print(e)


# # if __name__ == "__main__":
# #     dates = pd.date_range(start="2024-05-10",
# #                           end=datetime.now().strftime('%Y-%m-%d'), freq='B')
# #     count = 0
# #     for date in dates:
# #         add_hystorical_data_of_a_date(date)
# #         if count % 4 == 0:
# #             time.sleep(random.randint(0, 1))
# #         count += 1

# #     with next(get_db()) as db_session:
# #         latest_trade_date = db_session.execute(
# #             text('select max(trade_date) from fo_udiff_bhavdata')).scalar()
# #         last_updated = db_session.query(LastUpdatedDate).first()
# #         if last_updated:
# #             last_updated.last_updated_date = latest_trade_date
# #             db_session.add(last_updated)
# #         else:
# #             db_session.add(LastUpdatedDate(
# #                 last_updated_date=latest_trade_date))
# #         db_session.commit()


# import random
# from pathlib import Path
# import pandas as pd
# from datetime import datetime
# from sqlalchemy import text
# from db import get_db
# from main_old import NSE
# import time
# from models.last_updated_date import LastUpdatedDate
# import chardet

# def detect_encoding(file_path):
#     """Detect the encoding of a file."""
#     with open(file_path, 'rb') as f:
#         result = chardet.detect(f.read())
#         return result['encoding']

# def add_hystorical_data_of_a_date(data_date: datetime):
#     x = NSE()
#     try:
#         data = x.equity_cm_bhavcopy_final(data_date)
#         x.add_equity_udiff_bhavcopy_to_db(data, verify_in_db=False)
#     except RuntimeError as e:
#         print(f"Error processing CM Bhavcopy: {e}")
#     try:
#         data = x.equity_fo_bhavcopy_final(data_date)
#         x.add_fo_udiff_bhavcopy_to_db(data, verify_in_db=False)
#     except RuntimeError as e:
#         print(f"Error processing FO Bhavcopy: {e}")
#     try:
#         data = x.securities_bhavdata(data_date)
#         x.add_securities_bhavdata_to_db(data, verify_in_db=False)
#     except RuntimeError as e:
#         print(f"Error processing Securities Bhavdata: {e}")
#     try:
#         data = x.equity_block(data_date)
#         # Detect encoding for the data file
#         encoding = detect_encoding(data) if isinstance(data, str) and Path(data).is_file() else 'utf-8'
#         try:
#             csv_data = pd.read_csv(data, encoding=encoding, parse_dates=["Date "], dtype={
#                 'Symbol': str,
#                 'Open': float,
#                 'High': float,
#                 'Low': float,
#                 'Close': float,
#                 'Last': float,
#                 'Prev Close': float,
#                 'Tot Trd Qty': int,
#                 'Tot Trd Val': float,
#                 'Total Trades': int,
#                 'ISIN': str,
#             })
#             x.add_equity_block_to_db(csv_data, verify_in_db=False)
#         except UnicodeDecodeError as e:
#             print(f"UnicodeDecodeError while reading the file: {e}")
#     except RuntimeError as e:
#         print(f"Error processing Equity Block: {e}")
#     try:
#         data = x.equity_bulk(data_date)
#         x.add_equity_bulk_to_db(data, verify_in_db=False)
#     except RuntimeError as e:
#         print(f"Error processing Equity Bulk: {e}")

# if __name__ == "__main__":
#     dates = pd.date_range(start="2025-01-01",
#                           end=datetime.now().strftime('%Y-%m-%d'), freq='B')
#     count = 0
#     for date in dates:
#         add_hystorical_data_of_a_date(date)
#         if count % 4 == 0:
#             time.sleep(random.randint(0, 1))
#         count += 1

#     with next(get_db()) as db_session:
#         latest_trade_date = db_session.execute(
#             text('select max(trade_date) from fo_udiff_bhavdata')).scalar()
#         last_updated = db_session.query(LastUpdatedDate).first()
#         if last_updated:
#             last_updated.last_updated_date = latest_trade_date
#             db_session.add(last_updated)
#         else:
#             db_session.add(LastUpdatedDate(
#                 last_updated_date=latest_trade_date))
#         db_session.commit()


# x = NSE()
# import pandas as pd
# import numpy as np
# # csv_data = pd.read_csv(Path("/home/ayyappa/workspace/stock_market/temp/block_05012024.csv"), dtype={'Trade Price / Wght. Avg. Price ': str})
# # print(csv_data.dtypes)
# # print(csv_data.columns)
# # x.add_equity_block_to_db(Path("/home/ayyappa/workspace/stock_market/temp/block_05012024.csv"))


# import requests

# url = "https://api.upstox.com/v2/market/holidays"

# payload={}
# headers = {
#   'Accept': 'application/json'
# }

# response = requests.request("GET", url, headers=headers)

# print(response.json())





import random
from pathlib import Path

import pandas as pd
from datetime import datetime

from sqlalchemy import text
from db import get_db
from main_old import NSE
import time

from models.last_updated_date import LastUpdatedDate

# import pdb;pdb.set_trace()


def add_historical_data_of_a_date(data_date: datetime):
    x = NSE()
    try:
        data = x.equity_cm_bhavcopy_final(data_date)
        print("equity_cm_bhavcopy_final:", data)
        x.add_equity_udiff_bhavcopy_to_db(data, verify_in_db=False)
    except RuntimeError as e:
        print(e)
    try:
        data = x.equity_fo_bhavcopy_final(data_date)
        print("equity_fo_bhavcopy_final:", data)
        x.add_fo_udiff_bhavcopy_to_db(data, verify_in_db=False)
    except RuntimeError as e:
        print(e)
    try:
        data = x.securities_bhavdata(data_date)
        print("securities_bhavdata:", data)
        x.add_securities_bhavdata_to_db(data, verify_in_db=False)
    except RuntimeError as e:
        print(e)
    # try:
    #     data = x.equity_block(data_date)
    #     print("equity_block:", data)
    #     x.add_equity_block_to_db(data, verify_in_db=False)
    # except RuntimeError as e:
    #     print(e)
    # try:
    #     data = x.equity_bulk(data_date)
    #     print("equity_bulk:", data)
    #     x.add_equity_bulk_to_db(data, verify_in_db=False)
    # except RuntimeError as e:
    #     print(e)


if __name__ == "__main__":
    # Generate business days only
    dates = pd.date_range(start="2025-01-19", 
                          end=datetime.now().strftime('%Y-%m-%d'), 
                          freq='B')  # freq='B' ensures no Sat/Sun

    count = 0
    for date in dates:
        # Increment through business days
        add_historical_data_of_a_date(date)
        
        # Add sleep for every 4 iterations
        if count % 4 == 0:
            time.sleep(random.randint(1, 2))  # Slight delay to avoid overwhelming the system
        
        count += 1

    with next(get_db()) as db_session:
        latest_trade_date = db_session.execute(
            text('select max(trade_date) from fo_udiff_bhavdata')).scalar()
        last_updated = db_session.query(LastUpdatedDate).first()
        if last_updated:
            last_updated.last_updated_date = latest_trade_date
            db_session.add(last_updated)
        else:
            db_session.add(LastUpdatedDate(
                last_updated_date=latest_trade_date))
        db_session.commit()
