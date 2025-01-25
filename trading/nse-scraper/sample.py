# import requests
# import pandas as pd

# # def fetch_data_from_api(params):
# #     response = requests.get('https://www.nseindia.com/all-reports', params=params)
# #     if response.status_code == 200:
# #         return pd.DataFrame(response.json())  # Adjust based on API's response format
# #     else:
# #         raise Exception("API call failed with status code:", response.status_code)
    
# # print(api_data[['ROC_RS_7', 'ROC_RS_14']])

# # def fetch_data_from_api(date='2025-01-14'):
# #     url = f"http://example-api.com/data?date={date}"
# #     response = requests.get(url)
# #     if response.status_code == 200:
# #         return pd.DataFrame(response.json())  # Adjust based on your API's response format
# #     else:
# #         raise Exception(f"API call failed: {response.status_code}")
# # fetch_data_from_api(date='2025-01-14') 


# # import pandas as pd

# # df = pd.read_excel("filename.xlsx")
# # print(df.head())

# # import chardet

# # with open(r"C:\Page_Projects\trading.tar 1\trading.tar 1\trading\nse-scraper\temp\ma20-Jan-2025.csv", "rb") as f:
# #     raw_data = f.read()
# #     result = chardet.detect(raw_data)
# #     print(result)  # Detect the file encoding

# # # Open the file using the detected encoding
# # encoding = result['encoding']
# # with open(r"C:\Page_Projects\trading.tar 1\trading.tar 1\trading\nse-scraper\temp\ma20-Jan-2025.csv", "r", encoding=encoding) as f:
# #     print(f.read(100))  # Read and verify the content


# # import chardet

# # # File path
# # file_path = r"C:\Page_Projects\trading.tar 1\trading.tar 1\trading\nse-scraper\temp\ma20-Jan-2025.csv"

# # # Detect file encoding
# # with open(file_path, "rb") as f:
# #     raw_data = f.read()
# #     result = chardet.detect(raw_data)
# #     print("Encoding detection result:", result)

# # # Use detected encoding or fallback
# # encoding = result['encoding'] if result['encoding'] else 'utf-8'

# # try:
# #     with open(file_path, "r", encoding=encoding) as f:
# #         print(f.read(100))  # Read and verify the content
# # except UnicodeDecodeError as e:
# #     print(f"UnicodeDecodeError: {e}")
# #     print("Trying with 'iso-8859-1' fallback...")
# #     # Fallback encoding
# #     with open(file_path, "r", encoding="iso-8859-1") as f:
# #         print(f.read(100))  # Read and verify the content



# # import pandas as pd
# # from googletrans import Translator

# # # Load CSV into DataFrame
# # df = pd.read_csv(r'C:\Page_Projects\trading.tar 1\trading.tar 1\trading\nse-scraper\temp\ma20-Jan-2025.csv')

# # # Initialize translator
# # translator = Translator()

# # # Function to translate text in a column
# # def translate_text(text):
# #     try:
# #         return translator.translate(text, src='en', dest='es').text
# #     except Exception as e:
# #         return text  # In case of an error, return the original text

# # # Apply translation to all columns (if they contain text)
# # for column in df.columns:
# #     df[column] = df[column].apply(lambda x: translate_text(str(x)))

# # # Save the translated file
# # df.to_csv('translated_file.csv', index=False)



# import pandas as pd
# from googletrans import Translator

# # Load CSV into DataFrame with a different encoding (ISO-8859-1)
# df = pd.read_csv(r'C:\Page_Projects\trading.tar 1\trading.tar 1\trading\nse-scraper\temp\ma20-Jan-2025.csv', encoding='ISO-8859-1', delimiter=';',engine='python')

# # Initialize translator
# translator = Translator()

# # Function to translate text in a column
# def translate_text(text):
#     try:
#         return translator.translate(text, src='en', dest='es').text
#     except Exception as e:
#         return text  # In case of an error, return the original text

# # Apply translation to all columns (if they contain text)
# for column in df.columns:
#     df[column] = df[column].apply(lambda x: translate_text(str(x)))

# # Save the translated file
# df.to_csv('translated_file.csv', index=False)

import json
from datetime import datetime
import numpy as np
import pandas as pd

from flask import Flask, make_response, request, jsonify
from flask_cors import CORS


from config import settings

from sqlalchemy import func, text

from flask_sqlalchemy import SQLAlchemy
import talib


from db import engine, get_db
from models.fo_stock_intstruments_report_1 import FOStockInstrumentsReport1
from models.equity_block import EquityBlock
from models.equity_bulk import EquityBulk
from models.fo_stock_intstruments_report_1 import FOStockInstrumentsReport1
from models.last_updated_date import LastUpdatedDate
from models.securities_bhavdata import SecurityiesBhavData
from models.cm_udiff_bhavdata import CMUDIFFBhavData
from models.fo_udiff_bhavdata import FOUDIFFBhavData
from models.start_date_lookup import StartDateLookup
from models.last_updated_date import LastUpdatedDate
from models.cm_market_data_indexes import CMMarketDataIndexes
from models.fo_combine_oi_delta_equivalent import FOCombineOIDeltaEquivalent
from models.holidays import Holidays
from constants import TABLES, mst_table_mapping, INDEX_FOR_ADOPTIVE_EMA_CALCULATION


# HOST = "0.0.0.0"
# PORT = 9009

# app = Flask(__name__)


# CORS(app)

# app.config['SQLALCHEMY_DATABASE_URI'] = "mysql+pymysql://root:root@localhost:3306/testdb"

def get_report2():
    """stock generator"""
    
    with next(get_db()) as db_session:
        
        _date = request.args.get('date', '')
        import pdb;pdb.set_trace()
        if _date:
            try:
                x = datetime.strptime(_date, '%Y-%m-%d')
            except:
                return make_response(jsonify({"message": "Invalid Request"}), 400)
            last_updated = _date
        else:
            last_updated = db_session.execute(
                text("select DATE_FORMAT(max(last_updated_date), '%Y-%m-%d') as last_updated_date from last_updated_date")).scalar()
            if not last_updated:
                last_updated = db_session.execute(
                    text("select DATE_FORMAT(max(`date`), '%Y-%m-%d') as last_updated_date from securities_bhavdata")).scalar()
        
        js = db_session.query(FOUDIFFBhavData).filter(
            FOUDIFFBhavData.trade_date == last_updated).order_by(FOUDIFFBhavData.tckr_symb).all()
        
        sf_data = db_session.query(SecurityiesBhavData).filter(
            SecurityiesBhavData.date == last_updated).all()
        
        
        sf_data_dict = {}
        for row in sf_data:
            if row.series.strip() == "EQ":
                print(row.symbol)
                if row.symbol not in sf_data_dict:
                    sf_data_dict[row.symbol] = {
                        'VWAP': 0, 'CHANGE_IN_PRICE': 0}
                try:
                    sf_data_dict[row.symbol]['VWAP'] = (float(row.avg_price))
                except:
                    sf_data_dict[row.symbol]['VWAP'] = 0
                try:
                    sf_data_dict[row.symbol]['CLOSE_PRICE'] = (
                        float(row.close_price))
                except:
                    sf_data_dict[row.symbol]['CLOSE_PRICE'] = 0
                try:
                    sf_data_dict[row.symbol]['DELIVERY_PERCENTAGE'] = (
                        float(row.delivery_percentage))
                except:
                    sf_data_dict[row.symbol]['DELIVERY_PERCENTAGE'] = 0
                try:
                    sf_data_dict[row.symbol]['TOTAL_OPEN_TRADES'] = (
                        float(row.number_of_trades))
                except:
                    sf_data_dict[row.symbol]['TOTAL_OPEN_TRADES'] = 0
                try:
                    sf_data_dict[row.symbol]['CHANGE_IN_PRICE'] = (
                        (float(row.close_price)-float(row.prev_close))/float(row.prev_close))
                except:
                    sf_data_dict[row.symbol]['CHANGE_IN_PRICE'] = 0
                try:
                    sf_data_dict[row.symbol]['CHANGE_IN_PRICE_LOW'] = (
                        (float(row.close_price)-float(row.low_price))/float(row.low_price))
                except:
                    sf_data_dict[row.symbol]['CHANGE_IN_PRICE_LOW'] = 0
                try:
                    sf_data_dict[row.symbol]['CHANGE_IN_PRICE_HIGH'] = (
                        (float(row.high_price)-float(row.close_price))/float(row.high_price))
                except:
                    sf_data_dict[row.symbol]['CHANGE_IN_PRICE_HIGH'] = 0
        processed_bhavcopy_json = {}
        lot_sizes_dict = {}
        for row in js:
            lot_sizes_dict[row.tckr_symb] = row.new_brd_lot_qty
            if row.tckr_symb not in processed_bhavcopy_json:
                processed_bhavcopy_json[row.tckr_symb] = {
                    'Future_OI': 0,
                    'Change_in_Future_OI': 0,
                    "Symbol": row.tckr_symb,
                    'Options_PE_OI': 0,
                    'Change_in_Options_PE_OI': 0,
                    'Options_CE_OI': 0,
                    'Change_in_Options_CE_OI': 0}
                processed_bhavcopy_json[row.tckr_symb
                                        ]['Total_OI'] = int(row.opn_intrst)
                # processed_bhavcopy_json[row.tckr_symb]['Change_in_Total_OI'] = int(row.chng_in_opn_intrst)
                if row.fin_instrm_tp == 'STF':
                    processed_bhavcopy_json[row.tckr_symb
                                            ]['Future_OI'] = int(row.opn_intrst)
                    processed_bhavcopy_json[row.tckr_symb]['Change_in_Future_OI'] = int(
                        row.chng_in_opn_intrst)
                if row.fin_instrm_tp == 'STO':
                    if row.optn_tp == 'PE':
                        processed_bhavcopy_json[row.tckr_symb]['Options_PE_OI'] = int(
                            row.opn_intrst)
                        processed_bhavcopy_json[row.tckr_symb]['Change_in_Options_PE_OI'] = int(
                            row.chng_in_opn_intrst)
                    elif row.optn_tp == 'CE':
                        processed_bhavcopy_json[row.tckr_symb]['Options_CE_OI'] = int(
                            row.opn_intrst)
                        processed_bhavcopy_json[row.tckr_symb]['Change_in_Options_CE_OI'] = int(
                            row.chng_in_opn_intrst)
            else:
                processed_bhavcopy_json[row.tckr_symb
                                        ]['Total_OI'] += int(row.opn_intrst)
                # processed_bhavcopy_json[row.tckr_symb]['Change_in_Total_OI'] += int(row.chng_in_opn_intrst)

                if row.fin_instrm_tp == 'STF':
                    processed_bhavcopy_json[row.tckr_symb
                                            ]['Future_OI'] += int(row.opn_intrst)
                    processed_bhavcopy_json[row.tckr_symb
                                            ]['Change_in_Future_OI'] += int(row.chng_in_opn_intrst)
                if row.fin_instrm_tp == 'STO':
                    if row.optn_tp == 'PE':
                        processed_bhavcopy_json[row.tckr_symb
                                                ]['Options_PE_OI'] += int(row.opn_intrst)
                        processed_bhavcopy_json[row.tckr_symb
                                                ]['Change_in_Options_PE_OI'] += int(row.chng_in_opn_intrst)
                    elif row.optn_tp == 'CE':
                        processed_bhavcopy_json[row.tckr_symb
                                                ]['Options_CE_OI'] += int(row.opn_intrst)
                        processed_bhavcopy_json[row.tckr_symb
                                                ]['Change_in_Options_CE_OI'] += int(row.chng_in_opn_intrst)

        for key in processed_bhavcopy_json:
            print(key)
            try:
                if int(lot_sizes_dict[key]) > 0:
                    processed_bhavcopy_json[key]["lotsize"] = int(
                        lot_sizes_dict[key])
                    processed_bhavcopy_json[key]["Total_lots_OI"] = processed_bhavcopy_json[key]['Total_OI']/int(
                        lot_sizes_dict[key])
                    try:
                        processed_bhavcopy_json[key]["vwap"] = float(
                            sf_data_dict[key]['VWAP'])
                        processed_bhavcopy_json[key]["close_price"] = float(
                            sf_data_dict[key]['CLOSE_PRICE'])
                        processed_bhavcopy_json[key]["delivery_percentage"] = float(
                            sf_data_dict[key]['DELIVERY_PERCENTAGE'])
                        processed_bhavcopy_json[key]["change_percentage_in_price"] = float(
                            sf_data_dict[key]['CHANGE_IN_PRICE'])
                        # processed_bhavcopy_json[key]["total_open_trades"] = float(sf_data_dict[key]['TOTAL_OPEN_TRADES'])
                        processed_bhavcopy_json[key]["change_percentage_in_price_from_low"] = float(
                            sf_data_dict[key]['CHANGE_IN_PRICE_LOW'])
                        processed_bhavcopy_json[key]["change_percentage_in_price_from_high"] = float(
                            sf_data_dict[key]['CHANGE_IN_PRICE_HIGH'])
                    except:
                        processed_bhavcopy_json[key]["vwap"] = 0
                        processed_bhavcopy_json[key]["close_price"] = 0
                        processed_bhavcopy_json[key]["delivery_percentage"] = 0
                        processed_bhavcopy_json[key]["change_percentage_in_price"] = 0
                        # processed_bhavcopy_json[key]["total_open_trades"] = 0
                        processed_bhavcopy_json[key]["change_percentage_in_price_from_low"] = 0
                        processed_bhavcopy_json[key]["change_percentage_in_price_from_high"] = 0

                    try:
                        processed_bhavcopy_json[key]["Change_percentage_in_Future_OI"] = (float(processed_bhavcopy_json[key]["Change_in_Future_OI"])/(
                            float(processed_bhavcopy_json[key]["Future_OI"]) - processed_bhavcopy_json[key]["Change_in_Future_OI"])) * 100
                    except:
                        processed_bhavcopy_json[key]["Change_percentage_in_Future_OI"] = 0

                    try:
                        processed_bhavcopy_json[key]["Future_lots_Change_OI"] = float(
                            processed_bhavcopy_json[key]["Change_in_Future_OI"])/float(processed_bhavcopy_json[key]["lotsize"])
                    except:
                        processed_bhavcopy_json[key]["Future_lots_Change_OI"] = 0

                    processed_bhavcopy_json[key]["Change_lots_in_PE_OI"] = float(
                        processed_bhavcopy_json[key]["Change_in_Options_PE_OI"])/float(processed_bhavcopy_json[key]["lotsize"])

                    try:
                        processed_bhavcopy_json[key]["Change_percentage_in_PE_OI"] = (float(processed_bhavcopy_json[key]["Change_in_Options_PE_OI"])/(
                            float(processed_bhavcopy_json[key]["Options_PE_OI"]) - float(processed_bhavcopy_json[key]["Change_in_Options_PE_OI"]))) * 100
                    except:
                        processed_bhavcopy_json[key]["Change_percentage_in_PE_OI"] = 0

                    processed_bhavcopy_json[key]["Change_lots_in_CE_OI"] = float(
                        processed_bhavcopy_json[key]["Change_in_Options_CE_OI"])/float(processed_bhavcopy_json[key]["lotsize"])

                    try:
                        processed_bhavcopy_json[key]["Change_percentage_in_CE_OI"] = (float(processed_bhavcopy_json[key]["Change_in_Options_CE_OI"])/(
                            float(processed_bhavcopy_json[key]["Options_CE_OI"]) - float(processed_bhavcopy_json[key]["Change_in_Options_CE_OI"]))) * 100
                    except:
                        processed_bhavcopy_json[key]["Change_percentage_in_CE_OI"] = 0

                    try:
                        processed_bhavcopy_json[key]["change_ce_pe_oi_ratio"] = (float(
                            processed_bhavcopy_json[key]["Change_in_Options_CE_OI"])/float(processed_bhavcopy_json[key]["Change_in_Options_PE_OI"]))
                    except:
                        processed_bhavcopy_json[key]["change_ce_pe_oi_ratio"] = 0

                    # processed_bhavcopy_json[key]["Total_lots_OI"] = processed_bhavcopy_json[key]['Total_OI']/int(lot_sizes_dict[key])
                    # processed_bhavcopy_json[key]["Change_percentage_in_Total_OI"] = (float(processed_bhavcopy_json[key]["Change_in_Total_OI"])/(float(processed_bhavcopy_json[key]["Total_OI"]) - processed_bhavcopy_json[key]["Change_in_Total_OI"])) * 100

                    if processed_bhavcopy_json[key]["Change_percentage_in_PE_OI"] < 10 and processed_bhavcopy_json[key]["Change_percentage_in_CE_OI"] > 10:
                        # if processed_bhavcopy_json[key]["Change_lots_in_CE_OI"] > 100:
                        # if float(processed_bhavcopy_json[key]["Options_CE_OI"])/float(processed_bhavcopy_json[key]["lotsize"]) > 100.0:
                        processed_bhavcopy_json[key]['decision_based_on_options'] = "Possible Sell"
                    elif processed_bhavcopy_json[key]["Change_percentage_in_PE_OI"] > 10 and processed_bhavcopy_json[key]["Change_percentage_in_CE_OI"] < 10:
                        # if processed_bhavcopy_json[key]["Change_lots_in_PE_OI"] > 100:
                        # if float(processed_bhavcopy_json[key]["Options_PE_OI"])/float(processed_bhavcopy_json[key]["lotsize"]) > 100.0:
                        processed_bhavcopy_json[key]['decision_based_on_options'] = "Possible Buy"
                    else:
                        processed_bhavcopy_json[key]['decision_based_on_options'] = "None"

                    if processed_bhavcopy_json[key]["change_percentage_in_price_from_low"] > 0.02:
                        # if processed_bhavcopy_json[key]["Change_lots_in_CE_OI"] > 100:
                        # if float(processed_bhavcopy_json[key]["Options_CE_OI"])/float(processed_bhavcopy_json[key]["lotsize"]) > 100.0:
                        processed_bhavcopy_json[key]['decision_based_on_future'] = "Possible Buy"
                    elif processed_bhavcopy_json[key]["change_percentage_in_price_from_high"] > 0.02:
                        # if processed_bhavcopy_json[key]["Change_lots_in_PE_OI"] > 100:
                        # if float(processed_bhavcopy_json[key]["Options_PE_OI"])/float(processed_bhavcopy_json[key]["lotsize"]) > 100.0:
                        processed_bhavcopy_json[key]['decision_based_on_future'] = "Possible Sell"
                    else:
                        processed_bhavcopy_json[key]['decision_based_on_future'] = "None"

                    if (processed_bhavcopy_json[key]["vwap"] - processed_bhavcopy_json[key]["close_price"]) > 0:
                        # if processed_bhavcopy_json[key]["Change_lots_in_CE_OI"] > 100:
                        # if float(processed_bhavcopy_json[key]["Options_CE_OI"])/float(processed_bhavcopy_json[key]["lotsize"]) > 100.0:
                        processed_bhavcopy_json[key]['decision_based_on_price'] = "Weak. Price Close Below VWAP."
                    elif (processed_bhavcopy_json[key]["vwap"] - processed_bhavcopy_json[key]["close_price"]) < 0:
                        # if processed_bhavcopy_json[key]["Change_lots_in_PE_OI"] > 100:
                        # if float(processed_bhavcopy_json[key]["Options_PE_OI"])/float(processed_bhavcopy_json[key]["lotsize"]) > 100.0:
                        processed_bhavcopy_json[key]['decision_based_on_price'] = "Strong. Price Close Above VWAP."
                    else:
                        processed_bhavcopy_json[key]['decision_based_on_price'] = "Neutral"
            except KeyError:
                print(f"{key} not found in lot size")

        write_first = ["BANKNIFTY",
                       "FINNIFTY",
                       "MIDCPNIFTY",
                       "NIFTY",
                       "NIFTYNXT50"]
        sorted_dict_list = sorted(
            list(processed_bhavcopy_json.values()),
            key=lambda d: (0, write_first.index(d.get('Symbol', ''))) if d.get(
                'Symbol', '') in write_first else (1, d.get('name', ''))
        )
        resp = json.dumps({'data': [{
            "Symbol": values['Symbol'],
            "Lot Size": values['lotsize'],
            "Future OI": values['Future_OI'],
            "Change (Future OI)": values['Change_in_Future_OI'],
            "# of lots change (Future OI)": values['Future_lots_Change_OI'],
            "% Change in Future_OI": round(values['Change_percentage_in_Future_OI'], 6),
            "PE OI": values['Options_PE_OI'],
            "Change (PE OI)": values['Change_in_Options_PE_OI'],
            "# of lots change (PE OI)": values['Change_lots_in_PE_OI'],
            "% Change (PE OI)": round(values['Change_percentage_in_PE_OI'], 6),
            "CE OI": values['Options_CE_OI'],
            "Change (CE OI)": values['Change_in_Options_CE_OI'],
            "# of lots change (CE OI)": values['Change_lots_in_CE_OI'],
            "% Change (CE OI)": round(values['Change_percentage_in_CE_OI'], 6),
            "VWAP": values['vwap'],
            "CLOSE PRICE": values['close_price'],
            "Change CE / PE Ratio (OI)": round(values['change_ce_pe_oi_ratio'], 6),
            "Delivery %": round(values['delivery_percentage'], 6),
            "% Change in Price": round(values['change_percentage_in_price'], 6),
            "% Change in Price From Low": round(values['change_percentage_in_price_from_low'], 6),
            "% Change in Price From High": round(values['change_percentage_in_price_from_high'], 6),
            "Decision (Based on Futures": values['decision_based_on_future'],
            "Decision (Based on Options": values['decision_based_on_options'],
            "Decision (Based on Price": values['decision_based_on_price']
        }for values in sorted_dict_list], 'date': last_updated})

        # return app.response_class(
        #     response=resp,
        #     mimetype='application/json', status=200)


get_report2()