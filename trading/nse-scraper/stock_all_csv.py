# import copy

# import numpy as np
# from requests import Session, ReadTimeout
# from requests.exceptions import ConnectionError
# from pathlib import Path
# import pickle
# import os
# import random
# from typing import Literal, Any, Optional, Union, List, Dict
# from zipfile import ZipFile
# from datetime import date, datetime, timedelta
# import pandas as pd
# from sqlalchemy import text

# from config import settings
# from db import engine, get_db
# from load_start_days import get_instrumet_start_day
# from models.cm_market_data_indexes import CMMarketDataIndexes
# from models.equity_block import EquityBlock
# from models.equity_bulk import EquityBulk
# from models.fo_combine_oi_delta_equivalent import FOCombineOIDeltaEquivalent
# from models.fo_stock_intstruments_report_1 import FOStockInstrumentsReport1
# from models.last_updated_date import LastUpdatedDate
# from models.securities_bhavdata import SecurityiesBhavData
# from models.cm_udiff_bhavdata import CMUDIFFBhavData
# from models.fo_udiff_bhavdata import FOUDIFFBhavData
# from models.start_date_lookup import StartDateLookup
# from user_agents_list import user_agents
# import time
# from constants import mst_table_mapping, tables_to_get
# from analyze_trade_quantity_with_rs import main as all_data_csv_main



# with next(get_db()) as _db_session:
#     start_dates: Dict[str, str] = {_date.end_date.strftime(
#         '%Y-%m-%d'): _date.start_date.strftime('%Y-%m-%d') for _date in _db_session.query(StartDateLookup).all()}




# def add_fo_udiff_bhavcopy_to_db(self, file: Path, verify_in_db: bool = True):
#         file_name = file.name.split('/')[-1]
#         file_date = datetime.strptime(file_name.replace('BhavCopy_NSE_FO_STF', '').replace('_F_0000.csv', ''),
#                                       '%Y%m%d')
#         if file_date <= datetime(2025, 1, 1):
#             csv_data = pd.read_csv(
#                 file,
#                 parse_dates=["TradDt", "BizDt",
#                              "XpryDt", "FininstrmActlXpryDt"],
#                 usecols=['TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN',
#                          'TckrSymb', 'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric',
#                          'OptnTp', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric',
#                          'LastPric', 'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'OpnIntrst',
#                          'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd',
#                          'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd01', 'Rsvd02', 'Rsvd03', 'Rsvd04'])
#             csv_data.rename(columns={'TradDt': 'trade_date',
#                                      'BizDt': 'biz_date',
#                                      'Sgmt': 'sgmt',
#                                      'Src': 'src',
#                                      'FinInstrmTp': 'fin_instrm_tp',
#                                      'FinInstrmId': 'fin_instrm_id',
#                                      'ISIN': 'isin',
#                                      'TckrSymb': 'tckr_symb',
#                                      'SctySrs': 'scty_srs',
#                                      'XpryDt': 'xpry_date',
#                                      'FininstrmActlXpryDt': 'fininstrm_actl_xpry_date',
#                                      'StrkPric': 'strk_price',
#                                      'OptnTp': 'optn_tp',
#                                      'FinInstrmNm': 'fin_instrm_nm',
#                                      'OpnPric': 'open_price',
#                                      'HghPric': 'high_price',
#                                      'LwPric': 'low_price',
#                                      'ClsPric': 'close_price',
#                                      'LastPric': 'last_price',
#                                      'PrvsClsgPric': 'prev_closing_price',
#                                      'UndrlygPric': 'undrlyg_price',
#                                      'SttlmPric': 'sttlm_price',
#                                      'OpnIntrst': 'opn_intrst',
#                                      'ChngInOpnIntrst': 'chng_in_opn_intrst',
#                                      'TtlTradgVol': 'total_trade_volume',
#                                      'TtlTrfVal': 'ttl_trf_val',
#                                      'TtlNbOfTxsExctd': 'ttl_nb_of_txs_exctd',
#                                      'SsnId': 'ssn_id',
#                                      'NewBrdLotQty': 'new_brd_lot_qty',
#                                      'Rmks': 'rmks',
#                                      'Rsvd01': 'rsvd_01',
#                                      'Rsvd02': 'rsvd_02',
#                                      'Rsvd03': 'rsvd_03',
#                                      'Rsvd04': 'rsvd_04'
#                                      },
#                             inplace=True)
#         else:
#             csv_data = pd.read_csv(
#                 file,
#                 parse_dates=["TradDt", "BizDt",
#                              "XpryDt", "FininstrmActlXpryDt"],
#                 usecols=['TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN',
#                          'TckrSymb', 'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric',
#                          'OptnTp', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric',
#                          'LastPric', 'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'OpnIntrst',
#                          'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd',
#                          'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd1', 'Rsvd2', 'Rsvd3', 'Rsvd4'])
#             csv_data.rename(columns={'TradDt': 'trade_date',
#                                      'BizDt': 'biz_date',
#                                      'Sgmt': 'sgmt',
#                                      'Src': 'src',
#                                      'FinInstrmTp': 'fin_instrm_tp',
#                                      'FinInstrmId': 'fin_instrm_id',
#                                      'ISIN': 'isin',
#                                      'TckrSymb': 'tckr_symb',
#                                      'SctySrs': 'scty_srs',
#                                      'XpryDt': 'xpry_date',
#                                      'FininstrmActlXpryDt': 'fininstrm_actl_xpry_date',
#                                      'StrkPric': 'strk_price',
#                                      'OptnTp': 'optn_tp',
#                                      'FinInstrmNm': 'fin_instrm_nm',
#                                      'OpnPric': 'open_price',
#                                      'HghPric': 'high_price',
#                                      'LwPric': 'low_price',
#                                      'ClsPric': 'close_price',
#                                      'LastPric': 'last_price',
#                                      'PrvsClsgPric': 'prev_closing_price',
#                                      'UndrlygPric': 'undrlyg_price',
#                                      'SttlmPric': 'sttlm_price',
#                                      'OpnIntrst': 'opn_intrst',
#                                      'ChngInOpnIntrst': 'chng_in_opn_intrst',
#                                      'TtlTradgVol': 'total_trade_volume',
#                                      'TtlTrfVal': 'ttl_trf_val',
#                                      'TtlNbOfTxsExctd': 'ttl_nb_of_txs_exctd',
#                                      'SsnId': 'ssn_id',
#                                      'NewBrdLotQty': 'new_brd_lot_qty',
#                                      'Rmks': 'rmks',
#                                      'Rsvd1': 'rsvd_01',
#                                      'Rsvd2': 'rsvd_02',
#                                      'Rsvd3': 'rsvd_03',
#                                      'Rsvd4': 'rsvd_04'
#                                      },
#                             inplace=True)

#         with self.__get_db_session() as db_session:
#             for index, row in csv_data.iterrows():
#                 if row['fin_instrm_tp'] in ['STO', 'STF']:
#                     xpry_date = row['xpry_date'].strftime('%Y-%m-%d')
#                     s_date = start_dates.get(xpry_date)
#                     if not s_date:
#                         s_date = get_instrumet_start_day(xpry_date)
#                         db_session.add(StartDateLookup(
#                             end_date=xpry_date, start_date=s_date))
#                         start_dates[xpry_date] = s_date
#                     current_trade_date = row['trade_date'].strftime('%Y-%m-%d')
#                     fo_stock_intstruments_report = db_session.query(
#                         FOStockInstrumentsReport1).filter(FOStockInstrumentsReport1.name == row['fin_instrm_nm']).first()
#                     if fo_stock_intstruments_report:
#                         fo_stock_intstruments_report.current_date = current_trade_date

#                         fo_stock_intstruments_report.days_elapsed_since_birth = (datetime.strptime(current_trade_date, '%Y-%m-%d') - datetime.strptime(
#                             s_date, '%Y-%m-%d')).days + 1

#                         if fo_stock_intstruments_report.first_trade_date:
#                             fo_stock_intstruments_report.days_active = (datetime.strptime(current_trade_date, '%Y-%m-%d') - datetime.strptime(fo_stock_intstruments_report.first_trade_date.strftime(
#                                 '%Y-%m-%d'), '%Y-%m-%d')).days + 1

#                             fo_stock_intstruments_report.percentage_active = 100*fo_stock_intstruments_report.days_active / \
#                                 fo_stock_intstruments_report.days_elapsed_since_birth

#                             fo_stock_intstruments_report.latest_close_price = row['close_price']

#                             fo_stock_intstruments_report.latest_opn_intrst_lot = row[
#                                 'opn_intrst']/row['new_brd_lot_qty']

#                             fo_stock_intstruments_report.latest_chng_opn_intrst_lot = row[
#                                 'chng_in_opn_intrst']/row['new_brd_lot_qty']

#                         elif row['chng_in_opn_intrst'] > 0:
#                             fo_stock_intstruments_report.first_trade_date = current_trade_date

#                             fo_stock_intstruments_report.days_active = 1

#                             fo_stock_intstruments_report.percentage_active = 100 * fo_stock_intstruments_report.days_active / \
#                                 fo_stock_intstruments_report.days_elapsed_since_birth

#                             fo_stock_intstruments_report.first_trade_underlying_price = row[
#                                 'undrlyg_price']

#                             fo_stock_intstruments_report.first_trade_close_price = row['close_price']

#                             fo_stock_intstruments_report.latest_close_price = row['close_price']

#                             fo_stock_intstruments_report.latest_opn_intrst_lot = row[
#                                 'opn_intrst']/row['new_brd_lot_qty']

#                             fo_stock_intstruments_report.latest_chng_opn_intrst_lot = row[
#                                 'chng_in_opn_intrst']/row['new_brd_lot_qty']
#                         db_session.add(fo_stock_intstruments_report)

#                     else:
#                         report_data = {}
#                         # report_data['name'] = row['fin_instrm_nm']
#                         report_data['type'] = row['fin_instrm_tp']
#                         report_data['ticker_symbol'] = row['tckr_symb']
#                         # report_data['start_date'] = s_date
#                         report_data['expiry_date'] = xpry_date
#                         report_data['current_date'] = current_trade_date
#                         report_data['days_elapsed_since_birth'] = (datetime.strptime(current_trade_date, '%Y-%m-%d') - datetime.strptime(
#                             s_date, '%Y-%m-%d')).days + 1
#                         # if row['chng_in_opn_intrst'] > 0:
#                         #     report_data['first_trade_date'] = current_trade_date
#                         #     report_data['days_active'] = 1
#                         #     report_data['percentage_active'] = 100 * \
#                         #         report_data['days_active'] / \
#                         #         report_data['days_elapsed_since_birth']
#                         #     report_data['first_trade_underlying_price'] = row['undrlyg_price']
#                         #     report_data["first_trade_close_price"] = row['close_price']
#                         #     report_data["latest_close_price"] = row['close_price']
#                         #     report_data["latest_opn_intrst_lot"] = row[
#                         #         'opn_intrst']/row['new_brd_lot_qty']
#                         #     report_data["latest_chng_opn_intrst_lot"] = row[
#                         #         'chng_in_opn_intrst']/row['new_brd_lot_qty']
#                 #         db_session.add(
#                 #             FOStockInstrumentsReport1(**report_data))
#                 #     csv_data.at[index, 'start_date'] = s_date
#                 # else:
#                 #     csv_data.at[index, 'start_date'] = pd.NaT

#             db_session.commit()
#         if verify_in_db:
#             csv_data = csv_data.replace(np.nan, None)
#             # csv_data = csv_data.replace(pd.NaT, None)
#             csv_data = csv_data.replace([pd.NaT], [None])
#             self.add_data_to_db_helper(
#                 'fo_udiff_bhavdata_stf', csv_data)
#         else:
#             csv_data.to_sql('fo_udiff_bhavdata_stf', engine,
#                             if_exists='append', index=False)
#         file.unlink()
        
        
#         print(csv_data)
import json
from datetime import datetime
import numpy as np
import pandas as pd

from flask import Flask, make_response, request, jsonify
from flask_cors import CORS


from config import settings

from sqlalchemy import func, text

from flask_sqlalchemy import SQLAlchemy
# import talib


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



HOST = "0.0.0.0"
PORT = 9009

app = Flask(__name__)


CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = "mysql+pymysql://root:root@localhost:3306/testdb"
# TABLES = ['equity_bulks', 'equity_blocks', 'cm_udiff_bhavdata', 'last_updated_date',
#           'fo_udiff_bhavdata', 'securities_bhavdata', 'fo_stock_intstruments_report_1', 'start_date_lookup']

tracker_symbols_query = """SELECT  distinct tckr_symb
FROM fo_udiff_bhavdata;
"""

tracker_symbols = pd.read_sql(tracker_symbols_query, engine)

@app.route('/get-db-last-update-date', methods=['POST'])
def get_db_last_update_date():
    data = request.get_json()

    # Validate payload
    if not data or "secrete" not in data:
        return make_response(jsonify({"message": "Invalid Request: Missing 'secrete' key"}), 400)

    if data["secrete"] != settings.SECRET_KEY:
        return make_response(jsonify({"message": "Invalid Request: Incorrect 'secrete' key"}), 400)

    # Get table name and validate it
    table_name = data.get('table_name', 'last_updated_date')
    if table_name not in mst_table_mapping:
        return make_response(jsonify({"message": f"Invalid Request: Unknown table '{table_name}'"}), 400)

    try:
        # Retrieve last update date
        with next(get_db()) as db_session:
            date_column = mst_table_mapping[table_name]['date_column']

            # Safeguard for SQL Injection
            query = text(f"SELECT MAX(`{date_column}`) FROM `{table_name}`")
            last_updated = db_session.execute(query).scalar()

            return make_response(jsonify(
                {
                    "last_update_date": last_updated.strftime('%Y-%m-%d') if last_updated else None
                }
            ), 200)
    except Exception as e:
        # General error handling
        return make_response(jsonify({"message": f"Server Error: {str(e)}"}), 500)


@app.route('/get-report2', methods=['GET'])
def get_report2():
    """stock generator"""
    with next(get_db()) as db_session:
        _date = request.args.get('date', '')
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

        return app.response_class(
            response=resp,
            mimetype='application/json', status=200)
        
        
        
@app.route('/get-all-data-csv-report', methods=['GET'])
def get_all_data_csv_report():
    with next(get_db()) as db_session:
        _date = request.args.get('date', '')
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
    all_data_report_data = pd.read_sql(
        text("""
select * from all_data_csv_report where `date` = :data_date;
"""),
        engine,
        params={'data_date': last_updated})

    result = all_data_report_data.rename(columns={
        'symbol': 'SYMBOL',
        'date': 'DATE',
        'qnty_per_trade': 'qnty_per_trade',
        'avg_qnty_per_trade': 'avg_qnty_per_trade',
        'deliv_qty': 'deliv_qty',
        'avg_deliv_qty': 'avg_deliv_qty',
        'trade_qty_deviation': 'trade_qty_deviation',
        'delivery_qty_deviation': 'delivery_qty_deviation',
        'f_and_o': 'F&O',
        'nifty_500': 'NIFTY_500',
        'rs_current': 'RS_Current',
        'rs_7': 'RS_7',
        'rs_14': 'RS_14',
        'rs_7_minus_rs_14': 'RS7-RS14',
        'roc_rs_7': 'ROC_RS_7',
        'roc_rs_14': 'ROC_RS_14',
        'ad_count': 'AD_Count',
        'dd_count': 'DD_Count',
        'na_count': 'NA_Count',
        'vwap_positive_7days': 'vwap_positive_7days',
        'vwap_negative_7days': 'vwap_negative_7days',
        'vwap_positive_14days': 'vwap_positive_14days',
        'vwap_negative_14days': 'vwap_negative_14days',
    })
    result = result[[
        'SYMBOL',
        'DATE',
        'qnty_per_trade',
        'avg_qnty_per_trade',
        'deliv_qty',
        'avg_deliv_qty',
        'trade_qty_deviation',
        'delivery_qty_deviation',
        'F&O',
        'NIFTY_500',
        # 'RS_Current',
        # 'RS_7',
        # 'RS_14',
        # 'RS7-RS14',
        'ROC_RS_7',
        'ROC_RS_14',
        'AD_Count',
        'DD_Count',
        'NA_Count',
        'vwap_positive_7days',
        'vwap_negative_7days',
        'vwap_positive_14days',
        'vwap_negative_14days'
    ]]
    print(result[result['NSE CODE'] == 'ABB'])
    result.DATE = (
        result.DATE.apply(
            lambda x: x.strftime('%Y-%m-%d') if x else x))
    return app.response_class(
        response='{"data":' +
        result.to_json(orient="records", double_precision=6) +
        ', "date": "' + last_updated + '"}',
        mimetype='application/json', status=200)


if __name__ == '__main__':
    app.run(host=HOST, port=PORT, debug=True)

