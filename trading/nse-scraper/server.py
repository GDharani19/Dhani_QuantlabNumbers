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


@app.route('/get-holidays', methods=['GET'])
def get_holidays_dates():
    holidays_query = """

    SELECT DATE_FORMAT(tradingDate, '%Y-%m-%d') as trading_date FROM `holidays` WHERE tradingDate > DATE_SUB(CURDATE(),INTERVAL 1 YEAR);

    """
    holidays_data = pd.read_sql(
        text(holidays_query),
        engine)

    return holidays_data.trading_date.to_list()


@app.route('/get-instruments-expiry-dates', methods=['GET'])
def get_instruments_expiry_dates():
    financial_instrument_end_dates_query = """SELECT
    DATE_FORMAT(end_date, '%Y-%m-%d') as expiry_date
    FROM start_date_lookup
    order by end_date desc limit 6;
    """
    financial_instrument_end_date = pd.read_sql(
        text(financial_instrument_end_dates_query),
        engine)

    return financial_instrument_end_date.expiry_date.to_list()


@app.route('/get-instruments-dates', methods=['POST'])
def get_instruments_dates():
    data = request.get_json()
    if "secrete" not in data:
        return make_response(jsonify({"message": "Invalid Request"}), 400)
    if data["secrete"] != settings.SECRET_KEY:
        return make_response(jsonify({"message": "Invalid Request"}), 400)
    financial_instrument_dates_query = """SELECT
    DATE_FORMAT(end_date, '%Y-%m-%d') as expiry_date,
    DATE_FORMAT(start_date, '%Y-%m-%d') as start_date
    FROM start_date_lookup;
    """
    financial_instrument_dates = pd.read_sql(
        text(financial_instrument_dates_query),
        engine)

    return app.response_class(
        response=financial_instrument_dates.to_json(orient="records"),
        mimetype='application/json', status=200)


@app.route('/get-ticker-symbols', methods=['GET'])
def get_ticker_symbols():
    financial_instrument_names_query = """SELECT distinct
    ticker_symbol
    FROM fo_stock_intstruments_report_1
    order by ticker_symbol;
    """

    financial_instrument_names = pd.read_sql(
        financial_instrument_names_query,
        engine)

    return financial_instrument_names.ticker_symbol.to_list()


list_of_holidays = [
    '22-Jan-2024', '26-Jan-2024', '08-Mar-2024',
    '25-Mar-2024', '29-Mar-2024', '11-Apr-2024',
    '14-Apr-2024', '17-Apr-2024', '21-Apr-2024',
    '01-May-2024', '20-May-2024', '17-Jun-2024',
    '17-Jul-2024', '15-Aug-2024', '07-Sep-2024',
    '02-Oct-2024', '12-Oct-2024', '01-Nov-2024',
    '02-Nov-2024', '15-Nov-2024', '25-Dec-2024',
    '26-Jan-2025',  
    '17-Mar-2025',  
    '11-Apr-2025',  
    '14-Apr-2025',  
    '01-May-2025',  
    '15-Aug-2025',  
    '02-Oct-2025',  
    '21-Oct-2025',  
    '31-Oct-2025',  
    '25-Dec-2025']   


@app.route('/get-instruments-data', methods=['POST'])
def get_instruments_data():
    input_data = request.get_json()
    ticker_symbol = input_data.get('ticker_symbol')
    fin_instr_tp = input_data.get('instrument_type')
    instrns_end_date = input_data.get('expiry_date')
    percent_active = input_data.get('percentage_active')
    params = {}
    where_clause = ""
    if ticker_symbol:
        if not isinstance(ticker_symbol, list):
            ticker_symbol = [ticker_symbol]
        place_holders = ", ".join([
            f":ticker_symbol_{idx}" for idx, _ in enumerate(ticker_symbol)
        ])
        params.update(
            {f"ticker_symbol_{idx}": val for idx,
                val in enumerate(ticker_symbol)}
        )
        where_clause = f" ticker_symbol IN ({place_holders})"
    if fin_instr_tp:
        if not isinstance(fin_instr_tp, list):
            fin_instr_tp = [fin_instr_tp]
    else:
        fin_instr_tp = ['STO', 'STF']

    place_holders = ", ".join([
        f":fin_instr_tp_{idx}" for idx, _ in enumerate(fin_instr_tp)
    ])
    params.update(
        {f"fin_instr_tp_{idx}": val for idx, val in enumerate(fin_instr_tp)}
    )
    if where_clause:
        where_clause = " and ".join(
            [where_clause, f" `type` IN ({place_holders})"])
    else:
        where_clause = f" `type` IN ({place_holders})"

    if instrns_end_date:
        if not isinstance(instrns_end_date, list):
            instrns_end_date = [instrns_end_date]

        place_holders = ", ".join([
            f":instrns_end_date_{idx}" for idx, _ in enumerate(
                instrns_end_date)
        ])
        params.update(
            {f"instrns_end_date_{idx}": val for idx,
                val in enumerate(instrns_end_date)}
        )
        if where_clause:
            where_clause = " and ".join(
                [where_clause, f" expiry_date IN ({place_holders})"])
        else:
            where_clause = f" expiry_date IN ({place_holders})"
    else:
        with next(get_db()) as db_session:
            end_date = db_session.execute(text("""SELECT
    DATE_FORMAT(end_date, '%Y-%m-%d') as expiry_date
    FROM start_date_lookup
    order by end_date desc limit 5,1;
    """)).scalar()
        place_holders = ", ".join([
            f":instrns_end_date_{idx}" for idx, _ in enumerate(
                [end_date])
        ])
        params.update(
            {f"instrns_end_date_{idx}": val for idx,
                val in enumerate([end_date])}
        )
        if where_clause:
            where_clause = " and ".join(
                [where_clause, f" expiry_date >= ({place_holders})"])
        else:
            where_clause = f" expiry_date >= ({place_holders})"

    if percent_active:
        params.update(
            {"percentage_active": percent_active})
        if where_clause:
            where_clause = " and ".join(
                [where_clause, f" percentage_active >= {percent_active}"])
        else:
            where_clause = f" percentage_active >= {percent_active}"

    fo_stock_intstruments_report_1_query = f"""
    select
        name,
        `type`,
        ticker_symbol,
        DATE_FORMAT(start_date, '%Y-%m-%d') as start_date,
        DATE_FORMAT(expiry_date, '%Y-%m-%d') as expiry_date,
        DATE_FORMAT(`current_date`, '%Y-%m-%d') as `current_date`,
        DATE_FORMAT(first_trade_date, '%Y-%m-%d') as first_trade_date,
        days_elapsed_since_birth,
        days_active,
        percentage_active,
        first_trade_underlying_price,
        first_trade_close_price,
        latest_close_price,
        latest_opn_intrst_lot,
        latest_chng_opn_intrst_lot
        from fo_stock_intstruments_report_1 {'where' if where_clause else ''} {where_clause} order by name;
    """
    fo_stock_intstruments_report_1_data = pd.read_sql(
        text(fo_stock_intstruments_report_1_query),
        engine,
        params=params
    )

    # def get_instrumet_start_day(xpry_date):
    #     # Placeholder for the actual function logic
    #     return pd.Timestamp(xpry_date) - pd.DateOffset(days=30)

    def get_option_type(row):
        if row['type'] == 'STO':
            return row['name'][-2:]
        elif row['type'] == 'STF':
            return row['name'][-3:]
        else:
            return pd.NA

    def get_strike_price(row):
        if row['type'] == 'STO':
            return row['name'].replace(row['ticker_symbol'], '')[5:-2]
        else:
            return pd.NA
    fo_stock_intstruments_report_1_data['option_type'] = fo_stock_intstruments_report_1_data.apply(
        get_option_type, axis=1)
    fo_stock_intstruments_report_1_data['strike_price'] = fo_stock_intstruments_report_1_data.apply(
        get_strike_price, axis=1)

    # fo_stock_intstruments_report_1_data.sort_values('name')
    # fo_stock_intstruments_report_1_data = fo_stock_intstruments_report_1_data.replace([
    #                                                                                   pd.NaT], [None])

    return app.response_class(
        response=fo_stock_intstruments_report_1_data[[
            "name",
            "type",
            "option_type",
            "strike_price",
            "ticker_symbol",
            "start_date",
            "expiry_date",
            "current_date",
            "first_trade_date",
            "days_elapsed_since_birth",
            "days_active",
            "percentage_active",
            "first_trade_underlying_price",
            "first_trade_close_price",
            "latest_close_price",
            "latest_opn_intrst_lot",
            "latest_chng_opn_intrst_lot"
        ]].to_json(orient="records"),
        mimetype='application/json', status=200)


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    if "email" not in data or "password" not in data:
        return make_response(jsonify({"message": "Invalid Request"}), 400)
    if data["email"].lower() not in settings.USERS:
        return make_response(jsonify({"message": "User not found"}), 404)
    if data['password'] != settings.USERS.get(data["email"].lower()):
        return make_response(jsonify({"message": "Invalid Password"}), 401)

    return make_response(jsonify({
        "data": {
            "id": 1,
            "accessToken": "fsdfjdsgvnoasdnvdsovsdjvbajkBDIUFcnhsabiVsdv"},
        "message": "Login successful"

    }), 200)


@app.route('/update-db', methods=['POST'])
def update_db():
    data = request.get_json()
    if "secrete" not in data:
        return make_response(jsonify({"message": "Invalid Request"}), 400)
    if data["secrete"] != settings.SECRET_KEY:
        return make_response(jsonify({"message": "Invalid Request"}), 400)
    if data['table_name'] not in mst_table_mapping:
        return make_response(jsonify({"message": "Invalid Request"}), 400)
    table_name = data['table_name']
    new_data = data['new_data']

    if table_name == 'last_updated_date':
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

    elif table_name == 'fo_stock_intstruments_report_1':
        with next(get_db()) as db_session:
            for row in new_data:
                fo_stock_intstruments_report = db_session.query(
                    FOStockInstrumentsReport1).filter(FOStockInstrumentsReport1.name == row['name']).first()
                if fo_stock_intstruments_report:
                    fo_stock_intstruments_report.current_date = row['current_date']
                    fo_stock_intstruments_report.days_elapsed_since_birth = row[
                        'days_elapsed_since_birth']
                    if fo_stock_intstruments_report.first_trade_date:
                        fo_stock_intstruments_report.days_active = (datetime.strptime(row['current_date'], '%Y-%m-%d') - datetime.strptime(fo_stock_intstruments_report.first_trade_date.strftime(
                            '%Y-%m-%d'), '%Y-%m-%d')).days + 1
                        fo_stock_intstruments_report.percentage_active = 100*fo_stock_intstruments_report.days_active / \
                            fo_stock_intstruments_report.days_elapsed_since_birth
                        fo_stock_intstruments_report.latest_close_price = row['latest_close_price']
                        fo_stock_intstruments_report.latest_opn_intrst_lot = row[
                            'latest_opn_intrst_lot']
                        fo_stock_intstruments_report.latest_chng_opn_intrst_lot = row[
                            'latest_chng_opn_intrst_lot']
                    elif row.get('latest_chng_opn_intrst_lot'):
                        for key, value in row.items():
                            setattr(fo_stock_intstruments_report, key, value)
                    db_session.add(fo_stock_intstruments_report)
                else:
                    db_session.add(FOStockInstrumentsReport1(**row))
            db_session.commit()

    elif table_name in mst_table_mapping:
        mdl = mst_table_mapping[table_name]['model']
        clmns = mst_table_mapping[table_name]['filters']
        with next(get_db()) as db_session:
            if clmns:
                for row in new_data:
                    if not db_session.query(mdl).filter_by(**{k: v for k, v in row.items() if k in clmns}).first():
                        db_session.add(
                            mst_table_mapping[table_name]['model'](**row))
            else:
                for row in new_data:
                    if not db_session.query(mdl).filter_by(**row).first():
                        db_session.add(
                            mst_table_mapping[table_name]['model'](**row))
            db_session.commit()
            db_session.close()

    return make_response(jsonify({"message": "successful"}), 200)


# @app.route('/get-db-last-update-date', methods=['POST'])
# def get_db_last_update_date():
#     data = request.get_json()
#     table_name = data.get('table_name')
#     if "secrete" not in data:
#         return make_response(jsonify({"message": "Invalid Request"}), 400)
#     if data["secrete"] != settings.SECRET_KEY:
#         return make_response(jsonify({"message": "Invalid Request"}), 400)
#     if table_name and table_name not in mst_table_mapping:
#         return make_response(jsonify({"message": "Invalid Request"}), 400)
#     with next(get_db()) as db_session:
#         if not table_name:
#             table_name = 'last_updated_date'
#         date_column = mst_table_mapping[table_name]['date_column']
#         last_updated = db_session.execute(
#             text(f'select max(`{date_column}`) from {table_name}')).scalar()
#         return make_response(jsonify(
#             {
#                 "last_update_date": last_updated.strftime('%Y-%m-%d') if last_updated else None
#             }
#         ), 200)

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


@app.route('/get-report3', methods=['GET'])
def get_report3():
    """initial series stock oi increase"""
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
        v4 = last_updated

        financial_instrument_end_dates_query = f"""SELECT
        DATE_FORMAT(start_date, '%Y-%m-%d') as start_date
        FROM start_date_lookup where start_date < '{v4}'
        order by start_date desc limit 3;
        """
        financial_instrument_end_date = pd.read_sql(
            text(financial_instrument_end_dates_query),
            engine)

        v1, v2, v3 = financial_instrument_end_date.start_date.to_list()

        js = db_session.query(FOUDIFFBhavData).filter(
            FOUDIFFBhavData.trade_date == v1, FOUDIFFBhavData.fin_instrm_tp == 'STF').order_by(FOUDIFFBhavData.tckr_symb).all()

        js2 = db_session.query(FOUDIFFBhavData).filter(
            FOUDIFFBhavData.trade_date == v2, FOUDIFFBhavData.fin_instrm_tp == 'STF').order_by(FOUDIFFBhavData.tckr_symb).all()

        js3 = db_session.query(FOUDIFFBhavData).filter(
            FOUDIFFBhavData.trade_date == v3, FOUDIFFBhavData.fin_instrm_tp == 'STF').order_by(FOUDIFFBhavData.tckr_symb).all()

        js4 = db_session.query(FOUDIFFBhavData).filter(
            FOUDIFFBhavData.trade_date == v4, FOUDIFFBhavData.fin_instrm_tp == 'STF').order_by(FOUDIFFBhavData.tckr_symb).all()

        # dateForExpiry = "2024-07-25"
        dateForExpiry = db_session.execute(
            text("select DATE_FORMAT(min(end_date), '%Y-%m-%d') as end_date from start_date_lookup where end_date >= current_date()")).scalar()
        processed_bhavcopy_json = {}

        for row in js4:
            if row.tckr_symb not in processed_bhavcopy_json:
                processed_bhavcopy_json[row.tckr_symb] = {
                    'MONTH1_OI': 0, 'MONTH2_OI': 0, 'MONTH3_OI': 0, 'CURRENT_OI': 0, 'MONTH1_OI_LOTS': 0, 'MONTH2_OI_LOTS': 0, 'MONTH3_OI_LOTS': 0, 'CURRENT_OI_LOTS': 0}
                # print("EXPIRY_DT",row.xpry_date)
                # print("dateForExpiry",dateForExpiry)
                if row.fin_instrm_tp == 'STF' and str(row.xpry_date) == dateForExpiry.lower():
                    processed_bhavcopy_json[row.tckr_symb
                                            ]['CURRENT_OI'] = int(row.opn_intrst)
                    processed_bhavcopy_json[row.tckr_symb
                                            ]['CURRENT_OI_LOTS'] = int(row.opn_intrst)/row.new_brd_lot_qty
            else:
                if row.fin_instrm_tp == 'STF' and str(row.xpry_date) == dateForExpiry.lower():
                    processed_bhavcopy_json[row.tckr_symb
                                            ]['CURRENT_OI'] = int(row.opn_intrst)
                    processed_bhavcopy_json[row.tckr_symb
                                            ]['CURRENT_OI_LOTS'] = int(row.opn_intrst)/row.new_brd_lot_qty

        for row in js:
            if row.tckr_symb not in processed_bhavcopy_json:
                processed_bhavcopy_json[row.tckr_symb] = {
                    'MONTH1_OI': 0, 'MONTH2_OI': 0, 'MONTH3_OI': 0, 'CURRENT_OI': 0, 'MONTH1_OI_LOTS': 0, 'MONTH2_OI_LOTS': 0, 'MONTH3_OI_LOTS': 0, 'CURRENT_OI_LOTS': 0}
            if row.fin_instrm_tp == 'STF':
                processed_bhavcopy_json[row.tckr_symb
                                        ]['MONTH1_OI'] += float(row.opn_intrst)
                processed_bhavcopy_json[row.tckr_symb
                                        ]['MONTH1_OI_LOTS'] += float(row.opn_intrst)/row.new_brd_lot_qty

        for row in js2:
            if row.tckr_symb not in processed_bhavcopy_json:
                processed_bhavcopy_json[row.tckr_symb] = {
                    'MONTH1_OI': 0, 'MONTH2_OI': 0, 'MONTH3_OI': 0, 'CURRENT_OI': 0, 'MONTH1_OI_LOTS': 0, 'MONTH2_OI_LOTS': 0, 'MONTH3_OI_LOTS': 0, 'CURRENT_OI_LOTS': 0}
            if row.fin_instrm_tp == 'STF':
                processed_bhavcopy_json[row.tckr_symb
                                        ]['MONTH2_OI'] += float(row.opn_intrst)
                processed_bhavcopy_json[row.tckr_symb
                                        ]['MONTH2_OI_LOTS'] += float(row.opn_intrst)/row.new_brd_lot_qty

        for row in js3:
            if row.tckr_symb not in processed_bhavcopy_json:
                processed_bhavcopy_json[row.tckr_symb] = {
                    'MONTH1_OI': 0, 'MONTH2_OI': 0, 'MONTH3_OI': 0, 'CURRENT_OI': 0, 'MONTH1_OI_LOTS': 0, 'MONTH2_OI_LOTS': 0, 'MONTH3_OI_LOTS': 0, 'CURRENT_OI_LOTS': 0}
            if row.fin_instrm_tp == 'STF':
                processed_bhavcopy_json[row.tckr_symb
                                        ]['MONTH3_OI'] += float(row.opn_intrst)
                processed_bhavcopy_json[row.tckr_symb
                                        ]['MONTH3_OI_LOTS'] += float(row.opn_intrst)/row.new_brd_lot_qty
        # print(processed_bhavcopy_json)
        resp = []
        output = processed_bhavcopy_json
        for key in processed_bhavcopy_json:
            if key in ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTY"]:
                # print("SKIPPED",key)
                continue
            if output[key]["MONTH3_OI"] == 0 or output[key]["MONTH2_OI"] == 0 or output[key]["MONTH1_OI"] == 0:
                print("SKIPPED", key)
                continue

            max_val = max([output[key]["MONTH3_OI"], output[key]["MONTH2_OI"],
                          output[key]["MONTH1_OI"], output[key]["CURRENT_OI"]])
            max_lots_val = max([output[key]["MONTH3_OI_LOTS"], output[key]["MONTH2_OI_LOTS"],
                                output[key]["MONTH1_OI_LOTS"], output[key]["CURRENT_OI_LOTS"]])
            avg_val = int(int(output[key]["MONTH3_OI"] + output[key]
                              ["MONTH2_OI"] + output[key]["MONTH1_OI"])/3)
            avg_lots_val = int(int(output[key]["MONTH3_OI_LOTS"] + output[key]
                                   ["MONTH2_OI_LOTS"] + output[key]["MONTH1_OI_LOTS"])/3)
            avgtomax = 0 if avg_val == 0 else round(
                float((max_val-avg_val)/avg_val), 6)
            # avgtomax_lots = 0 if avg_lots_val == 0 else round(
            #     float((max_lots_val-avg_lots_val)/avg_lots_val), 6)
            keys = [
                "Symbol",
                datetime.strptime(v3, '%Y-%m-%d').strftime('%d-%m-%Y')+' OI',
                datetime.strptime(v2, '%Y-%m-%d').strftime('%d-%m-%Y')+' OI',
                datetime.strptime(v1, '%Y-%m-%d').strftime('%d-%m-%Y')+' OI',
                datetime.strptime(v4, '%Y-%m-%d').strftime('%d-%m-%Y')+' OI',
                'MAX',
                'CURRENT OI IS MAX ?',
                'AVG',
                'AVG TO MAX',
                'PRESENT TO LAST MONTH',
                datetime.strptime(
                    v3, '%Y-%m-%d').strftime('%d-%m-%Y')+' OI(LOTS)',
                datetime.strptime(
                    v2, '%Y-%m-%d').strftime('%d-%m-%Y')+' OI(LOTS)',
                datetime.strptime(
                    v1, '%Y-%m-%d').strftime('%d-%m-%Y')+' OI(LOTS)',
                datetime.strptime(
                    v4, '%Y-%m-%d').strftime('%d-%m-%Y')+' OI(LOTS)',
                'MAX(LOTS)',
                # 'CURRENT OI(LOTS) IS MAX(LOTS) ?',
                'AVG(LOTS)',
                # 'AVG(LOTS) TO MAX(LOTS)',
                # 'PRESENT TO LAST MONTH(LOTS)'
            ]
            resp.append({
                keys[0]: key,
                keys[1]: output[key]["MONTH3_OI"],
                keys[2]: output[key]["MONTH2_OI"],
                keys[3]: output[key]["MONTH1_OI"],
                keys[4]: output[key]["CURRENT_OI"],
                keys[5]: max_val,
                keys[6]: "YES" if max_val == output[key]["CURRENT_OI"] else "NO",
                keys[7]: avg_val,
                keys[8]: avgtomax,
                keys[9]: 0 if output[key]["MONTH1_OI"] == 0 else round(
                    float(output[key]["CURRENT_OI"]/output[key]["MONTH1_OI"]), 6),
                keys[10]: output[key]["MONTH3_OI_LOTS"],
                keys[11]: output[key]["MONTH2_OI_LOTS"],
                keys[12]: output[key]["MONTH1_OI_LOTS"],
                keys[13]: output[key]["CURRENT_OI_LOTS"],

                keys[14]: max_lots_val,
                # keys[15]: "YES" if max_lots_val == output[key]["CURRENT_OI_LOTS"] else "NO",
                keys[15]: avg_lots_val,
                # keys[17]: avgtomax_lots,
                # keys[18]: 0 if output[key]["MONTH1_OI_LOTS"] == 0 else round(
                #     float(output[key]["CURRENT_OI_LOTS"]/output[key]["MONTH1_OI_LOTS"]), 6),

            })

        resp = json.dumps({'data': resp, 'date': v4})

        return app.response_class(
            response=resp,
            mimetype='application/json', status=200)

    # return app.response_class(
    #         response=resp,
    #         mimetype='application/json', status=200)


@app.route('/get-report4', methods=['GET'])
def get_report4():
    # Define the benchmark index
    benchmark = 'Nifty 50'
    # adaptive_window params
    min_period = 5
    max_period = 30
    # initial rolling window
    window = 20

    def adaptive_window(volatility, min_period=5, max_period=30):
        return min_period + (max_period - min_period) * (volatility - volatility.min()) / (
            volatility.max() - volatility.min()
        )

    market_data_query = """select * from cm_market_data_indexes  where `index` in :a and `date` >= "2023-01-01" order by `date`, `index`"""
    df = pd.read_sql(
        text(market_data_query),
        engine,
        params={'a': INDEX_FOR_ADOPTIVE_EMA_CALCULATION}
    )
    # Filter data for the benchmark index
    benchmark_df = df[df['index'] == benchmark].copy()

    # Calculate rolling standard deviation (Benchmark Volatility)
    benchmark_df['rolling_std'] = benchmark_df['percentage_change'].rolling(
        window=window).std()
    # Calculate the min and max volatility for scaling
    min_volatility = benchmark_df['rolling_std'].min()
    max_volatility = benchmark_df['rolling_std'].max()

    def adaptive_window(volatility, min_vol, max_vol, min_period, max_period):
        scaled_volatility = (volatility - min_vol) / (max_vol - min_vol)
        adaptive_window = min_period + \
            (max_period - min_period) * (1 - scaled_volatility)
        return adaptive_window

    # Apply the function to get the adaptive window period
    benchmark_df['adaptive_window'] = benchmark_df['rolling_std'].apply(
        adaptive_window, args=(min_volatility, max_volatility, min_period, max_period))
    benchmark_df['adaptive_window'] = benchmark_df['adaptive_window'].fillna(
        5).round().astype(int)

    # Merge the adaptive window back to the original dataframe
    df = df.merge(
        benchmark_df[['date', 'adaptive_window']], on='date', how='left')

    # Calculate the exponential moving average (EMA) with adaptive window period for each index
    df['adaptive_ema'] = np.nan

    df['rolling_std'] = df.groupby('index')['percentage_change'].rolling(
        window=window).std().reset_index(level=0, drop=True)

    for idx in df['index'].unique():
        if idx != benchmark:
            index_df = df[df['index'] == idx]
            ema_values = []
            for i in range(len(index_df)):
                window_size = index_df['adaptive_window'].iloc[i]
                if i >= window_size - 1:
                    ema = talib.EMA(index_df['percentage_change'].iloc[i -
                                    window_size + 1:i + 1].values, timeperiod=window_size)[-1]
                    ema_values.append(ema)
                else:
                    ema_values.append(np.nan)
            df.loc[df['index'] == idx, 'adaptive_ema'] = ema_values
    df.date = (df.date.apply(
        lambda x: x.strftime('%Y-%m-%d') if x else x))

    # df[['date', 'index', 'prev_closing_price', 'close_price', 'gain_or_loss', 'percentage_change', 'relative_performance_ratio',
    #     'adaptive_window', 'adaptive_ema', 'rolling_std']].to_csv('percentage_change_based_ema.csv', index=False, float_format='%g')

    ########################################################################

    # market_data_query = """select * from cm_market_data_indexes  where `index` in :a and `date` >= "2023-01-01" order by `date`, `index`"""
    # df = pd.read_sql(
    #     text(market_data_query),
    #     engine,
    #     params={'a': INDEX_FOR_ADOPTIVE_EMA_CALCULATION}
    # )
    # # Filter data for the benchmark index
    # benchmark_df = df[df['index'] == benchmark].copy()

    # # Calculate rolling standard deviation (Benchmark Volatility)
    # benchmark_df['rolling_std'] = benchmark_df['percentage_change'].rolling(
    #     window=window).std()
    # # Calculate the min and max volatility for scaling
    # min_volatility = benchmark_df['rolling_std'].min()
    # max_volatility = benchmark_df['rolling_std'].max()

    # def adaptive_window_1(volatility, min_vol, max_vol, min_period, max_period):
    #     scaled_volatility = (volatility - min_vol) / (max_vol - min_vol)
    #     adaptive_window = min_period + \
    #         (max_period - min_period) * (1 - scaled_volatility)
    #     return adaptive_window

    # # Apply the function to get the adaptive window period
    # benchmark_df['adaptive_window'] = benchmark_df['rolling_std'].apply(
    #     adaptive_window_1, args=(min_volatility, max_volatility, min_period, max_period))
    # benchmark_df['adaptive_window'] = benchmark_df['adaptive_window'].fillna(
    #     5).round().astype(int)

    # # Merge the adaptive window back to the original dataframe
    # df = df.merge(
    #     benchmark_df[['date', 'adaptive_window']], on='date', how='left')

    # # Calculate the exponential moving average (EMA) with adaptive window period for each index
    # df['adaptive_ema'] = np.nan

    # df['rolling_std'] = df.groupby('index')['relative_performance_ratio'].rolling(
    #     window=window).std().reset_index(level=0, drop=True)

    # for idx in df['index'].unique():
    #     if idx != benchmark:
    #         index_df = df[df['index'] == idx]
    #         ema_values = []
    #         for i in range(len(index_df)):
    #             window_size = index_df['adaptive_window'].iloc[i]
    #             if i >= window_size - 1:
    #                 ema = talib.EMA(index_df['relative_performance_ratio'].iloc[i -
    #                                 window_size + 1:i + 1].values, timeperiod=window_size)[-1]
    #                 ema_values.append(ema)
    #             else:
    #                 ema_values.append(np.nan)
    #         df.loc[df['index'] == idx, 'adaptive_ema'] = ema_values

    # df.date = (df.date.apply(
    #             lambda x: x.strftime('%Y-%m-%d') if x else x))

    # df[['date', 'index', 'prev_closing_price', 'close_price', 'gain_or_loss', 'percentage_change', 'relative_performance_ratio',
    #     'adaptive_window', 'adaptive_ema', 'rolling_std']].to_csv('rpr_based_ema.csv', index=False, float_format='%g')

    return app.response_class(
        response='{"data":'+df.to_json(
            orient="records", double_precision=6)+' }',
        mimetype='application/json', status=200)


@app.route('/get-report5', methods=['GET'])
def get_report5():
    fo_combine_oi_delta_equivalent_query = """
    select DATE_FORMAT(`date`, '%Y-%m-%d') as `date`,
    isin,
    script_name,
    symbol,
    open_interest,
    delta_equivalent_open_interest_contract_wise,
    delta_equivalent_open_interest_portfolio_wise from fo_combine_oi_delta_equivalent order by `date` desc, `symbol`;
    """
    fo_combine_oi_delta_equivalent_data = pd.read_sql(
        text(fo_combine_oi_delta_equivalent_query),
        engine
    )

    fo_combine_oi_delta_equivalent_data.rename(columns={'date': 'Date',
                                                        'isin': 'ISIN',
                                                        'script_name': 'Scrip Name',
                                                        'symbol': 'Symbol',
                                                        'open_interest': 'Open Interest',
                                                        'delta_equivalent_open_interest_contract_wise': 'Delta Equivalent Open Interest Contract wise',
                                                        'delta_equivalent_open_interest_portfolio_wise': 'Delta Equivalent Open Interest Portfolio wise'
                                                        }, inplace=True)

    return app.response_class(
        response='{"data":'+fo_combine_oi_delta_equivalent_data.to_json(
            orient="records", double_precision=6)+' }',
        mimetype='application/json', status=200)


# @app.route('/get-report6', methods=['GET'])
# def get_report6():

#     with next(get_db()) as db_session:

#         current_start_date = db_session.execute(
#             text("""SELECT start_date from start_date_lookup where start_date <= current_date() order by start_date desc limit 1;""")).scalar()

#         end_date = db_session.execute(
#             text("select end_date from start_date_lookup where end_date >= current_date() order by end_date asc limit 1;")).scalar()
#         # bhav_data = db_session.query(FOUDIFFBhavData).filter(
#         #     FOUDIFFBhavData.trade_date == current_start_date, FOUDIFFBhavData.fin_instrm_tp.in_(['STO', 'STF'])).order_by(FOUDIFFBhavData.tckr_symb).all()

#         # current_start_date, end_date = '2024-06-28', '2024-07-25'

#         bhav_data = pd.read_sql(
#             text(f"""
# select fin_instrm_tp,close_price,tckr_symb, strk_price from fo_udiff_bhavdata where trade_date = '{current_start_date}' and xpry_date = '{end_date}'and fin_instrm_tp in ('STO', 'STF') order by tckr_symb
# """),
#             engine)

#         symbl_data = pd.read_sql(
#             text("""
# select tckr_symb, company_name from ticker_info order by tckr_symb;
# """),
#             engine)

#         f_bhav_data = bhav_data[bhav_data['fin_instrm_tp']
#                                 == 'STF'][['tckr_symb', 'close_price']].copy()
#         o_bhav_data = bhav_data[bhav_data['fin_instrm_tp']
#                                 == 'STO'].copy()

#         f_bhav_data.rename(
#             columns={'close_price': 'fut_close_price'}, inplace=True)
#         print(f_bhav_data)
#         o_bhav_data = o_bhav_data.merge(
#             f_bhav_data, on='tckr_symb', how='left')
#         o_bhav_data['diff_from_fut'] = o_bhav_data['fut_close_price'] - \
#             o_bhav_data['strk_price']

#         grouped = o_bhav_data.groupby('tckr_symb')

#         # Find indices of the smallest positive value and largest negative value for each group
#         idx_min_positive = grouped.apply(
#             lambda x: x[x['diff_from_fut'] > 0]['diff_from_fut'].idxmin())
#         idx_max_negative = grouped.apply(
#             lambda x: x[x['diff_from_fut'] <= 0]['diff_from_fut'].idxmax())

#         # Merge the results with the original DataFrame to get the corresponding 'strick_price'
#         df_min_positive = o_bhav_data.loc[idx_min_positive].reset_index(
#             drop=True)
#         df_max_negative = o_bhav_data.loc[idx_max_negative].reset_index(
#             drop=True)

#         df_min_positive = df_min_positive.rename(
#             columns={'strk_price': 'atm_ce', 'diff_from_fut': 'smallest_positive', 'close_price': 'atm_ce_value'})

#         df_max_negative = df_max_negative.rename(
#             columns={'strk_price': 'atm_pe', 'diff_from_fut': 'largest_negative', 'close_price': 'atm_pe_value'})

#         # Merge the results into a single DataFrame
#         result = pd.merge(df_min_positive[['tckr_symb', 'smallest_positive', 'atm_ce', 'atm_ce_value']],
#                           df_max_negative[[
#                               'tckr_symb', 'largest_negative', 'atm_pe', 'atm_pe_value']],
#                           on='tckr_symb', how='outer')

#         result['UPPER_BAND_CONS'] = result['atm_ce'] + \
#             result['atm_ce_value'] + result['atm_pe_value']
#         result['LOWER_BAND_CONS'] = result['atm_pe'] - \
#             (result['atm_ce_value'] + result['atm_pe_value'])

#         result = pd.merge(
#             result, f_bhav_data[['tckr_symb', 'fut_close_price']], on='tckr_symb', how='left')

#         result = pd.merge(
#             result, symbl_data[['tckr_symb', 'company_name']], on='tckr_symb', how='left')

#         result['UPPER_BAND'] = result['fut_close_price'] * 1.06
#         result['LOWER_BAND'] = result['fut_close_price'] * 0.94

#         result['GOOGLE CODE'] = 'NSE:' + result['tckr_symb']
#         result['TICKER'] = result['company_name'] + \
#             ' (XNSE:' + result['tckr_symb'] + ')'
#         result = result.rename(columns={
#             'tckr_symb': 'NSE CODE',
#             'fut_close_price': 'Last Price',
#             'atm_ce': 'ATM CE',
#             'atm_pe': 'ATM PE',
#             'atm_ce_value': 'ATM CE VALUE',
#             'atm_pe_value': 'ATM PE VALUE',
#             'UPPER_BAND': 'UPPER BAND',
#             'LOWER_BAND': 'LOWER BAND',
#             'UPPER_BAND_CONS': 'UPPER BAND CONS',
#             'LOWER_BAND_CONS': 'LOWER BAND CONS'
#         })
#         result = result[[
#             'TICKER',
#             'NSE CODE',
#             'GOOGLE CODE',
#             'Last Price',
#             'ATM CE',
#             'ATM PE',
#             'ATM CE VALUE',
#             'ATM PE VALUE',
#             'UPPER BAND',
#             'LOWER BAND',
#             'UPPER BAND CONS',
#             'LOWER BAND CONS']]
#         # print(result[result['NSE CODE'] == 'ABB'])
#         # result.to_csv('output.csv', float_format='%g')
#     return app.response_class(
#         response='{"data":' +
#         result.to_json(orient="records", double_precision=6)+' }',
#         mimetype='application/json', status=200)


# @app.route('/get-report7', methods=['GET'])
# def get_report7():

#     with next(get_db()) as db_session:
#         current_start_date = request.args.get('date', '')
#         if current_start_date:
#             try:
#                 x = datetime.strptime(
#                     current_start_date, '%Y-%m-%d')
#                 if x >= datetime.now():
#                     return make_response(jsonify({"message": "Invalid Request"}), 400)
#             except:
#                 return make_response(jsonify({"message": "Invalid Request"}), 400)
#         else:
#             current_start_date = db_session.execute(
#                 text("select DATE_FORMAT(max(last_updated_date), '%Y-%m-%d') as last_updated_date from last_updated_date")).scalar()
#             if not current_start_date:
#                 current_start_date = db_session.execute(
#                     text("select DATE_FORMAT(max(`date`), '%Y-%m-%d') as last_updated_date from securities_bhavdata")).scalar()

#         # current_start_date = db_session.execute(
#         #     text("""SELECT start_date from start_date_lookup where start_date <= current_date() order by start_date desc limit 1;""")).scalar()

#         end_date = db_session.execute(
#             text("select end_date from start_date_lookup where end_date >= :current_start_date order by end_date asc limit 1;"), {'current_start_date': current_start_date}).scalar()
#         # bhav_data = db_session.query(FOUDIFFBhavData).filter(
#         #     FOUDIFFBhavData.trade_date == current_start_date, FOUDIFFBhavData.fin_instrm_tp.in_(['STO', 'STF'])).order_by(FOUDIFFBhavData.tckr_symb).all()

#         # current_start_date, end_date = '2024-06-28', '2024-07-25'

#         bhav_data = pd.read_sql(
#             text(f"""
# select fin_instrm_tp,close_price,tckr_symb, strk_price from fo_udiff_bhavdata where trade_date = '{current_start_date}' and xpry_date = '{end_date}'and fin_instrm_tp in ('STO', 'STF') order by tckr_symb
# """),
#             engine)

#         symbl_data = pd.read_sql(
#             text("""
# select tckr_symb, company_name from ticker_info order by tckr_symb;
# """),
#             engine)

#         f_bhav_data = bhav_data[bhav_data['fin_instrm_tp']
#                                 == 'STF'][['tckr_symb', 'close_price']].copy()
#         o_bhav_data = bhav_data[bhav_data['fin_instrm_tp']
#                                 == 'STO'].copy()

#         f_bhav_data.rename(
#             columns={'close_price': 'fut_close_price'}, inplace=True)
#         print(f_bhav_data)
#         o_bhav_data = o_bhav_data.merge(
#             f_bhav_data, on='tckr_symb', how='left')
#         o_bhav_data['diff_from_fut'] = o_bhav_data['fut_close_price'] - \
#             o_bhav_data['strk_price']

#         grouped = o_bhav_data.groupby('tckr_symb')

#         # Find indices of the smallest positive value and largest negative value for each group
#         idx_min_positive = grouped.apply(
#             lambda x: x[x['diff_from_fut'] > 0]['diff_from_fut'].idxmin())
#         idx_max_negative = grouped.apply(
#             lambda x: x[x['diff_from_fut'] <= 0]['diff_from_fut'].idxmax())

#         # Merge the results with the original DataFrame to get the corresponding 'strick_price'
#         df_min_positive = o_bhav_data.loc[idx_min_positive].reset_index(
#             drop=True)
#         df_max_negative = o_bhav_data.loc[idx_max_negative].reset_index(
#             drop=True)

#         df_min_positive = df_min_positive.rename(
#             columns={'strk_price': 'atm_ce', 'diff_from_fut': 'smallest_positive', 'close_price': 'atm_ce_value'})

#         df_max_negative = df_max_negative.rename(
#             columns={'strk_price': 'atm_pe', 'diff_from_fut': 'largest_negative', 'close_price': 'atm_pe_value'})

#         # Merge the results into a single DataFrame
#         result = pd.merge(df_min_positive[['tckr_symb', 'smallest_positive', 'atm_ce', 'atm_ce_value']],
#                           df_max_negative[[
#                               'tckr_symb', 'largest_negative', 'atm_pe', 'atm_pe_value']],
#                           on='tckr_symb', how='outer')

#         result['UPPER_BAND_CONS'] = result['atm_ce'] + \
#             result['atm_ce_value'] + result['atm_pe_value']
#         result['LOWER_BAND_CONS'] = result['atm_pe'] - \
#             (result['atm_ce_value'] + result['atm_pe_value'])

#         result = pd.merge(
#             result, f_bhav_data[['tckr_symb', 'fut_close_price']], on='tckr_symb', how='left')

#         result = pd.merge(
#             result, symbl_data[['tckr_symb', 'company_name']], on='tckr_symb', how='left')

#         result['UPPER_BAND'] = result['fut_close_price'] * 1.06
#         result['LOWER_BAND'] = result['fut_close_price'] * 0.94

#         result['GOOGLE CODE'] = 'NSE:' + result['tckr_symb']
#         result['TICKER'] = result['company_name'] + \
#             ' (XNSE:' + result['tckr_symb'] + ')'
#         result = result.rename(columns={
#             'tckr_symb': 'NSE CODE',
#             'fut_close_price': 'Last Price',
#             'atm_ce': 'ATM CE',
#             'atm_pe': 'ATM PE',
#             'atm_ce_value': 'ATM CE VALUE',
#             'atm_pe_value': 'ATM PE VALUE',
#             'UPPER_BAND': 'UPPER BAND',
#             'LOWER_BAND': 'LOWER BAND',
#             'UPPER_BAND_CONS': 'UPPER BAND CONS',
#             'LOWER_BAND_CONS': 'LOWER BAND CONS'
#         })
#         result = result[[
#             'TICKER',
#             'NSE CODE',
#             'GOOGLE CODE',
#             'Last Price',
#             'ATM CE',
#             'ATM PE',
#             'ATM CE VALUE',
#             'ATM PE VALUE',
#             'UPPER BAND',
#             'LOWER BAND',
#             'UPPER BAND CONS',
#             'LOWER BAND CONS']]
#         # print(result[result['NSE CODE'] == 'ABB'])
#     return app.response_class(
#         response='{"data":' +
#         result.to_json(orient="records", double_precision=6) +
#         ', "date": "' + current_start_date + '"}',
#         mimetype='application/json', status=200)


@app.route('/get-report6', methods=['GET'])
def get_report6():

    with next(get_db()) as db_session:

        current_start_date = db_session.execute(
            text("""SELECT DATE_FORMAT((`start_date`), '%Y-%m-%d') start_date from start_date_lookup where start_date <= current_date() order by start_date desc limit 1;""")).scalar()

        end_date = db_session.execute(
            text("select DATE_FORMAT((`end_date`), '%Y-%m-%d') end_date from start_date_lookup where end_date >= current_date() order by end_date asc limit 1;")).scalar()
        # bhav_data = db_session.query(FOUDIFFBhavData).filter(
        #     FOUDIFFBhavData.trade_date == current_start_date, FOUDIFFBhavData.fin_instrm_tp.in_(['STO', 'STF'])).order_by(FOUDIFFBhavData.tckr_symb).all()

        # current_start_date, end_date = '2024-06-28', '2024-07-25'

        bhav_data = pd.read_sql(
            text(f"""
select fin_instrm_tp,close_price,tckr_symb,optn_tp, strk_price from fo_udiff_bhavdata where trade_date = '{current_start_date}' and xpry_date = '{end_date}'and fin_instrm_tp in ('STO', 'STF') order by tckr_symb asc,close_price asc
"""),
            engine)

        symbl_data = pd.read_sql(
            text("""
select tckr_symb, company_name from ticker_info order by tckr_symb;
"""),
            engine)

        f_bhav_data = bhav_data[bhav_data['fin_instrm_tp']
                                == 'STF'][['tckr_symb', 'close_price']].copy()
        o_bhav_data = bhav_data[bhav_data['fin_instrm_tp']
                                == 'STO'].copy()

        ce_o_bhav_data = o_bhav_data[o_bhav_data['optn_tp'] == 'CE']
        pe_o_bhav_data = o_bhav_data[o_bhav_data['optn_tp'] == 'PE']

        o_bhav_data = pd.merge(ce_o_bhav_data, pe_o_bhav_data,
                               on=['tckr_symb', 'strk_price'],
                               how='outer',
                               suffixes=('_ce', '_pe'))

        f_bhav_data.rename(
            columns={'close_price': 'fut_close_price'}, inplace=True)

        o_bhav_data = o_bhav_data.merge(
            f_bhav_data, on='tckr_symb', how='left')

        def closest_row(group):
            # Absolute difference
            group['Diff'] = (group['fut_close_price'] -
                             group['strk_price']).abs()
            group['Diff_sign'] = (group['fut_close_price'] -
                                  group['strk_price'])
            min_diff = group['Diff'].min()  # Minimum difference
            # Get candidates with minimum difference
            candidates = group[group['Diff'] == min_diff]
            # If there's a tie, choose the row with the closest positive value
            return candidates.loc[candidates['fut_close_price'] - candidates['strk_price'] <= 0].head(1) if not candidates[candidates['fut_close_price'] - candidates['strk_price'] <= 0].empty else candidates.head(1)

        result = o_bhav_data.groupby('tckr_symb').apply(
            closest_row).reset_index(drop=True)

        result = result.rename(
            columns={'strk_price': 'atm_strike', 'diff_from_fut': 'smallest_positive'})

        result['UPPER_BAND_CONS'] = result['atm_strike'] + \
            result['close_price_ce'] + result['close_price_pe']
        result['LOWER_BAND_CONS'] = result['atm_strike'] - \
            (result['close_price_ce'] + result['close_price_pe'])

        result = pd.merge(
            result, symbl_data[['tckr_symb', 'company_name']], on='tckr_symb', how='left')

        result['GOOGLE CODE'] = 'NSE:' + result['tckr_symb']
        # result['TICKER'] = result['company_name'] + \
        #     ' (XNSE:' + result['tckr_symb'] + ')'
        
        
        result['TICKER'] = 'XNSE:' + result['tckr_symb']
        result['combined_premium'] = (result["close_price_ce"] + result["close_price_pe"]).round(2)
        result['percent_premium'] = ((result['combined_premium'] / result['fut_close_price']) *100).round(2)
        result['percent_premium'] = result['percent_premium'].astype('str') + '%'
            
            
            
        result = result.rename(columns={
            'tckr_symb': 'NSE CODE',
            'fut_close_price': 'LAST PRICE',
            'atm_strike': 'ATM STRIKE',
            'close_price_ce': 'ATM CE VALUE',
            'close_price_pe': 'ATM PE VALUE',
            'UPPER_BAND_CONS': 'UPPER BAND CONS',
            'LOWER_BAND_CONS': 'LOWER BAND CONS',
            'combined_premium': 'COMBINED PREMIUM',
            'percent_premium': '% PREMIUM',
        })
        result = result[[
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
            '% PREMIUM']]
        
        
    return app.response_class(
        response='{"data":' +
        result.to_json(orient="records", double_precision=6) +
        ', "date": "' + current_start_date + '"}',
        mimetype='application/json', status=200)


@app.route('/get-report7', methods=['GET'])
def get_report7():

    with next(get_db()) as db_session:
        current_start_date = request.args.get('date', '')
        if current_start_date:
            try:
                x = datetime.strptime(
                    current_start_date, '%Y-%m-%d')
                if x >= datetime.now():
                    return make_response(jsonify({"message": "Invalid Request"}), 400)
            except:
                return make_response(jsonify({"message": "Invalid Request"}), 400)
        else:
            current_start_date = db_session.execute(
                text("select DATE_FORMAT(max(last_updated_date), '%Y-%m-%d') as last_updated_date from last_updated_date")).scalar()
            if not current_start_date:
                current_start_date = db_session.execute(
                    text("select DATE_FORMAT(max(`date`), '%Y-%m-%d') as last_updated_date from securities_bhavdata")).scalar()

        # current_start_date = db_session.execute(
        #     text("""SELECT start_date from start_date_lookup where start_date <= current_date() order by start_date desc limit 1;""")).scalar()

        end_date = db_session.execute(
            text("select end_date from start_date_lookup where end_date >= :current_start_date order by end_date asc limit 1;"), {'current_start_date': current_start_date}).scalar()
        bhav_data = pd.read_sql(
            text(f"""
select fin_instrm_tp,close_price,tckr_symb,optn_tp, strk_price from fo_udiff_bhavdata where trade_date = '{current_start_date}' and xpry_date = '{end_date}'and fin_instrm_tp in ('STO', 'STF') order by tckr_symb asc,close_price asc
"""),
            engine)

        symbl_data = pd.read_sql(
            text("""
select tckr_symb, company_name from ticker_info order by tckr_symb;
"""),
            engine)

        f_bhav_data = bhav_data[bhav_data['fin_instrm_tp']
                                == 'STF'][['tckr_symb', 'close_price']].copy()
        o_bhav_data = bhav_data[bhav_data['fin_instrm_tp']
                                == 'STO'].copy()
        ce_o_bhav_data = o_bhav_data[o_bhav_data['optn_tp'] == 'CE']
        pe_o_bhav_data = o_bhav_data[o_bhav_data['optn_tp'] == 'PE']

        o_bhav_data = pd.merge(ce_o_bhav_data, pe_o_bhav_data,
                               on=['tckr_symb', 'strk_price'],
                               how='outer',
                               suffixes=('_ce', '_pe'))
        f_bhav_data.rename(
            columns={'close_price': 'fut_close_price'}, inplace=True)
        o_bhav_data = o_bhav_data.merge(
            f_bhav_data, on='tckr_symb', how='left')

        o_bhav_data['diff_from_fut'] = (o_bhav_data['fut_close_price'] -
                                        o_bhav_data['strk_price']).abs()

        # grouped = o_bhav_data.groupby('tckr_symb')
        def find_min_row(group):
            min_diff = group['diff_from_fut'].min()
            filtered_group = group[group['diff_from_fut'] == min_diff]
            return filtered_group.loc[filtered_group['strk_price'].idxmin()]

        result = o_bhav_data.groupby('tckr_symb').apply(
            find_min_row).reset_index(drop=True)
        result = result.rename(
            columns={'strk_price': 'atm_strike', 'diff_from_fut': 'smallest_positive'})

        result['UPPER_BAND_CONS'] = result['atm_strike'] + \
            result['close_price_ce'] + result['close_price_pe']
        result['LOWER_BAND_CONS'] = result['atm_strike'] - \
            (result['close_price_ce'] + result['close_price_pe'])

        result = pd.merge(
            result, symbl_data[['tckr_symb', 'company_name']], on='tckr_symb', how='left')

        result['GOOGLE CODE'] = 'NSE:' + result['tckr_symb']

        # result['TICKER'] = result['company_name'] + \
        #     ' (XNSE:' + result['tckr_symb'] + ')'
        result['TICKER'] = 'XNSE:' + result['tckr_symb']
        result['combined_premium'] = (result["close_price_ce"] + result["close_price_pe"]).round(2)
        result['percent_premium'] = ((result['combined_premium'] / result['fut_close_price']) *100).round(2)
        result['percent_premium'] = result['percent_premium'].astype('str') + '%'
        
        result = result.rename(columns={
            'tckr_symb': 'NSE CODE',
            'fut_close_price': 'LAST PRICE',
            'atm_strike': 'ATM STRIKE',
            'close_price_ce': 'ATM CE VALUE',
            'close_price_pe': 'ATM PE VALUE',
            'UPPER_BAND_CONS': 'UPPER BAND CONS',
            'LOWER_BAND_CONS': 'LOWER BAND CONS',
            'combined_premium': 'COMBINED PREMIUM',
            'percent_premium': '% PREMIUM',
        })
        result = result[[
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
            '% PREMIUM']]
    return app.response_class(
        response='{"data":' +
        result.to_json(orient="records", double_precision=6) +
        ', "date": "' + current_start_date + '"}',
        mimetype='application/json', status=200)


@app.route('/get-hedge-report', methods=['GET'])
def get_hedge_report():
    hedge_report_data = pd.read_sql(
        text("""
select * from hedge_report;
"""),
        engine)
    result = hedge_report_data.rename(columns={
        'idx': 'IDX',
        'date': 'Date',
        'week': 'Week',
        'day': 'Day',
        'series_month': 'Series Month',
        'future_close': 'Fut Cls',
        'futre_previous_close': 'Fut Prvs Cls',
        'spot_close': 'Spot Cls',
        'spot_previous_close': 'Spot Prvs cls',
        'ce_atm': 'CE ATM',
        'ce_close_price': 'CE Cls Prc',
        'ce_previous_close_price': 'CE PrvsClsgPric',
        'pe_atm': 'PE ATM',
        'pe_close_price': 'PE Cls Prc',
        'pe_previous_close_price': 'PE PrvsClsgPric',
        'ce_hedge_cost': 'CE Hedge Cost',
        'pe_hedge_cost': 'PE Hedge Cost'
    })
    result = result[[
        'IDX',
        'Date',
        'Week',
        'Day',
        'Series Month',
        'Fut Cls',
        'Fut Prvs Cls',
        'Spot Cls',
        'Spot Prvs cls',
        'CE ATM',
        'CE Cls Prc',
        'CE PrvsClsgPric',
        'PE ATM',
        'PE Cls Prc',
        'PE PrvsClsgPric',
        'CE Hedge Cost',
        'PE Hedge Cost'
    ]]
    # print(result[result['NSE CODE'] == 'ABB'])
    return app.response_class(
        response='{"data":' +
        result.to_json(orient="records", double_precision=6) + '}',
        mimetype='application/json', status=200)


@app.route('/get-synthetic-check-report', methods=['GET'])
def get_synthetic_check_report():
    synthetic_check_report_data = pd.read_sql(
        text("""
select * from synthetic_check;
"""),
        engine)
    result = synthetic_check_report_data.rename(columns={
        'idx': 'IDX',
        'trade_date': 'Trade Date',
        'options_expiry_date': 'Options XpryDt',
        'expiry_month': 'XpryMth',
        'spot_close': 'Spot Close',
        'future_close': 'Futures Close',
        'spot_ce_atm_strik': 'ATM CE Strike (based on Spot)',
        'spot_pe_atm_strik': 'ATM PE Strike (based on Spot)',
        'spot_ce_close_price': 'ATM CE Cls Pric (based on Spot)',
        'spot_pe_close_price': 'ATM PE Cls Pric (based on Spot)',
        'future_ce_atm_strik': 'ATM CE Strike (based on FUT)',
        'future_pe_atm_strik': 'ATM PE Strike (based on FUT)',
        'future_ce_close_price': 'ATM CE Cls Pric (based on FUT)',
        'future_pe_close_price': 'ATM PE Cls Pric (based on FUT)',
        'spot_synthetic': 'Synthetic based on Futures',
        'future_synthetic': 'Synthetic Check Futures',
        'spot_synthetic_check': 'Synthetic based on SPOT',
        'future_synthetic_check': 'Synthetic Check SPOT'
    })
    result = result[[
        'IDX',
        'Trade Date',
        'Options XpryDt',
        'XpryMth',
        'Spot Close',
        'Futures Close',
        'ATM CE Strike (based on Spot)',
        'ATM PE Strike (based on Spot)',
        'ATM CE Cls Pric (based on Spot)',
        'ATM PE Cls Pric (based on Spot)',
        'ATM CE Strike (based on FUT)',
        'ATM PE Strike (based on FUT)',
        'ATM CE Cls Pric (based on FUT)',
        'ATM PE Cls Pric (based on FUT)',
        'Synthetic based on Futures',
        'Synthetic Check Futures',
        'Synthetic based on SPOT',
        'Synthetic Check SPOT'
    ]]
    # print(result[result['NSE CODE'] == 'ABB'])
    return app.response_class(
        response='{"data":' +
        result.to_json(orient="records", double_precision=6) + '}',
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
    # print(result[result['NSE CODE'] == 'ABB'])
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
