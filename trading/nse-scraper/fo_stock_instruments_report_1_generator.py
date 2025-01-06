from datetime import datetime
import pandas as pd
from sqlalchemy import text
from db import engine, get_db
from models.fo_stock_intstruments_report_1 import FOStockInstrumentsReport1


financial_instrument_end_dates_query = """SELECT
    end_date as expiry_date
    FROM start_date_lookup 
    order by end_date desc;
    """
financial_instrument_end_date = pd.read_sql(
    financial_instrument_end_dates_query,
    engine)

# list_of_expiry_dates = financial_instrument_end_date.expiry_date.apply(
#     lambda x: x.strftime('%Y-%m-%d')).to_list()

list_of_expiry_dates = financial_instrument_end_date.expiry_date.to_list()

financial_instrument_names_query = """SELECT distinct
    tckr_symb as ticker_symbol
    FROM fo_udiff_bhavdata 
    order by ticker_symbol;
    """

financial_instrument_names = pd.read_sql(
    financial_instrument_names_query,
    engine)

list_of_ticker_symbols = financial_instrument_names.ticker_symbol.to_list()


if __name__ == "__main__":
    for x in list_of_ticker_symbols:
        for y in list_of_expiry_dates:
            financial_instrument_data_query = f"""
            WITH 
        FirstTradeData AS (
            SELECT
                fin_instrm_nm,
                MIN(trade_date) AS first_trade_date
            FROM
                fo_udiff_bhavdata
            WHERE
                chng_in_opn_intrst > 0
                and tckr_symb = "{x}" and xpry_date = "{y}"
            GROUP BY
                fin_instrm_nm
        ),
        PriceData AS (
            SELECT 
                fin_instrm_nm,
                trade_date,
                undrlyg_price,
                close_price,
                (opn_intrst/new_brd_lot_qty) AS opn_intrst_lot,
                (chng_in_opn_intrst/new_brd_lot_qty) AS chng_opn_intrst_lot
            FROM 
                fo_udiff_bhavdata
            
                WHERE tckr_symb = "{x}" and xpry_date = "{y}"
        )
        select * from (
        SELECT
            id.fin_instrm_nm as name,
            id.tckr_symb as ticker_symbol,
            id.fin_instrm_tp as type,
            id.instrm_start_date as start_date,
            id.instrm_current_date as 'current_date',
            id.instrm_xpry_date as expiry_date,
            id.DaysElapsedSinceBirth as days_elapsed_since_birth,
            ift.first_trade_date,
            DATEDIFF(id.instrm_current_date, ift.first_trade_date) AS days_active,
            (DATEDIFF(id.instrm_current_date, 
                ift.first_trade_date)*100/id.DaysElapsedSinceBirth) AS percentage_active,    
            p1.undrlyg_price AS first_trade_underlying_price,
            p1.close_price AS first_trade_close_price,
            p2.close_price AS latest_close_price,
            p2.opn_intrst_lot AS latest_opn_intrst_lot,
            p2.chng_opn_intrst_lot AS latest_chng_opn_intrst_lot
        FROM
            (
            SELECT
                fin_instrm_nm,
                MAX(fin_instrm_tp) AS fin_instrm_tp,
                MAX(tckr_symb) AS tckr_symb,
                start_date AS instrm_start_date,
                MAX(trade_date) AS instrm_current_date,
                MAX(xpry_date) AS instrm_xpry_date,
                DATEDIFF(MAX(trade_date), start_date) AS DaysElapsedSinceBirth
            FROM
                fo_udiff_bhavdata
                WHERE tckr_symb = "{x}" and xpry_date = "{y}"
            GROUP BY
                fin_instrm_nm, start_date
        ) id
        LEFT JOIN
            FirstTradeData ift ON id.fin_instrm_nm = ift.fin_instrm_nm
        LEFT JOIN
            PriceData p1 ON id.fin_instrm_nm = p1.fin_instrm_nm AND 
            ift.first_trade_date = p1.trade_date
        LEFT JOIN
            PriceData p2 ON id.fin_instrm_nm = p2.fin_instrm_nm AND 
            id.instrm_current_date = p2.trade_date
        ) x
            """

            financial_instrument_data = pd.read_sql(
                text(financial_instrument_data_query),
                engine,
                parse_dates=[
                    "start_date", "current_date", "expiry_date", "first_trade_date"])

            financial_instrument_data.sort_values('name')

            # financial_instrument_data.start_date = (
            #     financial_instrument_data.start_date.apply(
            #         lambda x: x.strftime('%Y-%m-%d') if x else x))
            # financial_instrument_data.current_date = (
            #     financial_instrument_data.current_date.apply(
            #         lambda x: x.strftime('%Y-%m-%d') if x else x))
            # financial_instrument_data.expiry_date = (
            #     financial_instrument_data.expiry_date.apply(
            #         lambda x: x.strftime('%Y-%m-%d') if x else x))
            # financial_instrument_data.first_trade_date = (
            #     financial_instrument_data.first_trade_date.apply(
            #         lambda x: x.strftime('%Y-%m-%d') if x else x))

            # response = financial_instrument_data.to_dict(orient="records")

            financial_instrument_data.to_sql(
                'fo_stock_intstruments_report_1', engine, if_exists='append', index=False)

            print(x, y)
            
            # with next(get_db()) as db_session:
            #     for data in response:
            #         try:
            #             db_session.add(
            #                 FOStockInstrumentsReport1(
            #                     **data
            #                 )
            #             )
            #             db_session.commit()
            #         except Exception as e:
            #             print(e)
            #             print(data)
            #             db_session.rollback()
