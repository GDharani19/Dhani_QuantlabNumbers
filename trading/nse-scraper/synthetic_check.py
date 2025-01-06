from db import engine, get_db
from datetime import datetime, timedelta
from sqlalchemy import func, text
import pandas as pd
from pathlib import Path
import csv

from load_start_days import get_list_of_holidays
from models.synthetic_check import SyntheticCheck


def get_dates(start: str, end: str):
    holidays_query = """
    SELECT DATE_FORMAT(tradingDate, '%Y-%m-%d') as trading_date FROM `holidays`;
    """
    holidays_data = pd.read_sql(
        text(holidays_query),
        engine, parse_dates=['trading_date'],
        # date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d')
    )
    holidays = holidays_data.trading_date
    dates = pd.date_range(start=start,
                          end=end, freq='B')
    dates = dates[~dates.isin(holidays)]
    return dates


def next_thursday(given_date):
    # Days of the week: Monday is 0, Sunday is 6
    given_weekday = given_date.weekday()
    # Calculate the number of days until next Thursday
    days_until_thursday = (3 - given_weekday + 7) % 7
    if days_until_thursday == 0:
        days_until_thursday = 7
    # Calculate the next Thursday
    next_thursday_date = given_date + timedelta(days=days_until_thursday)
    return next_thursday_date


def next_wednesday(given_date):
    # Days of the week: Monday is 0, Sunday is 6
    given_weekday = given_date.weekday()
    # Calculate the number of days until next Thursday
    days_until_wednesday = (2 - given_weekday + 7) % 7
    if days_until_wednesday == 0:
        days_until_wednesday = 7
    # Calculate the next Thursday
    next_wednesday_date = given_date + timedelta(days=days_until_wednesday)
    return next_wednesday_date


def get_options_end_data(r_index, r_date):
    with next(get_db()) as db_session:
        list_of_holidays = get_list_of_holidays()

        x = db_session.execute(
            text(
                """SELECT distinct
        DATE_FORMAT(xpry_date, '%Y-%m-%d') as xpry_date
        FROM fo_udiff_bhavdata where fin_instrm_tp = "IDO" and tckr_symb = :r_index and xpry_date > :r_date and trade_date = :r_date order by xpry_date asc limit 1;
        """
            ),
            {'r_date': r_date.strftime('%Y-%m-%d'), 'r_index': r_index}
        ).scalar()

        return x


def get_futures_end_data(r_index, r_date):
    with next(get_db()) as db_session:
        x = db_session.execute(
            text(
                """SELECT distinct
    DATE_FORMAT(xpry_date, '%Y-%m-%d') as xpry_date
    FROM fo_udiff_bhavdata where fin_instrm_tp = "IDF" and tckr_symb = :r_index and xpry_date >= :current_start_date and trade_date = :current_start_date order by xpry_date asc limit 1;
    """
            ),
            {'current_start_date': r_date.strftime(
                '%Y-%m-%d'), 'r_index': r_index}
        ).scalar()
        return x


previous_close_price = {'NIFTY': None, 'BANKNIFTY': None}
week_numbers = {'NIFTY': {}, 'BANKNIFTY': {}}
week_dates = pd.date_range(start='2024-01-01', end='2024-12-30', freq='B')

x = 0
y = 0
for _date in week_dates:
    week_numbers['NIFTY'][_date] = x
    week_numbers['BANKNIFTY'][_date] = y
    if _date.weekday() == 3:
        x += 1
    if _date.weekday() == 2:
        y += 1


def get_report_data(r_date, r_index):
    global previous_close_price, week_numbers
    list_of_holidays = get_list_of_holidays()
    with next(get_db()) as db_session:

        futures_end_date = get_futures_end_data(r_index, r_date)
        if not futures_end_date:
            return
        options_end_date = get_options_end_data(r_index, r_date)
        # if r_index == 'BANKNIFTY' and r_date > datetime.strptime("2024-02-29", '%Y-%m-%d'):
        #     futures_end_date = datetime.strptime(
        #         futures_end_date, '%Y-%m-%d') - timedelta(days=1)
        #     while futures_end_date.strftime('%Y-%m-%d') in list_of_holidays:
        #         futures_end_date = futures_end_date - timedelta(days=1)
        #     futures_end_date = futures_end_date.strftime('%Y-%m-%d')

        print(r_date, futures_end_date, options_end_date, sep=" |||||| ")
        futures_bhav_data = pd.read_sql(
            text(f"""
    select close_price as fut_close_price,
                 undrlyg_price as 'Spot Cls',
                 strk_price,
                 prev_closing_price,
                 DATE_FORMAT(xpry_date, '%Y-%m-%d') as xpry_date
                 from fo_udiff_bhavdata where trade_date = '{r_date.strftime('%Y-%m-%d')}' and xpry_date = '{futures_end_date}'and fin_instrm_tp in ('IDF') and tckr_symb = '{r_index}'
    """), engine)
        print(futures_bhav_data)

        futures_bhav_data = futures_bhav_data.iloc[0]
        if futures_bhav_data['strk_price'] == 0:
            return
        fut_close_price = futures_bhav_data['fut_close_price']
        spot_price = futures_bhav_data['Spot Cls']
        previous_spot_close_price = previous_close_price[r_index]
        previous_close_price[r_index] = spot_price
        fut_prev_closing_price = futures_bhav_data['prev_closing_price']
        _week = f"Wk { week_numbers[r_index][r_date]}"

        options_bhav_data = pd.read_sql(
            text(f"""
    select fin_instrm_tp,close_price,tckr_symb,undrlyg_price as 'Spot Cls', strk_price, prev_closing_price,  DATE_FORMAT(xpry_date, '%Y-%m-%d') as xpry_date  from fo_udiff_bhavdata where trade_date = '{r_date.strftime('%Y-%m-%d')}' and xpry_date = '{options_end_date}'and fin_instrm_tp in ('IDO') and tckr_symb = '{r_index}'
    """), engine)
        print(
            '1111111111111111111111111111111111111111111111111111111111111111111111111111')

        print(options_bhav_data)
        options_bhav_data['diff_from_fut'] = fut_close_price - \
            options_bhav_data['strk_price']
        idx_min_positive_fut = options_bhav_data[options_bhav_data['diff_from_fut']
                                                 > 0]['diff_from_fut'].idxmin()
        idx_max_negative_fut = options_bhav_data[options_bhav_data['diff_from_fut']
                                                 <= 0]['diff_from_fut'].idxmax()
        ce_fut = options_bhav_data.iloc[idx_min_positive_fut]
        pe_fut = options_bhav_data.iloc[idx_max_negative_fut]

        options_bhav_data['diff_from_sopt'] = spot_price - \
            options_bhav_data['strk_price']
        idx_min_positive_spot = options_bhav_data[options_bhav_data['diff_from_sopt']
                                                  > 0]['diff_from_sopt'].idxmin()
        idx_max_negative_spot = options_bhav_data[options_bhav_data['diff_from_sopt']
                                                  <= 0]['diff_from_sopt'].idxmax()
        ce_spot = options_bhav_data.iloc[idx_min_positive_spot]
        pe_spot = options_bhav_data.iloc[idx_max_negative_spot]

        if r_index == 'NIFTY':
            if fut_close_price - ce_fut['strk_price'] > 30:
                ce_fut = pe_fut
            if spot_price - ce_spot['strk_price'] > 30:
                ce_spot = pe_spot
        if r_index == 'BANKNIFTY':
            if fut_close_price - ce_fut['strk_price'] > 60:
                ce_fut = pe_fut
            if spot_price - ce_spot['strk_price'] > 60:
                ce_spot = pe_spot
        synthetic_futures = fut_close_price + \
            abs(ce_fut['close_price'] - pe_fut['close_price'])

        synthetic_spot = fut_close_price + \
            abs(ce_fut['close_price'] - pe_fut['close_price'])

        return {
            'IDX': r_index,
            'Trade Date': r_date,
            'Options XpryDt': options_end_date,
            'XpryMth': datetime.strptime(futures_end_date, '%Y-%m-%d').strftime('%b'),
            'Spot Close': spot_price,
            'Futures Close': fut_close_price,

            'ATM CE Strike (based on Spot)': ce_spot['strk_price'],
            'ATM PE Strike (based on Spot)': pe_spot['strk_price'],
            'ATM CE Cls Pric (based on Spot)': ce_spot['close_price'],
            'ATM PE Cls Pric (based on Spot)': pe_spot['close_price'],


            'ATM CE Strike (based on FUT)': ce_fut['strk_price'],
            'ATM PE Strike (based on FUT)': pe_fut['strk_price'],
            'ATM CE Cls Pric (based on FUT)': ce_fut['close_price'],
            'ATM PE Cls Pric (based on FUT)': pe_fut['close_price'],

            'Synthetic based on Futures': synthetic_futures,
            'Synthetic Check Futures': 'YES' if synthetic_futures < spot_price else 'NO',
            'Synthetic based on SPOT': synthetic_spot,
            'Synthetic Check SPOT': 'YES' if synthetic_spot < spot_price else 'NO'
        }


def get_synthetic_check():
    indexes = [
        'NIFTY',
        'BANKNIFTY'
    ]
    file_name = "Synthetic Check.csv"
    file_path = Path(file_name)
    with open(file_path, "w", newline='')as f:
        f_csv = csv.writer(f)
        f_csv.writerow([
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
        ])
        for r_index in indexes:
            for _date in get_dates(start='2024-01-01', end='2024-12-30'):
                data = get_report_data(_date, r_index)
                if data:
                    f_csv.writerow([
                        data['IDX'],
                        data['Trade Date'].strftime('%d-%m-%Y'),
                        data['Options XpryDt'],
                        data['XpryMth'],
                        data['Spot Close'],
                        data['Futures Close'],
                        data['ATM CE Strike (based on Spot)'],
                        data['ATM PE Strike (based on Spot)'],
                        data['ATM CE Cls Pric (based on Spot)'],
                        data['ATM PE Cls Pric (based on Spot)'],
                        data['ATM CE Strike (based on FUT)'],
                        data['ATM PE Strike (based on FUT)'],
                        data['ATM CE Cls Pric (based on FUT)'],
                        data['ATM PE Cls Pric (based on FUT)'],
                        data['Synthetic based on Futures'],
                        data['Synthetic Check Futures'],
                        data['Synthetic based on SPOT'],
                        data['Synthetic Check SPOT']
                    ])


def load_synthetic_check_data_to_db(start: str, end: str):
    indexes = [
        'NIFTY',
        'BANKNIFTY'
    ]
    for _date in get_dates(start=start, end=end):
        for r_index in indexes:
            data = get_report_data(_date, r_index)
            if data:
                with next(get_db()) as db_session:
                    if not db_session.query(SyntheticCheck).filter_by(idx=data['IDX'],
                                                                      trade_date=data['Trade Date']).first():
                        db_session.add(SyntheticCheck(
                            idx=data['IDX'],
                            trade_date=data['Trade Date'],
                            options_expiry_date=data['Options XpryDt'],
                            expiry_month=data['XpryMth'],
                            spot_close=data['Spot Close'],
                            future_close=data['Futures Close'],
                            spot_ce_atm_strik=data['ATM CE Strike (based on Spot)'],
                            spot_pe_atm_strik=data['ATM PE Strike (based on Spot)'],
                            spot_ce_close_price=data['ATM CE Cls Pric (based on Spot)'],
                            spot_pe_close_price=data['ATM PE Cls Pric (based on Spot)'],
                            future_ce_atm_strik=data['ATM CE Strike (based on FUT)'],
                            future_pe_atm_strik=data['ATM PE Strike (based on FUT)'],
                            future_ce_close_price=data['ATM CE Cls Pric (based on FUT)'],
                            future_pe_close_price=data['ATM PE Cls Pric (based on FUT)'],
                            future_synthetic=data['Synthetic based on Futures'],
                            future_synthetic_check=(
                                data['Synthetic Check Futures'] == 'YES'),
                            spot_synthetic=data['Synthetic based on SPOT'],
                            spot_synthetic_check=(
                                data['Synthetic Check SPOT'] == 'YES')
                        ))
                        db_session.commit()


if __name__ == '__main__':
    load_synthetic_check_data_to_db(start='2024-01-01', end='2024-12-30')
