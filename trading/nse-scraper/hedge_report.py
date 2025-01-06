# import pandas as pd
# import numpy as np
# from sqlalchemy import create_engine, text
# import talib
# from db import engine, get_db

# # Assuming you have an engine created for your SQL connection
# # engine = create_engine('your_database_connection_string')

# market_data_query = """select `date`, `index`, percentage_change,relative_strength,
#  relative_performance_ratio from cm_market_data_indexes order by `date`, `index`"""

# df = pd.read_sql(text(market_data_query), engine, parse_dates=['date'])

# # Define the benchmark index
# benchmark = 'Nifty 50'
# # Adaptive window params
# min_period = 5
# max_period = 30
# # Initial rolling window
# window = 20

# # Filter data for the benchmark index
# benchmark_df = df[df['index'] == benchmark].copy()

# # Calculate rolling standard deviation (Benchmark Volatility)
# benchmark_df['rolling_std'] = benchmark_df['percentage_change'].rolling(
#     window=window).std()


# # Calculate the min and max volatility for scaling
# min_volatility = benchmark_df['rolling_std'].min()
# max_volatility = benchmark_df['rolling_std'].max()


# print(min_volatility)
# print(max_volatility)
# # print(benchmark_df)
# # Function to determine adaptive window period based on Benchmark Volatility


# def adaptive_window(volatility, min_vol, max_vol, min_period, max_period):
#     scaled_volatility = (volatility - min_vol) / (max_vol - min_vol)
#     adaptive_window = min_period + \
#         (max_period - min_period) * (1 - scaled_volatility)
#     return adaptive_window


# # Apply the function to get the adaptive window period
# benchmark_df['adaptive_window'] = benchmark_df['rolling_std'].apply(
#     adaptive_window, args=(min_volatility, max_volatility, min_period, max_period))

# benchmark_df['adaptive_window'] = benchmark_df['adaptive_window'].fillna(
#     5).round().astype(int)

# print(benchmark_df)
# # Merge the adaptive window back to the original dataframe
# df = df.merge(benchmark_df[['date', 'adaptive_window']], on='date', how='left')

# # Calculate the exponential moving average (EMA) with adaptive window period for each index
# df['adaptive_ema'] = np.nan

# df['rolling_std'] = df.groupby('index')['percentage_change'].rolling(
#     window=window).std().reset_index(level=0, drop=True)

# benchmark_df['percentage_change'].rolling(
#     window=window).std()


# for idx in df['index'].unique():
#     if idx != benchmark:
#         index_df = df[df['index'] == idx]
#         ema_values = []
#         for i in range(len(index_df)):
#             window_size = index_df['adaptive_window'].iloc[i]
#             if i >= window_size - 1:
#                 ema = talib.EMA(index_df['percentage_change'].iloc[i -
#                                 window_size + 1:i + 1].values, timeperiod=window_size)[-1]
#                 ema_values.append(ema)
#             else:
#                 ema_values.append(np.nan)
#         df.loc[df['index'] == idx, 'adaptive_ema'] = ema_values

# # Display the result
# print(df)
# df.to_csv('output.csv')

from db import engine, get_db
from datetime import datetime, timedelta
from sqlalchemy import func, text
import pandas as pd
from pathlib import Path
import csv

from load_start_days import get_list_of_holidays
from models.hedge_report import HedgeReport


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

# @app.route('/get-report7', methods=['GET'])


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

        # if r_index == 'NIFTY':
        #     # every thursday
        #     end_date = next_thursday(r_date)
        #     # print(list_of_holidays)
        #     while end_date.strftime('%Y-%m-%d') in list_of_holidays:
        #         end_date = end_date - timedelta(days=1)
        #     return end_date.strftime('%Y-%m-%d')

        # if r_index == 'BANKNIFTY':
        #     # if r_date < datetime.strptime("2024-02-29", '%Y-%m-%d'):
        #     #     end_date = next_thursday(r_date)
        #     #     while end_date.strftime('%Y-%m-%d') in list_of_holidays:
        #     #         end_date = end_date - timedelta(days=1)
        #     #     return end_date.strftime('%Y-%m-%d')

        #     # every wednesday
        #     end_date = next_wednesday(r_date)
        #     while end_date.strftime('%Y-%m-%d') in list_of_holidays:
        #         end_date = end_date - timedelta(days=1)
        #     return end_date.strftime('%Y-%m-%d')


def get_futures_end_data(r_index, r_date):
    with next(get_db()) as db_session:
        # if r_index == 'NIFTY':
        #     return db_session.execute(
        #         text(
        #             "select DATE_FORMAT(end_date, '%Y-%m-%d') as end_date from start_date_lookup where end_date >= :current_start_date order by end_date asc limit 1;"
        #         ),
        #         {'current_start_date': r_date.strftime('%Y-%m-%d')}
        #     ).scalar()
        # if r_index == 'BANKNIFTY':

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
        # print(x)
        return x


previous_close_price = {'NIFTY': None, 'BANKNIFTY': None}
week_numbers = {'NIFTY': {}, 'BANKNIFTY': {}}
week_dates = pd.date_range(start='2024-01-01',
                           end='2024-12-30', freq='B')

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
        options_bhav_data['diff_from_fut'] = fut_close_price - \
            options_bhav_data['strk_price']
        idx_min_positive = options_bhav_data[options_bhav_data['diff_from_fut']
                                             > 0]['diff_from_fut'].idxmin()
        idx_max_negative = options_bhav_data[options_bhav_data['diff_from_fut']
                                             <= 0]['diff_from_fut'].idxmax()
        ce = options_bhav_data.iloc[idx_min_positive]
        pe = options_bhav_data.iloc[idx_max_negative]

        if r_index == 'NIFTY':
            if fut_close_price - ce['strk_price'] > 30:
                ce = pe
        if r_index == 'BANKNIFTY':
            if fut_close_price - ce['strk_price'] > 60:
                ce = pe
        # print(ce)
        # print(pe)
        return {
            'IDX': r_index,
            'Date': r_date.strftime('%Y-%m-%d'),
            'Week': _week,
            'Day': r_date.strftime('%A'),
            'Series Month': datetime.strptime(futures_end_date, '%Y-%m-%d').strftime('%B'),
            'Fut Cls': fut_close_price,
            'Fut Prvs Cls': fut_prev_closing_price,
            'Spot Cls': spot_price,
            'Spot Prvs cls': previous_spot_close_price,
            'CE ATM': ce['strk_price'],
            'CE Cls Prc': ce['close_price'],
            'CE PrvsClsgPric': ce['prev_closing_price'],
            'PE ATM': pe['strk_price'],
            'PE Cls Prc': pe['close_price'],
            'PE PrvsClsgPric': ce['prev_closing_price'],
            'CE Hedge Cost': round(ce['close_price'] / fut_close_price, 6),
            'PE Hedge Cost': round(pe['close_price'] / fut_close_price, 6)
        }


def get_report7(start: str, end: str):

    ('IDX'	'Date'	'Fut Cls'	'Spot Cls'	'CE ATM'	'CE Cls Prc'	'CE PrvsClsgPric'	'PE ATM'	'PE Cls Prc'	'PE PrvsClsgPric'
     'CE Hedge Cost'	'PE Hedge Cost')

    indexes = [
        'NIFTY',
        'BANKNIFTY'
    ]
    file_name = "Hedge cost.csv"
    file_path = Path(file_name)
    with open(file_path, "w", newline='')as f:
        f_csv = csv.writer(f)
        f_csv.writerow([
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
        ])
        for r_index in indexes:
            for _date in get_dates(start=start,
                                   end=end):
                data = get_report_data(_date, r_index)
                if data:
                    with next(get_db()) as db_session:
                        if not db_session.query(HedgeReport).filter_by(idx=data['IDX'],
                                                                       date=data['Date'],).first():
                            db_session.add(HedgeReport(
                                idx=data['IDX'],
                                date=data['Date'],
                                week=data['Week'],
                                day=data['Day'],
                                series_month=data['Series Month'],
                                future_close=data['Fut Cls'],
                                futre_previous_close=data['Fut Prvs Cls'],
                                spot_close=data['Spot Cls'],
                                spot_previous_close=data['Spot Prvs cls'],
                                ce_atm=data['CE ATM'],
                                ce_close_price=data['CE Cls Prc'],
                                ce_previous_close_price=data['CE PrvsClsgPric'],
                                pe_atm=data['PE ATM'],
                                pe_close_price=data['PE Cls Prc'],
                                pe_previous_close_price=data['PE PrvsClsgPric'],
                                ce_hedge_cost=data['CE Hedge Cost'],
                                pe_hedge_cost=data['PE Hedge Cost']
                            ))
                            db_session.commit()


get_report7(start='2024-01-01',
            end='2024-12-30')
