import csv
from datetime import datetime
import json
import time
import os
import pathlib
from functools import lru_cache
from typing import Dict, List

import pandas as pd
import requests
from datetime import timedelta

from sqlalchemy import text

from db import engine, get_db
from models.cm_index_data import CMIndexData
from models.securities_bhavdata import SecurityiesBhavData
from models.fo_udiff_bhavdata import FOUDIFFBhavData

# import lxml.etree
# from lxml import etree

base_dir = '../Data/output/'
all_csv_file = 'all_csv_file.csv'
# nifty_500 = 'https://www1.nseindia.com/content/indices/ind_nifty500list.csv'

nifty_500 = 'https://archives.nseindia.com/content/indices/ind_nifty500list.csv'
nifty_500_csv = 'ind_nifty500list.csv'
fo_nifty = 'https://archives.nseindia.com/content/fo/fo_mktlots.csv'
fo_nifty_csv = 'fo_mktlots.csv'
DOWNLOAD_FILE_COUNT = 26
holiday_url = 'https://www.nseindia.com/api/holiday-master?type=trading'

pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)


REQUIRED_NUMBER_OF_DATES_FOR_REPORT = 26


def get_the_past_26_active_dates(current_date: datetime):
    # with next(get_db()) as db_session:
    #     return [i.date for i in db_session.query(
    #         SecurityiesBhavData.date).filter(
    #             SecurityiesBhavData.series.like('%EQ%'),
    #             SecurityiesBhavData.date <= current_date).order_by(SecurityiesBhavData.date.desc()).limit(REQUIRED_NUMBER_OF_DATES_FOR_REPORT)]

    dates_query = f"""
SELECT DISTINCT `date` FROM `testdb`.securities_bhavdata
where `date` <="{current_date.strftime('%Y-%m-%d')}" order by `date` DESC
limit {REQUIRED_NUMBER_OF_DATES_FOR_REPORT};
"""
    df = pd.read_sql(
        text(dates_query),
        engine, parse_dates=['date'])
    return df['date'].tolist()


def get_fo_equity_symbols_list(current_date: datetime):
    with next(get_db()) as db_session:
        tckr_symb_list = db_session.query(FOUDIFFBhavData.tckr_symb).filter(
            FOUDIFFBhavData.trade_date == current_date).distinct().all()
        return [result[0] for result in tckr_symb_list]


def download_file(url, csv_file_name):
    downloaded_file_name = os.path.join(base_dir, csv_file_name)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
    with requests.Session() as s:
        req = s.get(url, allow_redirects=False, headers=headers)
    if req.status_code == 200:
        decoded_content = req.content.decode('utf-8')
        with open(downloaded_file_name, 'w') as f:
            f.write(decoded_content)
    else:
        raise Exception(
            f'Download failed {req.text} HTTP status code: {req.status_code} \n, url: {url} ')
    return pathlib.Path(downloaded_file_name)


def get_nifty_500_symbols_list():
    nifty_500 = 'https://archives.nseindia.com/content/indices/ind_nifty500list.csv'
    filename = download_file(nifty_500, 'ind_nifty500list.csv')
    output = set()
    with open(filename, 'r') as f:
        f.readline()
        for l in f:
            if len(l.strip()) == 0:
                continue
            symbol = l.split(',')[2].strip()
            output.add(symbol)
    filename.unlink()
    return output


def get_eq_security_bhav_data(current_date):
    with next(get_db()) as db_session:
        return db_session.query(
            SecurityiesBhavData).filter(SecurityiesBhavData.series.like('%EQ%'), SecurityiesBhavData.date == current_date).all()


def get_security_bhav_data(current_date):
    with next(get_db()) as db_session:
        return db_session.query(
            SecurityiesBhavData).filter(SecurityiesBhavData.date == current_date).all()


def get_nifty_index_data(data_date: datetime):
    with next(get_db()) as db_session:
        return db_session.query(
            CMIndexData).filter(CMIndexData.index_name == 'Nifty 50', CMIndexData.index_date == data_date).first()


def get_ad_and_dd(past_26_active_dates: List[datetime]) -> dict:
    output = {}
    last_index_value = len(past_26_active_dates)-1
    i = 0
    data = []
    while i < last_index_value:
        today_date = past_26_active_dates[i]
        yesterday_date = past_26_active_dates[i+1]
        if len(data) < 2:
            today_data = get_security_bhav_data(today_date)
            today_data = {datum.symbol: datum for datum in today_data}
            yesterday_data = get_security_bhav_data(yesterday_date)
            yesterday_data = {datum.symbol: datum for datum in yesterday_data}
            data = [today_data, yesterday_data]
        else:
            yesterday_data = get_security_bhav_data(yesterday_date)
            yesterday_data = {datum.symbol: datum for datum in yesterday_data}
            data = [data[-1], yesterday_data]
        for t_symbol, t_datum in data[0].items():
            try:
                t_closed_price = t_datum.close_price
                t_volume = int(t_datum.total_trade_quantity)  # ttl_trd_qnty
                p_volume = int(data[1][t_symbol].total_trade_quantity)
                p_closed_price = data[1][t_symbol].close_price
            except Exception as e:
                # print(e)
                # print(e.__str__())
                continue
            # Calculate Accumulation and Distribution
            if ((t_volume - p_volume) > 0) and (((t_closed_price - p_closed_price)/p_closed_price)*100 >= 0.20):
                if t_symbol in output:
                    output[t_symbol]["ad_count"] += 1
                else:
                    output[t_symbol] = {"ad_count": 1, "dd_count": 0, "NA": 0}
            elif ((t_volume - p_volume) > 0) and (((t_closed_price - p_closed_price)/p_closed_price)*100 <= -0.20):
                if t_symbol in output:
                    output[t_symbol]["dd_count"] += 1
                else:
                    output[t_symbol] = {"ad_count": 0, "dd_count": 1, "NA": 0}
            else:
                if t_symbol in output:
                    output[t_symbol]["NA"] += 1
                else:
                    output[t_symbol] = {"ad_count": 0, "dd_count": 0, "NA": 1}
        i += 1
    return output


def get_rs(past_26_active_dates: list, days: int):
    today_date = past_26_active_dates[0]
    old_date = past_26_active_dates[days-1]
    rs = {}

    old_data_db = get_security_bhav_data(old_date)
    old_data = {datum.symbol: datum for datum in old_data_db}

    today_data_db = get_eq_security_bhav_data(today_date)
    old_datetime_fmt = old_date.strftime('%Y-%m-%d')
    today_datetime_fmt = today_date.strftime('%Y-%m-%d')
    old_index_data = get_nifty_index_data(old_datetime_fmt)
    today_index_data = get_nifty_index_data(today_datetime_fmt)
    for datum in today_data_db:
        symbol = datum.symbol
        try:
            today_closing_price = datum.close_price
        except Exception as e:
            # print(e.__str__())
            continue
        try:
            old_closing_price = old_data[symbol].close_price
        except Exception as e:
            # print(e.__str__())
            continue
        rs_today = today_closing_price / \
            today_index_data.closing_index_value
        rs_old = old_closing_price/old_index_data.closing_index_value
        rs[symbol] = [round(rs_today, 4), round(rs_old, 4)]
    return rs


def get_vwap(past_26_active_dates: list, days: int):
    req_dates = past_26_active_dates[:days]
    # output = {
    #     "symbol": {
    #         "positive_vwap": 0,
    #         "negative_vwap": 0
    #     }
    # }
    output = {}
    for req_date in req_dates:
        data = get_eq_security_bhav_data(req_date)
        for datum in data:
            avg_price = datum.avg_price
            closing_price = datum.close_price
            symbol = datum.symbol
            if symbol not in output:
                output[symbol] = {"positive_vwap": 0,
                                  "negative_vwap": 0
                                  }
            if avg_price < closing_price:
                output[symbol]["positive_vwap"] += 1
            elif avg_price > closing_price:
                output[symbol]["negative_vwap"] += 1
    return output


def data_processing(data_dates):
    total_data = {}
    for data_date in data_dates:
        # print(f'Processing {data_date}')
        data = get_eq_security_bhav_data(data_date)
        for datum in data:
            symbol = datum.symbol
            series = datum.series
            # date = datum.date
            ttl_trd_qnty = datum.total_trade_quantity
            turnover = datum.turnover_lacs
            no_of_trades = datum.number_of_trades
            deliv_qty = datum.delivery_quantity
            if series.strip() != "EQ":
                continue
            try:
                qnty_per_trade = int(ttl_trd_qnty)/int(no_of_trades)
            except Exception as e:
                qnty_per_trade = 1

            try:
                turnover_per_trade = turnover/no_of_trades
            except Exception as e:
                turnover_per_trade = 1

            try:
                deliv_qty = int(deliv_qty)
            except Exception as e:
                deliv_qty = 1
            if symbol in total_data:
                total_data[symbol]['qnty_per_trade'] += qnty_per_trade
                total_data[symbol]['turnover_per_trade'] += turnover_per_trade
                total_data[symbol]['deliv_qty'] += deliv_qty
                total_data[symbol]['count'] += 1
            else:
                total_data[symbol] = {
                    "qnty_per_trade": qnty_per_trade,
                    "turnover_per_trade": turnover_per_trade,
                    "deliv_qty": deliv_qty,
                    "count": 1
                }
    for symbol in total_data:
        total_data[symbol]['avg_qnty_per_trade'] = total_data[symbol]['qnty_per_trade'] / \
            total_data[symbol]['count']
        total_data[symbol]['avg_turnover_per_trade'] = total_data[symbol]['turnover_per_trade'] / \
            total_data[symbol]['count']
        total_data[symbol]['avg_deliv_qty'] = total_data[symbol]['deliv_qty'] / \
            total_data[symbol]['count']
    return total_data


def main(current_date: datetime):
    past_26_active_dates = get_the_past_26_active_dates(current_date)

    ad_dd_data = get_ad_and_dd(past_26_active_dates)

    total_data = data_processing(past_26_active_dates[1:14])

    nifty_fo_data = get_fo_equity_symbols_list(current_date)

    nifty_500_data = get_nifty_500_symbols_list()

    rs_7_data = get_rs(past_26_active_dates, 7)
    rs_14_data = get_rs(past_26_active_dates, 14)
    vwap_7_data = get_vwap(past_26_active_dates, 7)
    vwap_14_data = get_vwap(past_26_active_dates, 14)

    current_data = get_eq_security_bhav_data(current_date)

    all_data = []
    for datum in current_data:
        req_data = {}
        req_data['SYMBOL'] = datum.symbol
        req_data['series'] = datum.series
        req_data['DATE'] = datum.date
        req_data['ttl_trd_qnty'] = datum.total_trade_quantity
        req_data['no_of_trades'] = datum.number_of_trades
        req_data['deliv_qty'] = datum.delivery_quantity
        req_data['qnty_per_trade'] = datum.total_trade_quantity / \
            datum.number_of_trades
        try:
            req_data["trade_qty_deviation"] = int((
                (req_data['qnty_per_trade'] - total_data[datum.symbol]['avg_qnty_per_trade'])/total_data[datum.symbol]['avg_qnty_per_trade'])*100)
        except:
            req_data["trade_qty_deviation"] = ""
        try:
            req_data["delivery_qty_deviation"] = int((
                (req_data['deliv_qty'] - total_data[datum.symbol]['avg_deliv_qty'])/total_data[datum.symbol]['avg_deliv_qty'])*100)
        except:
            req_data["delivery_qty_deviation"] = ""
        try:
            req_data['avg_qnty_per_trade'] = str(
                int(total_data[datum.symbol]['avg_qnty_per_trade']))
        except:
            req_data['avg_qnty_per_trade'] = ""
        req_data['F&O'] = 'Y' if datum.symbol in nifty_fo_data else 'N'
        req_data["NIFTY_500"] = 'Y' if datum.symbol in nifty_500_data else 'N'
        try:
            req_data['avg_deliv_qty'] = int(
                total_data[datum.symbol]['avg_deliv_qty'])
        except:
            req_data['avg_deliv_qty'] = ''
        req_data['RS_Current'] = rs_7_data.get(datum.symbol, ['N/A', 'N/A'])[0]
        req_data['RS_7'] = rs_7_data.get(datum.symbol, ['N/A', 'N/A'])[1]
        req_data['RS_14'] = rs_14_data.get(datum.symbol, ['N/A', 'N/A'])[1]
        req_data['RS7-RS14'] = round(rs_7_data.get(datum.symbol, [0, 0])[1] -
                                     rs_14_data.get(datum.symbol, [0, 0])[1], 2)
        try:
            req_data['ROC_RS_7'] = ((rs_7_data.get(datum.symbol, ['N/A'])[0] - rs_7_data.get(
                datum.symbol, 'N/A')[1])/rs_7_data.get(datum.symbol, ['N/A', 'N/A'])[1])*100
        except:
            req_data['ROC_RS_7'] = ""
        try:
            req_data['ROC_RS_14'] = ((rs_14_data.get(datum.symbol, 'N/A')[0] - rs_14_data.get(
                datum.symbol, 'N/A')[1])/rs_14_data.get(datum.symbol, 'N/A')[1])*100
        except:
            req_data['ROC_RS_14'] = ""
        req_data['vwap_positive_7days'] = vwap_7_data[datum.symbol]["positive_vwap"]
        req_data['vwap_negative_7days'] = vwap_7_data[datum.symbol]["negative_vwap"]
        req_data['vwap_positive_14days'] = vwap_14_data[datum.symbol]["positive_vwap"]
        req_data['vwap_negative_14days'] = vwap_14_data[datum.symbol]["negative_vwap"]
        try:
            req_data['AD_Count'] = ad_dd_data[datum.symbol]["ad_count"]
        except:
            req_data['AD_Count'] = 0
        try:
            req_data['DD_Count'] = ad_dd_data[datum.symbol]["dd_count"]
        except:
            req_data['DD_Count'] = 0
        try:
            req_data['NA_Count'] = ad_dd_data[datum.symbol]["NA"]
        except:
            req_data['NA_Count'] = 0

        req_data['qnty_per_trade'] = int(req_data['qnty_per_trade'])

        del req_data['series']
        del req_data['ttl_trd_qnty']
        del req_data['no_of_trades']

        del req_data['RS_7']
        del req_data['RS_14']
        del req_data['RS_Current']
        del req_data['RS7-RS14']

        all_data.append(req_data)

    filename = 'all_data.csv'

    keys = [
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
    ]

    with open(filename, 'w', newline='') as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(all_data)

    return all_data


with next(get_db()) as _db_session:
    _last_updated = _db_session.execute(
        text("select DATE_FORMAT(max(`date`), '%Y-%m-%d') as last_updated_date from securities_bhavdata")).scalar()
# _last_updated = datetime.strptime("2024-09-19", '%Y-%m-%d')
_last_updated = datetime.strptime(_last_updated, '%Y-%m-%d')

# print(_last_updated)
main(_last_updated)
