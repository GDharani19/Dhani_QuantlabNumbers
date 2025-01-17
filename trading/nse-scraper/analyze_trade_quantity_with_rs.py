import csv
from datetime import datetime
from decimal import Decimal
import json
import time
import os
import pathlib
from functools import lru_cache
from typing import Dict, List

import pandas as pd
import numpy as np
import requests
from datetime import timedelta

from sqlalchemy import text

from db import engine, get_db
from models.cm_index_data import CMIndexData
from models.securities_bhavdata import SecurityiesBhavData
from models.fo_udiff_bhavdata import FOUDIFFBhavData
from constants import mst_table_mapping
from config import settings

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


@lru_cache
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
    nifty_500_data = get_nifty_500_symbols_list()
    with next(get_db()) as db_session:
        data = db_session.query(
            SecurityiesBhavData).filter(SecurityiesBhavData.series.like('%EQ%'), SecurityiesBhavData.date == current_date).all()

        return [datum for datum in data if 'EQ' in datum.series or datum.symbol in nifty_500_data]


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
        try:
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
        except Exception as e:
            # print(e.__str__())
            continue
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
    nifty_500_data = get_nifty_500_symbols_list()
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
            if series.strip() != "EQ" and (symbol not in nifty_500_data):
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


def main(current_date: datetime, upload_to_remote_server=False):
    past_26_active_dates = get_the_past_26_active_dates(current_date)

    ad_dd_data = get_ad_and_dd(past_26_active_dates)

    total_data = data_processing(past_26_active_dates[1:14])

    nifty_fo_data = get_fo_equity_symbols_list(current_date)

    nifty_500_data = get_nifty_500_symbols_list()
    # import pdb;pdb.set_trace()
    rs_7_data = get_rs(past_26_active_dates, 7)
    # print(rs_7_data)
    rs_14_data = get_rs(past_26_active_dates, 14)
    # print(rs_14_data)
    vwap_7_data = get_vwap(past_26_active_dates, 7)
    # print(vwap_7_data)
    vwap_14_data = get_vwap(past_26_active_dates, 14)

    current_data = get_eq_security_bhav_data(current_date)

    all_data = []
    for datum in current_data:
        req_data = {}
        req_data['symbol'] = datum.symbol
        req_data['series'] = datum.series
        req_data['date'] = datum.date
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
        req_data['f_and_o'] = 'Y' if datum.symbol in nifty_fo_data else 'N'
        req_data["nifty_500"] = 'Y' if datum.symbol in nifty_500_data else 'N'
        try:
            req_data['avg_deliv_qty'] = int(
                total_data[datum.symbol]['avg_deliv_qty'])
        except:
            req_data['avg_deliv_qty'] = ''
        req_data['rs_current'] = rs_7_data.get(datum.symbol, ['N/A', 'N/A'])[0]
        req_data['rs_7'] = rs_7_data.get(datum.symbol, ['N/A', 'N/A'])[1]
        req_data['rs_14'] = rs_14_data.get(datum.symbol, ['N/A', 'N/A'])[1]
        req_data['rs_7_minus_rs_14'] = round(rs_7_data.get(datum.symbol, [0, 0])[1] -
                                             rs_14_data.get(datum.symbol, [0, 0])[1], 2)
        try:
            req_data['roc_rs_7'] = ((rs_7_data.get(datum.symbol, ['N/A'])[0] - rs_7_data.get(
                datum.symbol, 'N/A')[1])/rs_7_data.get(datum.symbol, ['N/A', 'N/A'])[1])*100
        except:
            req_data['roc_rs_7'] = ""
        try:
            req_data['roc_rs_14'] = ((rs_14_data.get(datum.symbol, 'N/A')[0] - rs_14_data.get(
                datum.symbol, 'N/A')[1])/rs_14_data.get(datum.symbol, 'N/A')[1])*100
        except:
            req_data['roc_rs_14'] = ""
        req_data['vwap_positive_7days'] = vwap_7_data[datum.symbol]["positive_vwap"]
        req_data['vwap_negative_7days'] = vwap_7_data[datum.symbol]["negative_vwap"]
        req_data['vwap_positive_14days'] = vwap_14_data[datum.symbol]["positive_vwap"]
        req_data['vwap_negative_14days'] = vwap_14_data[datum.symbol]["negative_vwap"]
        try:
            req_data['ad_count'] = ad_dd_data[datum.symbol]["ad_count"]
        except:
            req_data['ad_count'] = 0
        try:
            req_data['dd_count'] = ad_dd_data[datum.symbol]["dd_count"]
        except:
            req_data['dd_count'] = 0
        try:
            req_data['na_count'] = ad_dd_data[datum.symbol]["NA"]
        except:
            req_data['na_count'] = 0

        req_data['qnty_per_trade'] = int(req_data['qnty_per_trade'])

        del req_data['series']
        del req_data['ttl_trd_qnty']
        del req_data['no_of_trades']

        # del req_data['RS_7']
        # del req_data['RS_14']
        # del req_data['RS_Current']
        # del req_data['RS7-RS14']

        all_data.append(req_data)
    csv_data = pd.DataFrame(all_data)

    # csv_data = csv_data.replace(, None)
    # csv_data = csv_data.replace(pd.NaT, None)
    csv_data = csv_data.replace([pd.NaT, np.nan, '-', 'N/A', ''], None)
    csv_data.date = (
        csv_data.date.apply(
            lambda x: x.strftime('%Y-%m-%d') if x else x))
    add_data_to_db_helper('all_data_csv_report', csv_data)
    if upload_to_remote_server:
        add_data_to_server('all_data_csv_report', csv_data)

    # return all_data


def add_data_to_db_helper(model_name, new_data):
    mdl = mst_table_mapping[model_name]['model']
    clmns = mst_table_mapping[model_name]['filters']
    with next(get_db()) as db_session:
        if clmns:
            for row in new_data.itertuples(index=False):
                if not db_session.query(mdl).filter_by(**{k: v for k, v in row._asdict().items() if k in clmns}).first():
                    db_session.add(mdl(**row._asdict()))
        else:
            for row in new_data.itertuples(index=False):
                if not db_session.query(mdl).filter_by(**row._asdict()).first():
                    db_session.add(mdl(**row._asdict()))
        db_session.commit()
        db_session.close()


def add_data_to_server(model_name, new_data: pd.DataFrame):
    new_data = new_data.to_json(
        orient="records", double_precision=14)
    csv_data_list = json.loads(new_data)
    print(csv_data_list[0])
    print(len(csv_data_list))
    total = len(csv_data_list)
    limit = 1000
    offset = 0

    while offset < total:
        _resp = requests.post(f"{settings.API_URL}/update-db", json={
            "secrete": settings.SECRET_KEY,
            "table_name": model_name, "new_data": csv_data_list[offset:limit+offset]})
        print(_resp.status_code)
        print(_resp.ok)
        print(_resp.text)
        if _resp.status_code == 500:
            raise RuntimeError(
                "Something went wrong while uploading: all csv data")
        offset += limit


# with next(get_db()) as _db_session:
#     _last_updated_dates = _db_session.execute(
#         text("select distinct(DATE_FORMAT((`date`), '%Y-%m-%d')) as last_updated_date from securities_bhavdata order by last_updated_date asc;")).all()
# # _last_updated = datetime.strptime("2024-09-19", '%Y-%m-%d')
# print(_last_updated_dates)
# for _last_updated in _last_updated_dates[26:]:
#     print(_last_updated[0])
#     _last_updated = datetime.strptime(_last_updated[0], '%Y-%m-%d')
#     # print(_last_updated)
#     main(_last_updated)


# print(_last_updated)
if __name__ == "__main__":
    with next(get_db()) as _db_session:
        _last_updated = _db_session.execute(
            text("select DATE_FORMAT(max(`date`), '%Y-%m-%d') as last_updated_date from securities_bhavdata")).scalar()
    # _last_updated = datetime.strptime("2024-09-19", '%Y-%m-%d')
    _last_updated = datetime.strptime(_last_updated, '%Y-%m-%d')
    main(_last_updated)
