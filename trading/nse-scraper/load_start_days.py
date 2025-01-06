from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import datetime, timedelta
import json

import pandas as pd
from sqlalchemy import text
from models.start_date_lookup import StartDateLookup
from db import engine, get_db


def get_last_thursday_of_the_month(year, month):
    # Get the last day of the month
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1)
    else:
        next_month_first = datetime(year, month + 1, 1)
    last_day_of_month = next_month_first - timedelta(days=1)

    # Find the last Thursday
    while last_day_of_month.weekday() != 3:  # Thursday is represented by 3
        last_day_of_month -= timedelta(days=1)

    return last_day_of_month


def get_list_of_holidays():
    # todo: need to implement this funtion to return holidays based on the year selected.
    holidays_query = """

    SELECT DATE_FORMAT(tradingDate, '%Y-%m-%d') as trading_date FROM `holidays` WHERE tradingDate > DATE_SUB(CURDATE(),INTERVAL 1 YEAR);

    """
    holidays_data = pd.read_sql(
        text(holidays_query),
        engine
        # date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d')
    )

    return holidays_data.trading_date.to_list()


list_of_holidays = get_list_of_holidays()


def get_instrumet_start_day(xpry_date):
    a = datetime.strptime(xpry_date, '%Y-%m-%d')
    y = a - relativedelta(months=3)
    y = get_last_thursday_of_the_month(y.year, y.month)
    start_date = (y+timedelta(days=1))
    while start_date.strftime('%Y-%m-%d') in list_of_holidays or start_date.weekday() > 4:
        start_date = start_date + timedelta(days=1)
    return start_date.strftime('%Y-%m-%d')


if __name__ == '__main__':
    financial_instrument_end_dates_query = """SELECT distinct
    xpry_date as expiry_date
    FROM fo_udiff_bhavdata where fin_instrm_tp in ("STO","STF")
    order by xpry_date desc;
    """
    financial_instrument_end_date = pd.read_sql(
        financial_instrument_end_dates_query,
        engine)

    financial_instrument_end_date = financial_instrument_end_date.expiry_date.apply(
        lambda x: x.strftime('%Y-%m-%d')).to_list()
    with next(get_db()) as db_session:
        for expiry_date in financial_instrument_end_date:
            if not db_session.query(StartDateLookup).filter(
                    StartDateLookup.end_date == expiry_date).first():
                try:
                    db_session.add(
                        StartDateLookup(
                            end_date=expiry_date,
                            start_date=get_instrumet_start_day(expiry_date)
                        )
                    )
                    db_session.commit()
                    db_session.close()
                except Exception as e:
                    print(e)
                    print(expiry_date)
