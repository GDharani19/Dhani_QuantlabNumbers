import copy
from email.message import EmailMessage
from functools import cache

import numpy as np
from requests import Session, ReadTimeout
import requests
from requests.exceptions import ConnectionError
from pathlib import Path
import pickle
import os
import random
from typing import Literal, Any, Optional, Union, List, Dict
from zipfile import ZipFile
from datetime import date, datetime, timedelta
import pandas as pd
from sqlalchemy import text

from config import settings
from load_start_days import get_instrumet_start_day

from models.cm_market_data_indexes import CMMarketDataIndexes
from user_agents_list import user_agents
import time

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from constants import mst_table_mapping, tables_to_get
from analyze_trade_quantity_with_rs import main as all_data_csv_main

root_dir = os.getcwd()
temp_folder = root_dir+'/temp'
if not os.path.exists(temp_folder):
    os.mkdir(temp_folder)


BASE_URL = "https://www.nseindia.com/api"
ARCHIVE_URL = "https://archives.nseindia.com"
NEW_ARCHIVE_URL = "https://nsearchives.nseindia.com"

referer_list = [
    # "https://www.nseindia.com/get-quotes/equity?symbol=HDFCBANK",
    "https://www.nseindia.com/all-reports"
]

headers = [
    {
        "User-Agent": random.choice(user_agents),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": random.choice(referer_list),
    }, {
        "User-Agent": random.choice(user_agents),
        "Accept": "*/*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": random.choice(referer_list),
    }
]


get_instruments_dates_url = f"{settings.API_URL}/get-instruments-dates"
resp = requests.post(get_instruments_dates_url, json={
    "secrete": settings.SECRET_KEY})
print(resp.status_code)
print(resp.ok)
print(resp.json())

start_dates: Dict[str, str] = {_date["expiry_date"] : _date['start_date'] for _date in resp.json()}


class NSE:
    def __init__(self):
        self.dir = Path(temp_folder)

        self.cookie_path = self.dir / "nse_cookies.pkl"
        self.session = Session()

        self.session.headers.update(random.choice(headers))
        self.session.cookies.update(self.__get_cookies())

    def __set_cookies(self):
        r = self.__req("https://www.nseindia.com/option-chain", timeout=30)
        cookies = r.cookies
        self.cookie_path.write_bytes(pickle.dumps(cookies))
        return cookies

    def __get_cookies(self):
        if self.cookie_path.exists():
            cookies = pickle.loads(self.cookie_path.read_bytes())
            if self.__has_cookies_expired(cookies):
                cookies = self.__set_cookies()
            return cookies

        return self.__set_cookies()

    @staticmethod
    def __has_cookies_expired(cookies):
        for cookie in cookies:
            if cookie.is_expired():
                return True
                8
        return False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.session.close()

        return False

    @staticmethod
    def __get_path(path: Union[str, Path], is_folder: bool = False):
        path = path if isinstance(path, Path) else Path(path)

        if is_folder:
            if path.is_file():
                raise ValueError(f"{path}: must be a folder")

            if not path.exists():
                path.mkdir(parents=True)

        return path

    def __req(self, url, params=None, timeout=15):
        try:
            r = self.session.get(url, params=params, timeout=timeout)
        except ReadTimeout as e:
            raise TimeoutError(repr(e))

        if not r.ok:
            raise ConnectionError(f"{url} {r.status_code}: {r.reason}")

        return r

    @staticmethod
    def __unzip(file: Path, folder: Path):
        with ZipFile(file) as _zip:
            filepath = _zip.extract(member=_zip.namelist()[0], path=folder)

        file.unlink()
        return Path(filepath)

    def __download(self, url: str, folder: Path, fname: str = None):
        if not fname:
            fname = folder / url.split("/")[-1]
        else:
            fname = folder / fname
        retries = 0
        while retries < settings.MAX_RETRIES:
            try:
                with self.session.get(url, stream=True, timeout=20) as r:

                    content_type = r.headers.get("content-type")

                    if content_type and "text/html" in content_type:
                        with open('temp/failed_files.txt', 'a') as f:
                            f.write(url.split("/")[-1])
                            f.write("\n")
                        raise RuntimeError(
                            f"NSE file, {url.split('/')[-1]} is unavailable or not yet updated."
                        )

                    with fname.open(mode="wb") as f:
                        for chunk in r.iter_content(chunk_size=1000000):
                            f.write(chunk)
                    break
            except (ReadTimeout, ConnectionError) as e:
                print(e)
                retries += 1
                time.sleep(retries)
                if retries >= settings.MAX_RETRIES:
                    raise e
        return fname

    def download_data(self, table_name, date: datetime) -> Path:
        folder = self.dir
        table_details = tables_to_get.get(table_name)
        date_str = date.strftime(table_details[4])
        fname = table_details[2].format(date_str=date_str)
        url = table_details[1].replace("{date_str}", date_str)
        file = self.__download(url, folder, fname=fname)
        if not file.is_file():
            file.unlink()
            raise FileNotFoundError(f"Failed to download file: {file.name}")
        return NSE.__unzip(file, file.parent) if table_details[3] else file

    def equity_cm_bhavcopy_final(self, date: datetime) -> Path:
        date_str = date.strftime("%Y%m%d").upper()
        month = date_str[2:5]

        folder = self.dir

        url = f"{NEW_ARCHIVE_URL}/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"

        file = self.__download(url, folder)

        if not file.is_file():
            file.unlink()
            raise FileNotFoundError(f"Failed to download file: {file.name}")

        return NSE.__unzip(file, file.parent)

    def equity_fo_bhavcopy_final(self, date: datetime) -> Path:
        date_str = date.strftime("%Y%m%d").upper()
        month = date_str[2:5]

        folder = self.dir

        url = f"{NEW_ARCHIVE_URL}/content/fo/BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"

        file = self.__download(url, folder)

        if not file.is_file():
            file.unlink()
            raise FileNotFoundError(f"Failed to download file: {file.name}")

        return NSE.__unzip(file, file.parent)

    # def equity_bulk(self,  date: datetime) -> Path:
    #     folder = self.dir
    #     # from_date_str = (date-timedelta(days=1)).strftime("%d-%m-%Y")
    #     to_date_str = date.strftime("%d-%m-%Y")
    #     fname = f"bulk_{date.strftime('%d%m%Y')}.csv"

    #     # url = f"{NEW_ARCHIVE_URL}/content/equities/bulk.csv"
    #     url = f"{BASE_URL}/historical/bulk-deals?from={to_date_str}&to={to_date_str}&csv=true"

    #     file = self.__download(url, folder, fname=fname)

    #     if not file.is_file():
    #         file.unlink()
    #         raise FileNotFoundError(f"Failed to download file: {file.name}")

    #     return file

    # def equity_block(self, date: datetime) -> Path:
    #     folder = self.dir
    #     # from_date_str = (date-timedelta(days=1)).strftime("%d-%m-%Y")
    #     to_date_str = date.strftime("%d-%m-%Y")
    #     fname = f"block_{date.strftime('%d%m%Y')}.csv"

    #     # url = f"{NEW_ARCHIVE_URL}/content/equities/block.csv"
    #     url = f"{BASE_URL}/historical/block-deals?from={to_date_str}&to={to_date_str}&csv=true"

    #     file = self.__download(url, folder, fname=fname)

    #     if not file.is_file():
    #         file.unlink()
    #         raise FileNotFoundError(f"Failed to download file: {file.name}")

    #     return file

    def securities_bhavdata(self, date: datetime) -> Path:
        date_str = date.strftime("%d%m%Y").upper()
        month = date_str[2:5]

        folder = self.dir
        url = f"{NEW_ARCHIVE_URL}/products/content/sec_bhavdata_full_{date_str}.csv"

        file = self.__download(url, folder)

        if not file.is_file():
            file.unlink()
            raise FileNotFoundError(f"Failed to download file: {file.name}")

        return file

    def fo_combine_oi_delta_eq(self, date: datetime) -> Path:
        date_str = date.strftime("%d%m%Y").upper()
        folder = self.dir

        url = f"{NEW_ARCHIVE_URL}/archives/nsccl/mwpl/combineoi_deleq_{date_str}.csv"

        file = self.__download(url, folder)

        if not file.is_file():
            file.unlink()
            raise FileNotFoundError(f"Failed to download file: {file.name}")

        return file
    # import pdb;pdb.set_trace()
    def cm_market_data(self, date: datetime) -> Path:
        date_str = date.strftime("%d-%b-%Y")
        month = date_str[2:5]
        fname = f"ma{date.strftime('%d%m%y')}.csv"

        folder = self.dir
        # url = f"{NEW_ARCHIVE_URL}/products/content/sec_bhavdata_full_{date_str}.csv"
        url = """https://www.nseindia.com/api/reports?archives=[{"name":"CM - Market Activity Report","type":"archives","category":"capital-market","section":"equities"}]&date=""" + \
            date_str+"&type=equities&mode=single"

        file = self.__download(url, folder, fname)

        if not file.is_file():
            file.unlink()
            raise FileNotFoundError(f"Failed to download file: {file.name}")

        return file

    def add_equity_bulk_to_db(self, file: Path):
        csv_data = pd.read_csv(file, parse_dates=["Date "], dtype={
                               'Trade Price / Wght. Avg. Price ': str})
        csv_data["Quantity Traded "] = csv_data["Quantity Traded "].str.replace(
            ',', '').astype(int)
        csv_data["Trade Price / Wght. Avg. Price "] = csv_data["Trade Price / Wght. Avg. Price "].str.replace(
            ',', '').astype(float)
        csv_data.rename(columns={'Date ': 'date',
                                 'Symbol ': 'symbol',
                                 'Security Name ': 'security_name',
                                 'Client Name ': 'client_name',
                                 'Buy / Sell ': 'buy_sell',
                                 'Quantity Traded ': 'quantity_traded',
                                 'Trade Price / Wght. Avg. Price ': 'trade_price',
                                 'Remarks ': 'remarks'}, inplace=True)

        csv_data = csv_data.replace([pd.NaT], [None])
        csv_data = csv_data.replace([np.nan], [None])
        csv_data.date = csv_data.date.apply(
            lambda x: x.strftime('%Y-%m-%d') if x else x)
        csv_data_list = csv_data.to_dict(
            orient='records')

        print(len(csv_data_list))

        total = len(csv_data_list)
        limit = 1000
        offset = 0
        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={"secrete": settings.SECRET_KEY,
                                                                         "table_name": "equity_bulks", "new_data": csv_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")
            offset += limit
        file.unlink()

    def add_equity_block_to_db(self, file: Path):
        csv_data = pd.read_csv(file, parse_dates=["Date "], dtype={
                               'Trade Price / Wght. Avg. Price ': str})
        csv_data["Quantity Traded "] = csv_data["Quantity Traded "].str.replace(
            ',', '').astype(int)
        csv_data["Trade Price / Wght. Avg. Price "] = csv_data["Trade Price / Wght. Avg. Price "].str.replace(
            ',', '').astype(float)
        csv_data.rename(columns={'Date ': 'date',
                                 'Symbol ': 'symbol',
                                 'Security Name ': 'security_name',
                                 'Client Name ': 'client_name',
                                 'Buy / Sell ': 'buy_sell',
                                 'Quantity Traded ': 'quantity_traded',
                                 'Trade Price / Wght. Avg. Price ': 'trade_price',
                                 'Remarks ': 'remarks'}, inplace=True)
        csv_data = csv_data.replace([pd.NaT], [None])
        csv_data = csv_data.replace([np.nan], [None])
        csv_data.date = csv_data.date.apply(
            lambda x: x.strftime('%Y-%m-%d') if x else x)
        csv_data_list = csv_data.to_dict(
            orient='records')

        print(len(csv_data_list))

        total = len(csv_data_list)
        limit = 1000
        offset = 0
        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={"secrete": settings.SECRET_KEY,
                                                                         "table_name": "equity_blocks", "new_data": csv_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")
            offset += limit
        file.unlink()

    def add_equity_udiff_bhavcopy_to_db(self, file: Path):
        file_name = file.name.split('/')[-1]
        file_date = datetime.strptime(file_name.replace(
            'BhavCopy_NSE_CM_0_0_0_', '').replace('_F_0000.csv', ''), '%Y%m%d')
        if file_date <= datetime(2024, 12, 27):
            csv_data = pd.read_csv(
                file,
                parse_dates=["TradDt", "BizDt",
                             "XpryDt", "FininstrmActlXpryDt"],
                usecols=['TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN',
                         'TckrSymb', 'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric',
                         'OptnTp', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric',
                         'LastPric', 'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'OpnIntrst',
                         'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd',
                         'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd01', 'Rsvd02', 'Rsvd03', 'Rsvd04'])
            csv_data.rename(columns={'TradDt': 'trade_date',
                                     'BizDt': 'biz_date',
                                     'Sgmt': 'sgmt',
                                     'Src': 'src',
                                     'FinInstrmTp': 'fin_instrm_tp',
                                     'FinInstrmId': 'fin_instrm_id',
                                     'ISIN': 'isin',
                                     'TckrSymb': 'tckr_symb',
                                     'SctySrs': 'scty_srs',
                                     'XpryDt': 'xpry_date',
                                     'FininstrmActlXpryDt': 'fininstrm_actl_xpry_date',
                                     'StrkPric': 'strk_price',
                                     'OptnTp': 'optn_tp',
                                     'FinInstrmNm': 'fin_instrm_nm',
                                     'OpnPric': 'open_price',
                                     'HghPric': 'high_price',
                                     'LwPric': 'low_price',
                                     'ClsPric': 'close_price',
                                     'LastPric': 'last_price',
                                     'PrvsClsgPric': 'prev_closing_price',
                                     'UndrlygPric': 'undrlyg_price',
                                     'SttlmPric': 'sttlm_price',
                                     'OpnIntrst': 'opn_intrst',
                                     'ChngInOpnIntrst': 'chng_in_opn_intrst',
                                     'TtlTradgVol': 'total_trade_volume',
                                     'TtlTrfVal': 'ttl_trf_val',
                                     'TtlNbOfTxsExctd': 'ttl_nb_of_txs_exctd',
                                     'SsnId': 'ssn_id',
                                     'NewBrdLotQty': 'new_brd_lot_qty',
                                     'Rmks': 'rmks',
                                     'Rsvd01': 'rsvd_01',
                                     'Rsvd02': 'rsvd_02',
                                     'Rsvd03': 'rsvd_03',
                                     'Rsvd04': 'rsvd_04'
                                     },
                            inplace=True)
        else:
            csv_data = pd.read_csv(
                file,
                parse_dates=["TradDt", "BizDt",
                             "XpryDt", "FininstrmActlXpryDt"],
                usecols=['TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN',
                         'TckrSymb', 'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric',
                         'OptnTp', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric',
                         'LastPric', 'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'OpnIntrst',
                         'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd',
                         'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd1', 'Rsvd2', 'Rsvd3', 'Rsvd4'])
            csv_data.rename(columns={'TradDt': 'trade_date',
                                     'BizDt': 'biz_date',
                                     'Sgmt': 'sgmt',
                                     'Src': 'src',
                                     'FinInstrmTp': 'fin_instrm_tp',
                                     'FinInstrmId': 'fin_instrm_id',
                                     'ISIN': 'isin',
                                     'TckrSymb': 'tckr_symb',
                                     'SctySrs': 'scty_srs',
                                     'XpryDt': 'xpry_date',
                                     'FininstrmActlXpryDt': 'fininstrm_actl_xpry_date',
                                     'StrkPric': 'strk_price',
                                     'OptnTp': 'optn_tp',
                                     'FinInstrmNm': 'fin_instrm_nm',
                                     'OpnPric': 'open_price',
                                     'HghPric': 'high_price',
                                     'LwPric': 'low_price',
                                     'ClsPric': 'close_price',
                                     'LastPric': 'last_price',
                                     'PrvsClsgPric': 'prev_closing_price',
                                     'UndrlygPric': 'undrlyg_price',
                                     'SttlmPric': 'sttlm_price',
                                     'OpnIntrst': 'opn_intrst',
                                     'ChngInOpnIntrst': 'chng_in_opn_intrst',
                                     'TtlTradgVol': 'total_trade_volume',
                                     'TtlTrfVal': 'ttl_trf_val',
                                     'TtlNbOfTxsExctd': 'ttl_nb_of_txs_exctd',
                                     'SsnId': 'ssn_id',
                                     'NewBrdLotQty': 'new_brd_lot_qty',
                                     'Rmks': 'rmks',
                                     'Rsvd1': 'rsvd_01',
                                     'Rsvd2': 'rsvd_02',
                                     'Rsvd3': 'rsvd_03',
                                     'Rsvd4': 'rsvd_04'
                                     },
                            inplace=True)

        csv_data = csv_data.replace([pd.NaT], [None])
        csv_data = csv_data.replace([np.nan], [None])

        csv_data.trade_date = (
            csv_data.trade_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))
        csv_data.biz_date = (
            csv_data.biz_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))
        csv_data.xpry_date = (
            csv_data.xpry_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))
        csv_data.fininstrm_actl_xpry_date = (
            csv_data.fininstrm_actl_xpry_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))

        csv_data_list = csv_data.to_dict(
            orient='records')

        print(len(csv_data_list))

        total = len(csv_data_list)
        limit = 1000
        offset = 0
        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={"secrete": settings.SECRET_KEY,
                                                                         "table_name": "cm_udiff_bhavdata", "new_data": csv_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")
            offset += limit
        file.unlink()

    def add_fo_udiff_bhavcopy_to_db(self, file: Path):
        file_name = file.name.split('/')[-1]
        file_date = datetime.strptime(file_name.replace('BhavCopy_NSE_FO_0_0_0_', '').replace('_F_0000.csv', ''),
                                      '%Y%m%d')
        if file_date <= datetime(2024, 12, 27):
            csv_data = pd.read_csv(
                file,
                parse_dates=["TradDt", "BizDt",
                             "XpryDt", "FininstrmActlXpryDt"],
                usecols=['TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN',
                         'TckrSymb', 'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric',
                         'OptnTp', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric',
                         'LastPric', 'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'OpnIntrst',
                         'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd',
                         'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd01', 'Rsvd02', 'Rsvd03', 'Rsvd04'])
            csv_data.rename(columns={'TradDt': 'trade_date',
                                     'BizDt': 'biz_date',
                                     'Sgmt': 'sgmt',
                                     'Src': 'src',
                                     'FinInstrmTp': 'fin_instrm_tp',
                                     'FinInstrmId': 'fin_instrm_id',
                                     'ISIN': 'isin',
                                     'TckrSymb': 'tckr_symb',
                                     'SctySrs': 'scty_srs',
                                     'XpryDt': 'xpry_date',
                                     'FininstrmActlXpryDt': 'fininstrm_actl_xpry_date',
                                     'StrkPric': 'strk_price',
                                     'OptnTp': 'optn_tp',
                                     'FinInstrmNm': 'fin_instrm_nm',
                                     'OpnPric': 'open_price',
                                     'HghPric': 'high_price',
                                     'LwPric': 'low_price',
                                     'ClsPric': 'close_price',
                                     'LastPric': 'last_price',
                                     'PrvsClsgPric': 'prev_closing_price',
                                     'UndrlygPric': 'undrlyg_price',
                                     'SttlmPric': 'sttlm_price',
                                     'OpnIntrst': 'opn_intrst',
                                     'ChngInOpnIntrst': 'chng_in_opn_intrst',
                                     'TtlTradgVol': 'total_trade_volume',
                                     'TtlTrfVal': 'ttl_trf_val',
                                     'TtlNbOfTxsExctd': 'ttl_nb_of_txs_exctd',
                                     'SsnId': 'ssn_id',
                                     'NewBrdLotQty': 'new_brd_lot_qty',
                                     'Rmks': 'rmks',
                                     'Rsvd01': 'rsvd_01',
                                     'Rsvd02': 'rsvd_02',
                                     'Rsvd03': 'rsvd_03',
                                     'Rsvd04': 'rsvd_04'
                                     },
                            inplace=True)
        else:
            csv_data = pd.read_csv(
                file,
                parse_dates=["TradDt", "BizDt",
                             "XpryDt", "FininstrmActlXpryDt"],
                usecols=['TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN',
                         'TckrSymb', 'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric',
                         'OptnTp', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric',
                         'LastPric', 'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'OpnIntrst',
                         'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd',
                         'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd1', 'Rsvd2', 'Rsvd3', 'Rsvd4'])
            csv_data.rename(columns={'TradDt': 'trade_date',
                                     'BizDt': 'biz_date',
                                     'Sgmt': 'sgmt',
                                     'Src': 'src',
                                     'FinInstrmTp': 'fin_instrm_tp',
                                     'FinInstrmId': 'fin_instrm_id',
                                     'ISIN': 'isin',
                                     'TckrSymb': 'tckr_symb',
                                     'SctySrs': 'scty_srs',
                                     'XpryDt': 'xpry_date',
                                     'FininstrmActlXpryDt': 'fininstrm_actl_xpry_date',
                                     'StrkPric': 'strk_price',
                                     'OptnTp': 'optn_tp',
                                     'FinInstrmNm': 'fin_instrm_nm',
                                     'OpnPric': 'open_price',
                                     'HghPric': 'high_price',
                                     'LwPric': 'low_price',
                                     'ClsPric': 'close_price',
                                     'LastPric': 'last_price',
                                     'PrvsClsgPric': 'prev_closing_price',
                                     'UndrlygPric': 'undrlyg_price',
                                     'SttlmPric': 'sttlm_price',
                                     'OpnIntrst': 'opn_intrst',
                                     'ChngInOpnIntrst': 'chng_in_opn_intrst',
                                     'TtlTradgVol': 'total_trade_volume',
                                     'TtlTrfVal': 'ttl_trf_val',
                                     'TtlNbOfTxsExctd': 'ttl_nb_of_txs_exctd',
                                     'SsnId': 'ssn_id',
                                     'NewBrdLotQty': 'new_brd_lot_qty',
                                     'Rmks': 'rmks',
                                     'Rsvd1': 'rsvd_01',
                                     'Rsvd2': 'rsvd_02',
                                     'Rsvd3': 'rsvd_03',
                                     'Rsvd4': 'rsvd_04'
                                     },
                            inplace=True)

        report_data_list = []
        for index, row in csv_data.iterrows():
            if row['fin_instrm_tp'] in ['STO', 'STF']:
                xpry_date = row['xpry_date'].strftime('%Y-%m-%d')
                s_date = start_dates.get(xpry_date)
                if not s_date:
                    s_date = get_instrumet_start_day(xpry_date)
                    _resp = requests.post(f"{settings.API_URL}/update-db", json={"secrete": settings.SECRET_KEY,
                                                                                 "table_name": "start_date_lookup", "new_data": [{"end_date": xpry_date, "start_date": s_date}]})
                    print(_resp.status_code)
                    print(_resp.ok)
                    print(_resp.text)
                    start_dates[xpry_date] = s_date
                current_trade_date = row['trade_date'].strftime('%Y-%m-%d')
                report_data = {}
                report_data['name'] = row['fin_instrm_nm']
                report_data['type'] = row['fin_instrm_tp']
                report_data['ticker_symbol'] = row['tckr_symb']
                report_data['start_date'] = s_date
                report_data['expiry_date'] = xpry_date
                report_data['current_date'] = current_trade_date
                report_data['days_elapsed_since_birth'] = (datetime.strptime(current_trade_date, '%Y-%m-%d') - datetime.strptime(
                    s_date, '%Y-%m-%d')).days + 1
                if row['chng_in_opn_intrst'] > 0:
                    report_data['first_trade_date'] = current_trade_date
                    report_data['days_active'] = 1
                    report_data['percentage_active'] = 100 * \
                        report_data['days_active'] / \
                        report_data['days_elapsed_since_birth']
                    report_data['first_trade_underlying_price'] = row['undrlyg_price']
                    report_data["first_trade_close_price"] = row['close_price']
                report_data["latest_close_price"] = row['close_price']
                report_data["latest_opn_intrst_lot"] = row[
                    'opn_intrst']/row['new_brd_lot_qty']
                report_data["latest_chng_opn_intrst_lot"] = row[
                    'chng_in_opn_intrst']/row['new_brd_lot_qty']
                report_data_list.append(report_data)
                csv_data.at[index, 'start_date'] = s_date
            else:
                csv_data.at[index, 'start_date'] = pd.NaT

        csv_data = csv_data.replace([pd.NaT], [None])
        csv_data = csv_data.replace([np.nan], [None])

        csv_data.start_date = (
            csv_data.start_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x and isinstance(x, datetime) else x))
        csv_data.trade_date = (
            csv_data.trade_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))
        csv_data.biz_date = (
            csv_data.biz_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))
        csv_data.xpry_date = (
            csv_data.xpry_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))
        csv_data.fininstrm_actl_xpry_date = (
            csv_data.fininstrm_actl_xpry_date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))

        csv_data_list = csv_data.to_dict(orient='records')

        print(len(csv_data_list))

        total = len(csv_data_list)
        limit = 1000
        offset = 0
        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={
                "secrete": settings.SECRET_KEY,
                "table_name": "fo_udiff_bhavdata", "new_data": csv_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")
            offset += limit

        print(len(report_data_list))

        total = len(report_data_list)
        limit = 1000
        offset = 0
        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={
                "secrete": settings.SECRET_KEY,
                "table_name": "fo_stock_intstruments_report_1", "new_data": report_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")
            offset += limit

        file.unlink()

    def add_securities_bhavdata_to_db(self, file: Path):
        csv_data = pd.read_csv(file, parse_dates=[" DATE1"])
        csv_data.replace(' ', np.nan, inplace=True)
        csv_data.replace(' -', np.nan, inplace=True)

        csv_data.rename(columns={' DATE1': 'date',
                                 ' SYMBOL': 'symbol',
                                 ' SERIES': 'series',
                                 ' PREV_CLOSE': 'prev_close',
                                 ' OPEN_PRICE': 'open_price',
                                 ' HIGH_PRICE': 'high_price',
                                 ' LOW_PRICE': 'low_price',
                                 ' LAST_PRICE': 'last_price',
                                 ' CLOSE_PRICE': 'close_price',
                                 ' AVG_PRICE': 'avg_price',
                                 ' TTL_TRD_QNTY': 'total_trade_quantity',
                                 ' TURNOVER_LACS': 'turnover_lacs',
                                 ' NO_OF_TRADES': 'number_of_trades',
                                 ' DELIV_QTY': 'delivery_quantity',
                                 ' DELIV_PER': 'delivery_percentage'}, inplace=True)
        csv_data = csv_data.replace([pd.NaT], [None])
        csv_data = csv_data.replace([np.nan], [None])

        csv_data.date = (
            csv_data.date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))

        csv_data_list = csv_data.to_dict(orient='records')

        print(len(csv_data_list))

        total = len(csv_data_list)
        limit = 1000
        offset = 0
        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={
                "secrete": settings.SECRET_KEY,
                "table_name": "securities_bhavdata", "new_data": csv_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")

            offset += limit

        file.unlink()

    def add_fo_combine_oi_delta_eq_to_db(self, file: Path, data_date: date):
        csv_data = pd.read_csv(file)
        csv_data['Date'] = data_date
        csv_data.replace(' ', np.nan, inplace=True)
        csv_data.replace(' -', np.nan, inplace=True)
        csv_data.rename(columns={'Date': 'date',
                                 'ISIN': 'isin',
                                 'Scrip Name': 'script_name',
                                 'Symbol': 'symbol',
                                 'Open Interest': 'open_interest',
                                 'Delta Equivalent Open Interest Contract wise': 'delta_equivalent_open_interest_contract_wise',
                                 'Delta Equivalent Open Interest Portfolio wise': 'delta_equivalent_open_interest_portfolio_wise'
                                 }, inplace=True)
        csv_data = csv_data.replace([pd.NaT], [None])
        csv_data = csv_data.replace([np.nan], [None])
        csv_data.date = (
            csv_data.date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))
        csv_data_list = csv_data.to_dict(orient='records')

        print(len(csv_data_list))

        total = len(csv_data_list)
        limit = 1000
        offset = 0

        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={
                "secrete": settings.SECRET_KEY,
                "table_name": "fo_combine_oi_delta_equivalent", "new_data": csv_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")

            offset += limit
        file.unlink()

    def add_cm_market_data_to_db(self, file: Path, verify_in_db: bool = True):
        with open(file) as f:
            data_date = datetime.strptime(
                (f.readline().strip(',').strip()), '%d-%b-%Y')
            print(data_date)
            # Initialize variables
            skip_rows = 5  # (8 - 2)
            rows_skipped = 0
            header = None
            index_data = []
            processing = False
            for line in f:
                stripped_line = line.strip()
                if rows_skipped < skip_rows:
                    rows_skipped += 1
                    continue
                if not processing and stripped_line == '':
                    processing = True
                    continue

                if processing and stripped_line == '':
                    break
                if processing:
                    row = stripped_line.split(',')[1:]
                    if not header:
                        header = [x.strip() for x in row]
                    else:
                        index_data.append(row)
            csv_data = pd.DataFrame(index_data, columns=header)

            csv_data = csv_data.astype({
                'INDEX': 'str',
                'PREVIOUS CLOSE': 'float',
                'OPEN': 'float',
                'HIGH': 'float',
                'LOW': 'float',
                'CLOSE': 'float',
                'GAIN/LOSS': 'float'})
            csv_data['date'] = data_date
            csv_data['percentage_change'] = (
                (csv_data['CLOSE']/csv_data['PREVIOUS CLOSE']) - 1) * 100
            nifty_50_change = csv_data[csv_data['INDEX']
                                       == 'Nifty 50']['percentage_change'].values[0]
            csv_data['relative_strength'] = csv_data['percentage_change'] - \
                nifty_50_change

            csv_data['relative_performance_ratio'] = csv_data['percentage_change'] / \
                nifty_50_change

        csv_data.rename(columns={
            'INDEX': 'index',
            'PREVIOUS CLOSE': 'prev_closing_price',
            'OPEN': 'open_price',
            'HIGH': 'high_price',
            'LOW': 'low_price',
            'CLOSE': 'close_price',
            'GAIN/LOSS': 'gain_or_loss'}, inplace=True)
        csv_data.replace([np.inf, -np.inf, ' ', ' -', '-'],
                         np.nan, inplace=True)
        csv_data.date = (
            csv_data.date.apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))

        csv_data_list = csv_data.to_dict(orient='records')

        print(len(csv_data_list))

        total = len(csv_data_list)
        limit = 1000
        offset = 0

        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={
                "secrete": settings.SECRET_KEY,
                "table_name": "cm_market_data_indexes", "new_data": csv_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")

            offset += limit
        file.unlink()

    def add_cm_index_data_to_db(self, file: Path, verify_in_db: bool = True):
        csv_data = pd.read_csv(
            file,
            parse_dates=["Index Date"],
            usecols=["Index Name",
                     "Index Date",
                     "Open Index Value",
                     "High Index Value",
                     "Low Index Value",
                     "Closing Index Value",
                     "Points Change",
                     "Change(%)",
                     "Volume",
                     "Turnover (Rs. Cr.)",
                     "P/E",
                     "P/B",
                     "Div Yield"], dayfirst=True)
        # print(csv_data.columns)
        csv_data['Index Date'] = pd.to_datetime(
            csv_data['Index Date'], format='%d-%m-%Y')
        csv_data = csv_data.dropna(subset=['Index Date'])
        csv_data.rename(columns={
            "Index Name": 'index_name',
            "Index Date": 'index_date',
            "Open Index Value": 'open_index_value',
            "High Index Value": 'high_index_value',
            "Low Index Value": 'low_index_value',
            'Closing Index Value': 'closing_index_value',
            'Points Change': 'points_change',
            'Change(%)': 'percentage_change',
            'Volume': 'volume',
            'Turnover (Rs. Cr.)': 'turnover_in_crores',
            'P/E': 'p_e',
            'P/B': 'p_b',
            'Div Yield': 'div_yield'
        }, inplace=True)
        csv_data['index_date'] = (
            csv_data['index_date'].apply(
                lambda x: x.strftime('%Y-%m-%d') if x else x))
        csv_data = csv_data.replace([pd.NaT, np.nan, '-'], None)

        csv_data_list = csv_data.to_dict(orient='records')

        print(len(csv_data_list))

        total = len(csv_data_list)
        limit = 1000
        offset = 0

        while offset < total:
            _resp = requests.post(f"{settings.API_URL}/update-db", json={
                "secrete": settings.SECRET_KEY,
                "table_name": "cm_index_data", "new_data": csv_data_list[offset:limit+offset]})
            print(_resp.status_code)
            print(_resp.ok)
            print(_resp.text)
            if _resp.status_code == 500:
                file.unlink()
                raise FileNotFoundError(
                    f"Something went wrong while uploading: {file.name}")
            offset += limit
        file.unlink()


def send_email(subject, body, to_emails):
    # Gmail account credentials
    # gmail_user = 'ayyappa.chodisetty@cytrion.com'
    # gmail_password = 'qrop lwbs gigm rbgc'

    # Create the container (outer) email message
    msg = MIMEMultipart()
    msg['From'] = settings.EMAILS_FROM_EMAIL
    msg['Subject'] = subject
    msg['To'] = ', '.join(to_emails)

    # Attach the email body to the message
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connect to the Gmail SMTP server
        server = smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT)
        server.starttls()  # Upgrade the connection to a secure encrypted SSL/TLS connection

        # Login to the Gmail account
        server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)

        # Send the email
        server.sendmail(settings.EMAILS_FROM_EMAIL,
                        to_emails, msg.as_string())

        # Disconnect from the server
        server.quit()

        print('Email sent successfully!')

    except Exception as e:
        print(f'Failed to send email: {e}')


@cache
def get_holidays():
    holidays_url = f"{settings.API_URL}/get-holidays"
    resp = requests.get(holidays_url)
    print(resp.status_code)
    print(resp.ok)
    print(resp.json())

    holidays = resp.json()
    return pd.to_datetime(holidays)


def get_dates(table_name):
    last_updated_date_url = f"{settings.API_URL}/get-db-last-update-date"
    resp = requests.post(last_updated_date_url, json={
                         "secrete": settings.SECRET_KEY, 'table_name': table_name})

    # print(resp.status_code)
    # print(resp.ok)
    # print(resp.json())
    _last_updated = None
    if resp.ok:
        _last_updated = resp.json().get('last_update_date')
    if not _last_updated:
        _last_updated = datetime.now().strftime('%Y-%m-%d')
    else:
        # _last_updated = (datetime.strptime(
        #     _last_updated, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        _last_updated = (datetime.strptime(
            _last_updated, '%Y-%m-%d')).strftime('%Y-%m-%d')
    holidays = get_holidays()
    dates = pd.date_range(start=_last_updated,
                          end=datetime.now().strftime('%Y-%m-%d'),
                          #   end='2024-11-02',

                          freq='B')
    dates = dates[~dates.isin(holidays)]
    return dates


if __name__ == "__main__":
    list_of_errors = {}
    for table_name in tables_to_get:
        print(table_name)
        for data_date in get_dates(table_name):
            print(data_date)
            list_of_errors.setdefault(data_date.strftime('%Y-%m-%d'), [])
            x = NSE()
            try:
                data = x.download_data(table_name, data_date)
                print("file_name:", data)
                if table_name == 'cm_udiff_bhavdata':
                    x.add_equity_udiff_bhavcopy_to_db(data)
                elif table_name == 'securities_bhavdata':
                    x.add_securities_bhavdata_to_db(data)
                elif table_name == 'equity_blocks':
                    x.add_equity_block_to_db(data)
                elif table_name == 'equity_bulks':
                    x.add_equity_bulk_to_db(data)
                elif table_name == 'fo_udiff_bhavdata':
                    x.add_fo_udiff_bhavcopy_to_db(data)
                elif table_name == 'fo_combine_oi_delta_equivalent':
                    x.add_fo_combine_oi_delta_eq_to_db(data, data_date)
                elif table_name == 'cm_market_data_indexes':
                    x.add_cm_market_data_to_db(data)
                elif table_name == 'cm_index_data':
                    x.add_cm_index_data_to_db(data)

            except Exception as e:
                print(e)
                err = True
                if table_name != 'fo_combine_oi_delta_equivalent':
                    list_of_errors[data_date.strftime('%Y-%m-%d')].append(e)

    updated_db_url = f"{settings.API_URL}/update-db"
    resp = requests.post(updated_db_url, json={
        "secrete": settings.SECRET_KEY,
        'table_name': 'last_updated_date',
        'new_data': []})
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print(resp.status_code)
    print(resp.ok)
    print(resp.json())
    print(list_of_errors)
    if list_of_errors[datetime.now().strftime('%Y-%m-%d')]:
        print(list_of_errors)
        body_str = """Execution failed.\n\nRan into errors.

"""+"""
""".join(
            [str(e) for e in list_of_errors[datetime.now().strftime('%Y-%m-%d')]]
        )
        send_email(f"ERROR NSE data sync: {datetime.now().strftime('%Y-%m-%d')}", body_str,
                   ['gottamdharani7626@gmail.com'])
    else:
        body_str = f"""Execution sucessfull.
        Data has been successfully synced.
        Now the data is up to date as of {datetime.now().strftime('%Y-%m-%d')}"""
        send_email(f"NSE data sync: {datetime.now().strftime('%Y-%m-%d')}", body_str,
                   ['gottamdharani7626@gmail.com'])
# 'bhargav.m@pagesolutions.co.uk', 'kpdasari@gmail.com',
    print('All data csv report uploading started')

    for data_date in get_dates('all_data_csv_report'):
        try:
            all_data_csv_main(data_date, upload_to_remote_server=True)
        except Exception as e:
            print(e)
    print('All data csv report uploading finished')
    
    
    
    print('Synthetic generator data report uploading started')

    for data_date in get_dates('synthetic_generator_data_report'):
        try:
            synthetic_generator_data_report(data_date, upload_to_remote_server=True)
        except Exception as e:
            print(e)

    print('Synthetic generator data report uploading finished')

    
    
    # print('Synthetic generator data report uploading started')
    
    # for data_date in get_dates('synthetic_generator_data_report'):
    #     try:
    #         synthetic_generator_data_report(data_date, upload_to_remote_server=True)
    #     except Exception as e:
    #         print(e)
    # print('Synthetic generator data report uploading finished')
    
# {'status': 'success', 'data': [{'date': '2025-02-01', 'description': 'Budget Day Session', 
# 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': [], 
# 'open_exchanges': [{'exchange': 'NSE', 'start_time': 1738381500000, 'end_time': 1738404000000}, 
# {'exchange': 'NFO', 'start_time': 1738381500000, 'end_time': 1738404000000}, 
# {'exchange': 'CDS', 'start_time': 1738380600000, 'end_time': 1738409400000}, 
# {'exchange': 'BSE', 'start_time': 1738381500000, 'end_time': 1738404000000}, 
# {'exchange': 'BFO', 'start_time': 1738381500000, 'end_time': 1738404000000}, 
# {'exchange': 'BCD', 'start_time': 1738380600000, 'end_time': 1738409400000}, 
# {'exchange': 'MCX', 'start_time': 1738380600000, 'end_time': 1738409400000}, 
# {'exchange': 'NSCOM', 'start_time': 1738380600000, 'end_time': 1738409400000}]}, 
# {'date': '2025-02-26', 'description': 'Mahashivratri', 'holiday_type': 'TRADING_HOLIDAY', 
# 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 
# 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1740569400000, 'end_time': 1740594300000}, 
# {'exchange': 'NSCOM', 'start_time': 1740569400000, 'end_time': 1740594300000}]}, 
# {'date': '2025-03-14', 'description': 'Holi', 'holiday_type': 'TRADING_HOLIDAY', 
# 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 
# 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1741951800000, 'end_time': 1741975200000}, 
# {'exchange': 'NSCOM', 'start_time': 1741951800000, 'end_time': 1741975200000}]}, 
# {'date': '2025-03-31', 'description': 'Id-Ul-Fitr (Ramadan Eid)', 'holiday_type': 'TRADING_HOLIDAY', 
# 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1743420600000, 
# 'end_time': 1743444000000}, {'exchange': 'NSCOM', 'start_time': 1743420600000, 'end_time': 1743444000000}]}, 
# {'date': '2025-04-01', 'description': 'Annual Bank Closing', 'holiday_type': 'SETTLEMENT_HOLIDAY', 'closed_exchanges': ['CDS', 'BCD'], 'open_exchanges': [{'exchange': 'NSE', 'start_time': 1743479100000, 'end_time': 1743501600000}, {'exchange': 'NFO', 'start_time': 1743479100000, 'end_time': 1743501600000}, {'exchange': 'BSE', 'start_time': 1743479100000, 'end_time': 1743501600000}, {'exchange': 'BFO', 'start_time': 1743479100000, 'end_time': 1743501600000}, {'exchange': 'MCX', 'start_time': 1743478200000, 'end_time': 1743530400000}, {'exchange': 'NSCOM', 'start_time': 1743478200000, 'end_time': 1743530400000}]}, {'date': '2025-04-10', 'description': 'Shri Mahavir Jayanti', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1744284600000, 'end_time': 1744308000000}, {'exchange': 'NSCOM', 'start_time': 1744284600000, 'end_time': 1744308000000}]}, {'date': '2025-04-14', 'description': 'Dr. Baba Saheb Ambedkar Jayanti', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1744630200000, 'end_time': 1744653600000}, {'exchange': 'NSCOM', 'start_time': 1744630200000, 'end_time': 1744653600000}]}, {'date': '2025-04-18', 'description': 'Good Friday', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD', 'MCX', 'NSCOM'], 'open_exchanges': []}, {'date': '2025-05-01', 'description': 'Maharashtra Day', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1746099000000, 'end_time': 1746122400000}, {'exchange': 'NSCOM', 'start_time': 1746099000000, 'end_time': 1746122400000}]}, {'date': '2025-08-15', 'description': 'Independence Day', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD', 'MCX', 'NSCOM'], 'open_exchanges': []}, {'date': '2025-08-27', 'description': 'Ganesh Chaturthi', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1756294200000, 'end_time': 1756317600000}, {'exchange': 'NSCOM', 'start_time': 1756294200000, 'end_time': 1756317600000}]}, {'date': '2025-10-02', 'description': 'Mahatma Gandhi Jayanti/Dussehra', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD', 'MCX', 'NSCOM'], 'open_exchanges': []}, {'date': '2025-10-21', 'description': 'Diwali Laxmi Pujan', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': [], 'open_exchanges': [{'exchange': 'NSE', 'start_time': 1761049800000, 'end_time': 1761053400000}, {'exchange': 'NFO', 'start_time': 1761049800000, 'end_time': 1761053400000}, {'exchange': 'CDS', 'start_time': 1761049800000, 'end_time': 1761053400000}, {'exchange': 'BSE', 'start_time': 1761049800000, 'end_time': 1761053400000}, {'exchange': 'BFO', 'start_time': 1761049800000, 'end_time': 1761053400000}, {'exchange': 'BCD', 'start_time': 1761049800000, 'end_time': 1761053400000}, {'exchange': 'MCX', 'start_time': 1761049800000, 'end_time': 1761053400000}, {'exchange': 'NSCOM', 'start_time': 1761049800000, 'end_time': 1761053400000}]}, {'date': '2025-10-22', 'description': 'Diwali-Balipratipada', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1761132600000, 'end_time': 1761156000000}, {'exchange': 'NSCOM', 'start_time': 1761132600000, 'end_time': 1761156000000}]}, {'date': '2025-11-05', 'description': 'Prakash Gurpurb Sri Guru Nanak Dev', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD'], 'open_exchanges': [{'exchange': 'MCX', 'start_time': 1762342200000, 'end_time': 1762367100000}, {'exchange': 'NSCOM', 'start_time': 1762342200000, 'end_time': 1762367100000}]}, {'date': '2025-12-25', 'description': 'Christmas', 'holiday_type': 'TRADING_HOLIDAY', 'closed_exchanges': ['NSE', 'NFO', 'CDS', 'BSE', 'BFO', 'BCD', 'MCX', 'NSCOM'], 'open_exchanges': []}]}