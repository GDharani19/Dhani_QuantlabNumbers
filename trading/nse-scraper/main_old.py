import copy

import numpy as np
from requests import Session, ReadTimeout
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
from db import engine, get_db
from load_start_days import get_instrumet_start_day
from models.cm_market_data_indexes import CMMarketDataIndexes
from models.equity_block import EquityBlock
from models.equity_bulk import EquityBulk
from models.fo_combine_oi_delta_equivalent import FOCombineOIDeltaEquivalent
from models.fo_stock_intstruments_report_1 import FOStockInstrumentsReport1
from models.last_updated_date import LastUpdatedDate
from models.securities_bhavdata import SecurityiesBhavData
from models.cm_udiff_bhavdata import CMUDIFFBhavData
from models.fo_udiff_bhavdata import FOUDIFFBhavData
from models.start_date_lookup import StartDateLookup
from user_agents_list import user_agents
import time
from constants import mst_table_mapping

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

with next(get_db()) as _db_session:
    start_dates: Dict[str, str] = {_date.end_date.strftime(
        '%Y-%m-%d'): _date.start_date.strftime('%Y-%m-%d') for _date in _db_session.query(StartDateLookup).all()}


class NSE:
    def __init__(self):
        self.dir = Path(temp_folder)

        self.cookie_path = self.dir / "nse_cookies.pkl"
        self.session = Session()

        self.session.headers.update(random.choice(headers))
        self.session.cookies.update(self.__get_cookies())

    @staticmethod
    def __get_db_session():
        return next(get_db())

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
        return fname

    # def equity_bhavcopy(self, date: datetime) -> Path:
    #     # deprecated
    #     date_str = date.strftime("%d%b%Y").upper()
    #     month = date_str[2:5]
    #
    #     folder = self.dir
    #
    #     url = f"{ARCHIVE_URL}/content/historical/EQUITIES/{date.year}/{month}/cm{date_str}bhav.csv.zip"
    #
    #     file = self.__download(url, folder)
    #
    #     if not file.is_file():
    #         file.unlink()
    #         raise FileNotFoundError(f"Failed to download file: {file.name}")
    #
    #     return NSE.__unzip(file, file.parent)

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

    def equity_bulk(self,  date: datetime) -> Path:
        folder = self.dir
        # from_date_str = (date-timedelta(days=1)).strftime("%d-%m-%Y")
        to_date_str = date.strftime("%d-%m-%Y")
        fname = f"bulk_{date.strftime('%d%m%Y')}.csv"

        # url = f"{NEW_ARCHIVE_URL}/content/equities/bulk.csv"
        url = f"{BASE_URL}/historical/bulk-deals?from={to_date_str}&to={to_date_str}&csv=true"

        file = self.__download(url, folder, fname=fname)

        if not file.is_file():
            file.unlink()
            raise FileNotFoundError(f"Failed to download file: {file.name}")

        return file

    def equity_block(self, date: datetime) -> Path:
        folder = self.dir
        # from_date_str = (date-timedelta(days=1)).strftime("%d-%m-%Y")
        to_date_str = date.strftime("%d-%m-%Y")
        fname = f"block_{date.strftime('%d%m%Y')}.csv"

        # url = f"{NEW_ARCHIVE_URL}/content/equities/block.csv"
        url = f"{BASE_URL}/historical/block-deals?from={to_date_str}&to={to_date_str}&csv=true"

        file = self.__download(url, folder, fname=fname)

        if not file.is_file():
            file.unlink()
            raise FileNotFoundError(f"Failed to download file: {file.name}")

        return file

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

    def add_equity_bulk_to_db(self, file: Path, verify_in_db: bool = True):
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
        if verify_in_db:
            csv_data = csv_data.replace(np.nan, None)
            # csv_data = csv_data.replace(pd.NaT, None)
            csv_data = csv_data.replace([pd.NaT], [None])
            self.add_data_to_db_helper(
                'equity_bulks', csv_data)
        else:
            csv_data.to_sql('equity_bulks', engine,
                            if_exists='append', index=False)
        file.unlink()

    def add_equity_block_to_db(self, file: Path, verify_in_db: bool = True):
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

        if verify_in_db:
            csv_data = csv_data.replace(np.nan, None)
            # csv_data = csv_data.replace(pd.NaT, None)
            csv_data = csv_data.replace([pd.NaT], [None])
            self.add_data_to_db_helper(
                'equity_blocks', csv_data)
        else:
            csv_data.to_sql('equity_blocks', engine,
                            if_exists='append', index=False)
        file.unlink()

    def add_equity_udiff_bhavcopy_to_db(self, file: Path, verify_in_db: bool = True):
        file_name = file.name.split('/')[-1]
        file_date = datetime.strptime(file_name.replace(
            'BhavCopy_NSE_CM_0_0_0_', '').replace('_F_0000.csv', ''), '%Y%m%d')
        if file_date <= datetime(2024, 6, 20):
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

        if verify_in_db:
            csv_data = csv_data.replace(np.nan, None)
            # csv_data = csv_data.replace(pd.NaT, None)
            csv_data = csv_data.replace([pd.NaT], [None])
            self.add_data_to_db_helper(
                'cm_udiff_bhavdata', csv_data)
        else:
            csv_data.to_sql('cm_udiff_bhavdata', engine,
                            if_exists='append', index=False)
        file.unlink()

    def add_fo_udiff_bhavcopy_to_db(self, file: Path, verify_in_db: bool = True):
        file_name = file.name.split('/')[-1]
        file_date = datetime.strptime(file_name.replace('BhavCopy_NSE_FO_0_0_0_', '').replace('_F_0000.csv', ''),
                                      '%Y%m%d')
        if file_date <= datetime(2024, 6, 20):
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

        with self.__get_db_session() as db_session:
            for index, row in csv_data.iterrows():
                if row['fin_instrm_tp'] in ['STO', 'STF']:
                    xpry_date = row['xpry_date'].strftime('%Y-%m-%d')
                    s_date = start_dates.get(xpry_date)
                    if not s_date:
                        s_date = get_instrumet_start_day(xpry_date)
                        db_session.add(StartDateLookup(
                            end_date=xpry_date, start_date=s_date))
                        start_dates[xpry_date] = s_date
                    current_trade_date = row['trade_date'].strftime('%Y-%m-%d')
                    fo_stock_intstruments_report = db_session.query(
                        FOStockInstrumentsReport1).filter(FOStockInstrumentsReport1.name == row['fin_instrm_nm']).first()
                    if fo_stock_intstruments_report:
                        fo_stock_intstruments_report.current_date = current_trade_date

                        fo_stock_intstruments_report.days_elapsed_since_birth = (datetime.strptime(current_trade_date, '%Y-%m-%d') - datetime.strptime(
                            s_date, '%Y-%m-%d')).days + 1

                        if fo_stock_intstruments_report.first_trade_date:
                            fo_stock_intstruments_report.days_active = (datetime.strptime(current_trade_date, '%Y-%m-%d') - datetime.strptime(fo_stock_intstruments_report.first_trade_date.strftime(
                                '%Y-%m-%d'), '%Y-%m-%d')).days + 1

                            fo_stock_intstruments_report.percentage_active = 100*fo_stock_intstruments_report.days_active / \
                                fo_stock_intstruments_report.days_elapsed_since_birth

                            fo_stock_intstruments_report.latest_close_price = row['close_price']

                            fo_stock_intstruments_report.latest_opn_intrst_lot = row[
                                'opn_intrst']/row['new_brd_lot_qty']

                            fo_stock_intstruments_report.latest_chng_opn_intrst_lot = row[
                                'chng_in_opn_intrst']/row['new_brd_lot_qty']

                        elif row['chng_in_opn_intrst'] > 0:
                            fo_stock_intstruments_report.first_trade_date = current_trade_date

                            fo_stock_intstruments_report.days_active = 1

                            fo_stock_intstruments_report.percentage_active = 100 * fo_stock_intstruments_report.days_active / \
                                fo_stock_intstruments_report.days_elapsed_since_birth

                            fo_stock_intstruments_report.first_trade_underlying_price = row[
                                'undrlyg_price']

                            fo_stock_intstruments_report.first_trade_close_price = row['close_price']

                            fo_stock_intstruments_report.latest_close_price = row['close_price']

                            fo_stock_intstruments_report.latest_opn_intrst_lot = row[
                                'opn_intrst']/row['new_brd_lot_qty']

                            fo_stock_intstruments_report.latest_chng_opn_intrst_lot = row[
                                'chng_in_opn_intrst']/row['new_brd_lot_qty']
                        db_session.add(fo_stock_intstruments_report)

                    else:
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
                        db_session.add(
                            FOStockInstrumentsReport1(**report_data))
                    csv_data.at[index, 'start_date'] = s_date
                else:
                    csv_data.at[index, 'start_date'] = pd.NaT

            db_session.commit()
        if verify_in_db:
            csv_data = csv_data.replace(np.nan, None)
            # csv_data = csv_data.replace(pd.NaT, None)
            csv_data = csv_data.replace([pd.NaT], [None])
            self.add_data_to_db_helper(
                'fo_udiff_bhavdata', csv_data)
        else:
            csv_data.to_sql('fo_udiff_bhavdata', engine,
                            if_exists='append', index=False)
        file.unlink()

    def add_securities_bhavdata_to_db(self, file: Path, verify_in_db: bool = True):
        csv_data = pd.read_csv(file, parse_dates=[" DATE1"])
        # csv_data[" DELIV_QTY"] = csv_data[" DELIV_QTY"].replace(' -', np.nan)
        # csv_data[" DELIV_PER"] = csv_data[" DELIV_PER"].replace(' -', np.nan)
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
        if verify_in_db:
            csv_data = csv_data.replace(np.nan, None)
            # csv_data = csv_data.replace(pd.NaT, None)
            csv_data = csv_data.replace([pd.NaT], [None])
            self.add_data_to_db_helper(
                'securities_bhavdata', csv_data)
        else:
            csv_data.to_sql('securities_bhavdata', engine,
                            if_exists='append', index=False)
        file.unlink()

    def add_fo_combine_oi_delta_eq_to_db(self, file: Path, data_date: date, verify_in_db: bool = True):
        csv_data = pd.read_csv(
            file)
        csv_data['Date'] = data_date
        # csv_data[" DELIV_QTY"] = csv_data[" DELIV_QTY"].replace(' -', np.nan)
        # csv_data[" DELIV_PER"] = csv_data[" DELIV_PER"].replace(' -', np.nan)
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
        if verify_in_db:
            csv_data = csv_data.replace(np.nan, None)
            # csv_data = csv_data.replace(pd.NaT, None)
            csv_data = csv_data.replace([pd.NaT], [None])

            self.add_data_to_db_helper(
                'fo_combine_oi_delta_equivalent', csv_data)
        else:
            csv_data.to_sql('fo_combine_oi_delta_equivalent', engine,
                            if_exists='append', index=False)
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

        # for row in (csv_data[['INDEX', 'relative_strength']]).itertuples(index=False):
        #     print(row._asdict())

        # print(csv_data[['INDEX', 'relative_strength']])

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
        # csv_data.replace(' ', np.nan, inplace=True)
        # csv_data.replace(' -', np.nan, inplace=True)
        # csv_data.replace('-', np.nan, inplace=True)
        # x = csv_data[['index', 'relative_strength']]
        # print(x[x['index'] == 'Nifty50 Div Point'])
        if verify_in_db:
            csv_data = csv_data.replace(np.nan, None)
            # csv_data = csv_data.replace(pd.NaT, None)
            csv_data = csv_data.replace([pd.NaT], [None])
            csv_data.date = (
                csv_data.date.apply(
                    lambda x: x.strftime('%Y-%m-%d') if x else x))
            self.add_data_to_db_helper('cm_market_data_indexes', csv_data)
        else:
            csv_data.to_sql('cm_market_data_indexes', engine,
                            if_exists='append', index=False)
        file.unlink()

    def add_data_to_db_helper(self, model_name, new_data):
        mdl = mst_table_mapping[model_name]['model']
        clmns = mst_table_mapping[model_name]['filters']
        with self.__get_db_session() as db_session:
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


if __name__ == "__main__":
    holidays_query = """

    SELECT DATE_FORMAT(tradingDate, '%Y-%m-%d') as trading_date FROM `holidays` WHERE tradingDate > DATE_SUB(CURDATE(),INTERVAL 1 YEAR);

    """
    holidays_data = pd.read_sql(
        text(holidays_query),
        engine, parse_dates=['trading_date'],
        # date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d')
    )

    holidays = holidays_data.trading_date

    # holidays = pd.to_datetime(holidays)
    with next(get_db()) as db_session:
        _last_updated = db_session.execute(
            text('select max(last_updated_date) from last_updated_date')).scalar()
        if not _last_updated:
            _last_updated = db_session.execute(
                text('select max(trade_date) from fo_udiff_bhavdata')).scalar()
    # print(_last_updated)
    _last_updated = _last_updated + timedelta(days=1)
    print(_last_updated)
    dates = pd.date_range(start=_last_updated.strftime('%Y-%m-%d'),
                          end=datetime.now().strftime('%Y-%m-%d'), freq='B')
    dates = dates[~dates.isin(holidays  )]
    print(dates)
    # dates = [datetime.strptime("2024-12-01", '%Y-%m-%d')] 

    for data_date in dates:
        x = NSE()
        err = False
        try:
            data = x.equity_cm_bhavcopy_final(data_date)
            print("equity_cm_bhavcopy_final:", data)
            x.add_equity_udiff_bhavcopy_to_db(data)
        except RuntimeError as e:
            print(e)
            err = True

        try:
            data = x.securities_bhavdata(data_date)
            print("securities_bhavdata:", data)
            x.add_securities_bhavdata_to_db(data)
        except RuntimeError as e:
            print(e)
            err = True

        try:
            data = x.equity_block(data_date)
            print("equity_block:", data)
            x.add_equity_block_to_db(data)
        except RuntimeError as e:
            print(e)
            err = True

        try:
            data = x.equity_bulk(data_date)
            print("equity_bulk:", data)
            x.add_equity_bulk_to_db(data)
        except RuntimeError as e:
            print(e)
            err = True

        try:
            data = x.equity_fo_bhavcopy_final(data_date)
            print("equity_fo_bhavcopy_final:", data)
            x.add_fo_udiff_bhavcopy_to_db(data)
        except RuntimeError as e:
            print(e)
            err = True

        try:
            data = x.fo_combine_oi_delta_eq(data_date)
            print("fo_combine_oi_delta_eq:", data)
            x.add_fo_combine_oi_delta_eq_to_db(
                data, data_date=data_date)
        except RuntimeError as e:
            print(e)
            err = True

        try:
            data = x.cm_market_data(data_date)
            print("cm_market_data:", data)
            x.add_cm_market_data_to_db(data)
        except RuntimeError as e:
            print(e)
            err = True
        if not err:
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
        else:
            print('***********************error*********************')

    """
fileURLs:[
"https://nsearchives.nseindia.com/content/equities/bulk.csv",
"https://nsearchives.nseindia.com/content/equities/block.csv",
"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_14062024.csv",
"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20240614_F_0000.csv.zip"
"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_20240621_F_0000.csv.zip"
]
type:Daily
    """

    """
archives: [
{"name":"Full Bhavcopy and Security Deliverable data",
    "type":"daily-reports","category":"capital-market","section":"equities"},
{"name":"CM-UDiFF Common Bhavcopy Final (zip)","type":"daily-reports","category":"capital-market","section":"equities"}]
date: 17-Jun-2024
type: Archives
    """

    """
    https://www.nseindia.com/api/reports?archives=[
{"name":"Full Bhavcopy and Security Deliverable data","type":"daily-reports","category":"capital-market","section":"equities"}]
&date=17-Jun-2024&type=equities&mode=single
    """

    """
https://nsearchives.nseindia.com/content/historical/EQUITIES/2024/JUN/cm14JUN2024bhav.csv.zip

https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20240614_F_0000.csv.zip

https://www.nseindia.com/api/reports?archives=[{"name":"Full Bhavcopy and Security Deliverable data","type":"daily-reports","category":"capital-market","section":"equities"}]&date=17-Jun-2024&type=equities&mode=single

https://nsearchives.nseindia.com/content/equities/bulk.csv

https://nsearchives.nseindia.com/content/equities/block.csv
"""

    """
    # holidays
    https://www.nseindia.com/api/holiday-master?type=clearing

    https://www.nseindia.com/api/holiday-master?type=trading

    """

"""
https://nsearchives.nseindia.com/archives/nsccl/mwpl/combineoi_deleq_25102023.csv
"""
