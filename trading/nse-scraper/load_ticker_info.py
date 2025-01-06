import csv
import json
import pickle
import requests
from requests import ReadTimeout, Session
import random
from user_agents_list import user_agents
import time
from models.ticker_info import TickerInfo
from db import engine, get_db
from pathlib import Path
import os
from urllib.parse import quote_plus


BASE_URL = "https://www.nseindia.com/api"
ARCHIVE_URL = "https://archives.nseindia.com"
NEW_ARCHIVE_URL = "https://nsearchives.nseindia.com"
root_dir = os.getcwd()
temp_folder = root_dir+'/temp'
if not os.path.exists(temp_folder):
    os.mkdir(temp_folder)

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


class Requester:
    def __init__(self):
        self.dir = Path(temp_folder)

        self.cookie_path = self.dir / "nse_cookies.pkl"
        self.session = Session()

        self.session.headers.update(random.choice(headers))
        self.session.cookies.update(self.__get_cookies())

    @staticmethod
    def __get_db_session():
        return next(get_db())

    def __req(self, url, params=None, timeout=15):
        try:
            r = self.session.get(url, params=params, timeout=timeout)
        except ReadTimeout as e:
            raise TimeoutError(repr(e))

        if not r.ok:
            raise ConnectionError(f"{url} {r.status_code}: {r.reason}")

        return r

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

    @staticmethod
    def __has_cookies_expired(cookies):
        for cookie in cookies:
            if cookie.is_expired():
                return True

        return False

    def add_data(self, url: str):
        retries = 0
        while retries < 5:
            try:
                with self.session.get(url, stream=True, timeout=20) as r:

                    content_type = r.headers.get("content-type")
                    if not r.ok:
                        raise ConnectionError(
                            f"{url} {r.status_code}: {r.reason}")

                    if content_type and "text/html" in content_type:
                        with open('temp/failed_files.txt', 'a') as f:
                            f.write(url.split("/")[-1])
                            f.write("\n")
                        raise RuntimeError(
                            url
                        )

                    data = r.json()
                    if 'error' in data:
                        print(data)
                        raise RuntimeError(
                            url
                        )
                    with self.__get_db_session() as db_session:
                        if not db_session.query(TickerInfo).filter(
                                TickerInfo.tckr_symb == data['info']['symbol']).first():
                            try:
                                db_session.add(
                                    TickerInfo(
                                        tckr_symb=data['info'].get('symbol'),
                                        company_name=data['info'].get(
                                            'companyName'),
                                        isin=data['info'].get('isin'),
                                        industry=data['industryInfo'].get(
                                            'industry'),
                                        macro=data['industryInfo'].get(
                                            'macro'),
                                        sector=data['industryInfo'].get(
                                            'sector'),
                                        basic_industry=data['industryInfo'].get(
                                            'basicIndustry'),
                                        info=json.dumps(r.json())
                                    )
                                )
                                db_session.commit()
                                db_session.close()
                            except Exception as e:
                                print(e)
                                print(data)
                    break
            except (ReadTimeout, ConnectionError, RuntimeError) as e:
                print(e)
                retries += 1
                time.sleep(retries)


if __name__ == "__main__":
    requester = Requester()
    count = 0
    with open('ticker_list.csv', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=' ', quotechar='|')
        next(rows)
        for row in rows:
            with requester._Requester__get_db_session() as db_session:
                if db_session.query(TickerInfo).filter(
                        TickerInfo.tckr_symb == row[0]).first():
                    continue
            requester.add_data(
                f"https://www.nseindia.com/api/quote-equity?symbol={quote_plus(row[0])}")

            count += 1
            if count % 10 == 0:
                time.sleep(random.choice([0, 1]))
