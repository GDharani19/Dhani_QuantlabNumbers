from models.equity_bulk import EquityBulk
from models.equity_block import EquityBlock
from models.securities_bhavdata import SecurityiesBhavData
from models.cm_udiff_bhavdata import CMUDIFFBhavData
from models.fo_udiff_bhavdata import FOUDIFFBhavData
from models.holidays import Holidays
from models.start_date_lookup import StartDateLookup
from models.ticker_info import TickerInfo
from models.fo_stock_intstruments_report_1 import FOStockInstrumentsReport1
from models.last_updated_date import LastUpdatedDate
from models.cm_market_data_indexes import CMMarketDataIndexes
from models.fo_combine_oi_delta_equivalent import FOCombineOIDeltaEquivalent
from models.hedge_report import HedgeReport
from models.synthetic_check import SyntheticCheck
from models.cm_index_data import CMIndexData
from models.all_data_csv_report import AllDataCsvReport

from db import Base, engine

DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/testdb"

Base.metadata.create_all(bind=engine)
