from models.cm_index_data import CMIndexData
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
from models.all_data_csv_report import AllDataCsvReport


TABLES = [
    'equity_bulks',
    'equity_blocks',
    'cm_udiff_bhavdata',
    'last_updated_date',
    'fo_udiff_bhavdata',
    'securities_bhavdata',
    'fo_stock_intstruments_report_1',
    'start_date_lookup',
    'cm_market_data_indexes',
    'fo_combine_oi_delta_equivalent',
    'cm_index_data'
]

# from pathlib import Path
# directory_path = Path('./Data')

# if not directory_path.exists():
#     # Create the directory if it doesn't exist
#     directory_path.mkdir(parents=True)


tables_to_get = {

    # '<Table Name>': [
    #     '<Table Name>',
    #     "<Source Url to download data>",
    #     '<file name to save data>',
    #     <Boolean: is the file downloaded is a zip file>,
    #     "<data date format>"
    # ],

    'cm_udiff_bhavdata': [
        'cm_udiff_bhavdata',
        "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip",
        'BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip',
        True,
        "%Y%m%d"
    ],
    'securities_bhavdata': [
        'securities_bhavdata',
        "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
        'sec_bhavdata_full_{date_str}.csv',
        False,
        "%d%m%Y"
    ],
    'equity_blocks': [
        'equity_blocks',
        "https://www.nseindia.com/api/historical/block-deals?from={date_str}&to={date_str}&csv=true",
        "block_{date_str}.csv",
        False,
        "%d-%m-%Y"
    ],
    'equity_bulks': [
        'equity_bulks',
        "https://www.nseindia.com/api/historical/bulk-deals?from={date_str}&to={date_str}&csv=true",
        "bulk_{date_str}.csv",
        False,
        "%d-%m-%Y"
    ],
    'fo_udiff_bhavdata': [
        'fo_udiff_bhavdata',
        "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip",
        'BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip',
        True,
        "%Y%m%d"
    ],
    'fo_combine_oi_delta_equivalent': [
        'fo_combine_oi_delta_equivalent',
        "https://nsearchives.nseindia.com/archives/nsccl/mwpl/combineoi_deleq_{date_str}.csv",
        'combineoi_deleq_{date_str}.csv',
        False,
        "%d%m%Y"
    ],
    'cm_market_data_indexes': [
        'cm_market_data_indexes',
        """https://www.nseindia.com/api/reports?archives=[{"name":"CM - Market Activity Report","type":"archives","category":"capital-market","section":"equities"}]&date={date_str}&type=equities&mode=single""",
        "ma{date_str}.csv",
        False,
        "%d-%b-%Y"
    ],
    'cm_index_data': [
        'cm_index_data',
        "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22Daily%20Snapshot%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22indices%22%7D%5D&date={date_str}&type=indices&mode=single",
        "ind_close_all_{date_str}.csv",
        False,
        "%d-%b-%Y"
    ]
}


mst_table_mapping = {
    'equity_bulks': {'model': EquityBulk, 'filters': [], 'date_column': 'date'},
    'equity_blocks': {'model': EquityBlock, 'filters': [], 'date_column': 'date'},
    'cm_udiff_bhavdata': {'model': CMUDIFFBhavData, 'filters': ['trade_date', 'fin_instrm_nm', 'tckr_symb'], 'date_column': 'trade_date'},
    'fo_udiff_bhavdata': {'model': FOUDIFFBhavData, 'filters': ['trade_date', 'tckr_symb', 'fin_instrm_nm'], 'date_column': 'trade_date'},
    'securities_bhavdata':  {'model': SecurityiesBhavData, 'filters': ['date', 'symbol', 'series'], 'date_column': 'date'},
    'start_date_lookup':  {'model': StartDateLookup, 'filters': [], 'date_column': 'end_date'},
    'cm_market_data_indexes': {'model': CMMarketDataIndexes, 'filters': ['date', 'index'], 'date_column': 'date'},
    'fo_combine_oi_delta_equivalent': {'model': FOCombineOIDeltaEquivalent, 'filters': ['date', 'isin', 'script_name', 'symbol'], 'date_column': 'date'},
    'fo_stock_intstruments_report_1': {'model': FOStockInstrumentsReport1, 'filters': [], 'date_column': 'current_date'},
    'last_updated_date': {'model': LastUpdatedDate, 'filters': [], 'date_column': 'last_updated_date'},
    'cm_index_data': {'model': CMIndexData, 'filters': ['index_name', 'index_date'], 'date_column': 'index_date'},
    'synthetic_check': {'model': SyntheticCheck, 'filters': ['idx', 'trade_date'], 'date_column': 'trade_date'},
    'hedge_report': {'model': HedgeReport, 'filters': ['idx', 'date'], 'date_column': 'date'},
    'all_data_csv_report': {'model': AllDataCsvReport, 'filters': ['symbol', 'date'], 'date_column': 'date'},
}

INDEX_FOR_ADOPTIVE_EMA_CALCULATION = [
    "Nifty 100",
    "Nifty 200",
    "Nifty 50",
    "Nifty 500",
    "Nifty Auto",
    "Nifty Bank",
    "Nifty Commodities",
    "NIFTY CONSR DURBL",
    "Nifty Consumption",
    "Nifty CPSE",
    "Nifty Energy",
    "Nifty Fin Service",
    "Nifty FMCG",
    "NIFTY HEALTHCARE",
    "Nifty Infra",
    "Nifty IT",
    "Nifty Media",
    "Nifty Metal",
    "NIFTY OIL AND GAS",
    "Nifty Pharma",
    "Nifty PSE",
    "Nifty PSU Bank",
    "Nifty Pvt Bank",
    "Nifty Realty",
    "Nifty Serv Sector"
]



# import requests
# import os
# import zipfile
# import csv
# import json
# from datetime import datetime, timedelta
# from io import BytesIO
# from models.cm_index_data import CMIndexData
# from models.equity_bulk import EquityBulk
# from models.equity_block import EquityBlock
# from models.securities_bhavdata import SecurityiesBhavData
# from models.cm_udiff_bhavdata import CMUDIFFBhavData
# from models.fo_udiff_bhavdata import FOUDIFFBhavData
# from models.holidays import Holidays
# from models.start_date_lookup import StartDateLookup
# from models.ticker_info import TickerInfo
# from models.fo_stock_intstruments_report_1 import FOStockInstrumentsReport1
# from models.last_updated_date import LastUpdatedDate
# from models.cm_market_data_indexes import CMMarketDataIndexes
# from models.fo_combine_oi_delta_equivalent import FOCombineOIDeltaEquivalent
# from models.hedge_report import HedgeReport
# from models.synthetic_check import SyntheticCheck
# from models.all_data_csv_report import AllDataCsvReport

# from pathlib import Path
# directory_path = Path('data')

# if not directory_path.exists():
#     # Create the directory if it doesn't exist
#     directory_path.mkdir(parents=True)

# # Constants for tables and configurations
# TABLES = [
#     'equity_bulks',
#     'equity_blocks',
#     'cm_udiff_bhavdata',
#     'last_updated_date',
#     'fo_udiff_bhavdata',
#     'securities_bhavdata',
#     'fo_stock_intstruments_report_1',
#     'start_date_lookup',
#     'cm_market_data_indexes',
#     'fo_combine_oi_delta_equivalent',
#     'cm_index_data'
# ]

# tables_to_get = {
#     'cm_udiff_bhavdata': [
#         'cm_udiff_bhavdata',
#         "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip",
#         'BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip',
#         True,
#         "%Y%m%d"
#     ],
#     'securities_bhavdata': [
#         'securities_bhavdata',
#         "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
#         'sec_bhavdata_full_{date_str}.csv',
#         False,
#         "%d%m%Y"
#     ],
#     'equity_blocks': [
#         'equity_blocks',
#         "https://www.nseindia.com/api/historical/block-deals?from={date_str}&to={date_str}&csv=true",
#         "block_{date_str}.csv",
#         False,
#         "%d-%m-%Y"
#     ],
#     'equity_bulks': [
#         'equity_bulks',
#         "https://www.nseindia.com/api/historical/bulk-deals?from={date_str}&to={date_str}&csv=true",
#         "bulk_{date_str}.csv",
#         False,
#         "%d-%m-%Y"
#     ],
#     'fo_udiff_bhavdata': [
#         'fo_udiff_bhavdata',
#         "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip",
#         'BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip',
#         True,
#         "%Y%m%d"
#     ],
#     'fo_combine_oi_delta_equivalent': [
#         'fo_combine_oi_delta_equivalent',
#         "https://nsearchives.nseindia.com/archives/nsccl/mwpl/combineoi_deleq_{date_str}.csv",
#         'combineoi_deleq_{date_str}.csv',
#         False,
#         "%d%m%Y"
#     ],
#     'cm_market_data_indexes': [
#         'cm_market_data_indexes',
#         """https://www.nseindia.com/api/reports?archives=[{"name":"CM - Market Activity Report","type":"archives","category":"capital-market","section":"equities"}]&date={date_str}&type=equities&mode=single""",
#         "ma{date_str}.csv",
#         False,
#         "%d-%b-%Y"
#     ],
#     'cm_index_data': [
#         'cm_index_data',
#         "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22Daily%20Snapshot%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22indices%22%7D%5D&date={date_str}&type=indices&mode=single",
#         "ind_close_all_{date_str}.csv",
#         False,
#         "%d-%b-%Y"
#     ]
# }

# mst_table_mapping = {
#     'equity_bulks': {'model': EquityBulk, 'filters': [], 'date_column': 'date'},
#     'equity_blocks': {'model': EquityBlock, 'filters': [], 'date_column': 'date'},
#     'cm_udiff_bhavdata': {'model': CMUDIFFBhavData, 'filters': ['trade_date', 'fin_instrm_nm', 'tckr_symb'], 'date_column': 'trade_date'},
#     'fo_udiff_bhavdata': {'model': FOUDIFFBhavData, 'filters': ['trade_date', 'tckr_symb', 'fin_instrm_nm'], 'date_column': 'trade_date'},
#     'securities_bhavdata':  {'model': SecurityiesBhavData, 'filters': ['date', 'symbol', 'series'], 'date_column': 'date'},
#     'start_date_lookup':  {'model': StartDateLookup, 'filters': [], 'date_column': 'end_date'},
#     'cm_market_data_indexes': {'model': CMMarketDataIndexes, 'filters': ['date', 'index'], 'date_column': 'date'},
#     'fo_combine_oi_delta_equivalent': {'model': FOCombineOIDeltaEquivalent, 'filters': ['date', 'isin', 'script_name', 'symbol'], 'date_column': 'date'},
#     'fo_stock_intstruments_report_1': {'model': FOStockInstrumentsReport1, 'filters': [], 'date_column': 'current_date'},
#     'last_updated_date': {'model': LastUpdatedDate, 'filters': [], 'date_column': 'last_updated_date'},
#     'cm_index_data': {'model': CMIndexData, 'filters': ['index_name', 'index_date'], 'date_column': 'index_date'},
#     'synthetic_check': {'model': SyntheticCheck, 'filters': ['idx', 'trade_date'], 'date_column': 'trade_date'},
#     'hedge_report': {'model': HedgeReport, 'filters': ['idx', 'date'], 'date_column': 'date'},
#     'all_data_csv_report': {'model': AllDataCsvReport, 'filters': ['symbol', 'date'], 'date_column': 'date'},
# }

# INDEX_FOR_ADOPTIVE_EMA_CALCULATION = [
#     "Nifty 100", "Nifty 200", "Nifty 50", "Nifty 500",
#     "Nifty Auto", "Nifty Bank", "Nifty Commodities", 
#     "NIFTY CONSR DURBL", "Nifty Consumption", "Nifty CPSE", 
#     "Nifty Energy", "Nifty Fin Service", "Nifty FMCG", 
#     "NIFTY HEALTHCARE", "Nifty Infra", "Nifty IT", 
#     "Nifty Media", "Nifty Metal", "NIFTY OIL AND GAS", 
#     "Nifty Pharma", "Nifty PSE", "Nifty PSU Bank", 
#     "Nifty Pvt Bank", "Nifty Realty", "Nifty Serv Sector"
# ]


# # print(url)