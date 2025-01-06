from sqlalchemy import Column, Index, Integer, Date, BigInteger, Numeric, String, Text
# import urlparse
from db import Base


class AllDataCsvReport(Base):
    """
    SQLAlchemy model for the 'AllDataCsvReport' table.
    """
    __tablename__ = "all_data_csv_report"
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(50), index=True)
    date = Column(Date,  index=True)
    qnty_per_trade = Column(BigInteger)
    avg_qnty_per_trade = Column(BigInteger)
    deliv_qty = Column(BigInteger)
    avg_deliv_qty = Column(BigInteger)
    trade_qty_deviation = Column(BigInteger)
    delivery_qty_deviation = Column(BigInteger)
    f_and_o = Column(String(5))
    nifty_500 = Column(String(5))
    rs_current = Column(Numeric(10, 4))
    rs_7 = Column(Numeric(10, 4))
    rs_14 = Column(Numeric(10, 4))
    rs_7_minus_rs_14 = Column(Numeric(10, 4))
    roc_rs_7 = Column(Numeric(25, 14))
    roc_rs_14 = Column(Numeric(25, 14))
    ad_count = Column(Integer)
    dd_count = Column(Integer)
    na_count = Column(Integer)
    vwap_positive_7days = Column(Integer)
    vwap_negative_7days = Column(Integer)
    vwap_positive_14days = Column(Integer)
    vwap_negative_14days = Column(Integer)

    __table_args__ = (
        Index('ix_all_data_csv_report_symbol_date',
              'symbol', 'date'),
    )
