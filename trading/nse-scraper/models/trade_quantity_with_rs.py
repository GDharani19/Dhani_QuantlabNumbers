from sqlalchemy import Boolean, Column, Integer, Date, BigInteger, Numeric, String, Text


from db import Base


class TradeQuantityWthRS(Base):
    """
    SQLAlchemy model for the 'trade quantity with rs' table.
    """
    __tablename__ = "trade_quantity_with_rs"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date)  # DATE
    symbol = Column(String(1024))
    qnty_per_trade = Column(Numeric(20, 5))
    avg_qnty_per_trade = Column(Numeric(20, 5))
    deliv_qty = Column(BigInteger)
    avg_deliv_qty = Column(Numeric(20, 5))
    trade_qty_deviation = Column(Numeric(20, 5))
    delivery_qty_deviation = Column(Numeric(20, 5))
    f_and_o = Column(Boolean)
    nifty_500 = Column(Boolean)
    rs_current = Column(Numeric(20, 6))
    rs_7 = Column(Numeric(20, 6))
    rs_14 = Column(Numeric(20, 6))
    rs7_rs14 = Column(Numeric(30, 16))
    roc_rs_7 = Column(Numeric(30, 16))
    roc_rs_14 = Column(Numeric(30, 16))
    ad_count = Column(Integer)
    dd_count = Column(Integer)
    na_count = Column(Integer)
    vwap_positive_7days = Column(Integer)
    vwap_negative_7days = Column(Integer)
    vwap_positive_14days = Column(Integer)
    vwap_negative_14days = Column(Integer)
