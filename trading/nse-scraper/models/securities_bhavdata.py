from sqlalchemy import Column, Integer, Date, BigInteger, Numeric, String, Text


from db import Base


class SecurityiesBhavData(Base):
    """
    SQLAlchemy model for the 'securities_bhavdata' table.
    """
    __tablename__ = "securities_bhavdata"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date)  # DATE1
    symbol = Column(String(1024))
    series = Column(String(50))
    prev_close = Column(Numeric(20, 2))
    open_price = Column(Numeric(20, 2))
    high_price = Column(Numeric(20, 2))
    low_price = Column(Numeric(20, 2))
    last_price = Column(Numeric(20, 2))
    close_price = Column(Numeric(20, 2))
    avg_price = Column(Numeric(20, 2))
    total_trade_quantity = Column(BigInteger)  # TTL_TRD_QNTY
    turnover_lacs = Column(Numeric(20, 2))
    number_of_trades = Column(BigInteger)  # NO_OF_TRADES
    delivery_quantity = Column(BigInteger)  # DELIV_QTY
    delivery_percentage = Column(Numeric(5, 2)) # DELIV_PER


