from sqlalchemy import Column, Integer, Date, BigInteger, Numeric, String, Text


from db import Base


class CMMarketDataIndexes(Base):
    """
    SQLAlchemy model for the 'cm_market_data_indexes' table all prices are in lakhs.
    """
    __tablename__ = "cm_market_data_indexes"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date)
    index = Column(String(1024))
    prev_closing_price = Column(Numeric(20, 2))  # PrvsClsgPric
    open_price = Column(Numeric(20, 2))  # OpnPric
    high_price = Column(Numeric(20, 2))  # HghPric
    low_price = Column(Numeric(20, 2))  # LwPric
    close_price = Column(Numeric(20, 2))  # ClsPric
    gain_or_loss = Column(Numeric(20, 2))  # gain/loss
    percentage_change = Column(Numeric(10, 6))
    relative_strength = Column(Numeric(10, 6))  # w.r.t Nifty50
    relative_performance_ratio = Column(Numeric(15, 6))  # w.r.t Nifty50
