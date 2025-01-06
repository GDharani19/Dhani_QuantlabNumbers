from sqlalchemy import Column, Integer, Date, BigInteger, Numeric, String, Text


from db import Base


class EquityBlock(Base):
    """
    SQLAlchemy model for the 'equity_block' table.
    """
    __tablename__ = "equity_blocks"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date)
    symbol = Column(String(1024))
    security_name = Column(String(1024))
    client_name = Column(String(1024))
    buy_sell = Column(String(50))
    quantity_traded = Column(BigInteger)
    trade_price = Column(Numeric(20, 2))  # "Trade Price / Wght. Avg. Price"
    remarks = Column(Text)
