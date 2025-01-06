from sqlalchemy import Column, Integer, Date, BigInteger, Numeric, String, Text

from db import Base


class TickerInfo(Base):
    """
    SQLAlchemy model for the 'ticker_info' table.
    """
    __tablename__ = "ticker_info"
    id = Column(Integer, primary_key=True, index=True)
    tckr_symb = Column(String(50))
    company_name = Column(String(1024))
    isin = Column(String(100))
    industry = Column(String(1024))
    macro = Column(String(1024))
    sector = Column(String(1024))
    basic_industry = Column(String(1024))
    info = Column(Text)
