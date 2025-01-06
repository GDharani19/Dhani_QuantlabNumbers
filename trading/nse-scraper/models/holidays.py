from sqlalchemy import Column, Integer, Date, BigInteger, Numeric, String, Text

from db import Base


class Holidays(Base):
    """
    SQLAlchemy model for the 'holidays' table.
    """
    __tablename__ = "holidays"
    id = Column(Integer, primary_key=True, index=True)
    tradingDate = Column(Date, unique=True, index=True)
    description = Column(String(500))
    serial_number = Column(Integer)
    week_day = Column(String(15))
