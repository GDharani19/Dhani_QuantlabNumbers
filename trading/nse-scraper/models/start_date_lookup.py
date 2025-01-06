from sqlalchemy import Column, Integer, Date, BigInteger, Numeric, String, Text

from db import Base


class StartDateLookup(Base):
    """
    SQLAlchemy model for the 'start_date_lookup' table.
    """
    __tablename__ = "start_date_lookup"
    id = Column(Integer, primary_key=True, index=True)
    end_date = Column(Date, unique=True, index=True)
    start_date = Column(Date, unique=True, index=True)
