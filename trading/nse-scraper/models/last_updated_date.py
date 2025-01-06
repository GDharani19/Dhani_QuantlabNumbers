from sqlalchemy import Column, Integer, Date, BigInteger, Numeric, String, Text

from db import Base


class LastUpdatedDate(Base):
    """
    SQLAlchemy model for the 'last_updated_date' table.
    """
    __tablename__ = "last_updated_date"
    last_updated_date = Column(Date, primary_key=True)
