from sqlalchemy import Column, Integer, Date, Text, BigInteger, Numeric, String, Text, Index


from db import Base


class HedgeReport(Base):
    """
    SQLAlchemy model for the 'HedgeReport' table.
    """
    __tablename__ = "hedge_report"
    id = Column(Integer, primary_key=True, index=True)
    idx = Column(String(50), index=True)
    date = Column(Date)
    week = Column(String(10), index=True)
    day = Column(String(10), index=True)
    series_month = Column(String(20), index=True)
    future_close = Column(Numeric(20, 2))
    futre_previous_close = Column(Numeric(20, 2))
    spot_close = Column(Numeric(20, 2))
    spot_previous_close = Column(Numeric(20, 2))
    ce_atm = Column(Numeric(20, 2))
    ce_close_price = Column(Numeric(20, 2))
    ce_previous_close_price = Column(Numeric(20, 2))
    pe_atm = Column(Numeric(20, 2))
    pe_close_price = Column(Numeric(20, 2))
    pe_previous_close_price = Column(Numeric(20, 2))
    ce_hedge_cost = Column(Numeric(25, 6))
    pe_hedge_cost = Column(Numeric(25, 6))

    __table_args__ = (
        Index('idx', 'date'),
    )
