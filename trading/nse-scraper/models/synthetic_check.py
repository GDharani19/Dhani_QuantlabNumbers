from sqlalchemy import Boolean, Column, Integer, Date, Text, BigInteger, Numeric, String, Text, Index


from db import Base


class SyntheticCheck(Base):
    """
    SQLAlchemy model for the 'SyntheticCheck' table.
    """
    __tablename__ = "synthetic_check"
    id = Column(Integer, primary_key=True, index=True)
    idx = Column(String(50), index=True)
    trade_date = Column(Date)
    options_expiry_date = Column(Date)
    expiry_month = Column(String(10), index=True)
    spot_close = Column(Numeric(20, 2))
    future_close = Column(Numeric(20, 2))

    spot_ce_atm_strik = Column(Numeric(20, 2))
    spot_pe_atm_strik = Column(Numeric(20, 2))
    spot_ce_close_price = Column(Numeric(20, 2))
    spot_pe_close_price = Column(Numeric(20, 2))

    future_ce_atm_strik = Column(Numeric(20, 2))
    future_pe_atm_strik = Column(Numeric(20, 2))
    future_ce_close_price = Column(Numeric(20, 2))
    future_pe_close_price = Column(Numeric(20, 2))

    spot_synthetic = Column(Numeric(25, 6))
    future_synthetic = Column(Numeric(25, 6))
    spot_synthetic_check = Column(Boolean)
    future_synthetic_check = Column(Boolean)

    __table_args__ = (
        Index('idx', 'trade_date'),
    )
