from sqlalchemy import Column, Integer, Date, Text, BigInteger, Numeric, String, Text, Index


from db import Base


class FOStockInstrumentsReport1(Base):
    """
    SQLAlchemy model for the 'FOStockInstrumentsReport1' table.
    """
    __tablename__ = "fo_stock_intstruments_report_1"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(500), index=True)
    type = Column(String(10), index=True)
    ticker_symbol = Column(String(50), index=True)
    start_date = Column(Date)
    expiry_date = Column(Date, index=True)
    current_date = Column(Date)
    first_trade_date = Column(Date)
    days_elapsed_since_birth = Column(Integer)
    days_active = Column(Integer)
    percentage_active = Column(Numeric(6, 3), index=True)
    first_trade_underlying_price = Column(Numeric(20, 2))
    first_trade_close_price = Column(Numeric(20, 2))
    latest_close_price = Column(Numeric(20, 2))
    latest_opn_intrst_lot = Column(BigInteger)
    latest_chng_opn_intrst_lot = Column(BigInteger)

    __table_args__ = (
        Index('ix_xpry_tckr_type', 'expiry_date', 'ticker_symbol', 'type'),
    )

    # days_elapsed_since_birth, days_active, percentage_active >>> calculate after data insertion
