from sqlalchemy import Column, Integer, Date, Text, BigInteger, Numeric, String, Text, Index


from db import Base


class FOCombineOIDeltaEquivalent(Base):
    """
    SQLAlchemy model for the 'FOCombineOIDeltaEquivalent' table.
    """
    __tablename__ = "fo_combine_oi_delta_equivalent"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date,  index=True)
    isin = Column(String(50))  # ISIN
    script_name = Column(String(1024))
    symbol = Column(String(50), index=True)
    open_interest = Column(Numeric(20, 2))
    delta_equivalent_open_interest_contract_wise = Column(Numeric(25, 6))
    delta_equivalent_open_interest_portfolio_wise = Column(Numeric(25, 6))
