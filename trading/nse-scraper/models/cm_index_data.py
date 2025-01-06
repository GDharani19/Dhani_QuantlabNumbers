from sqlalchemy import Column, Index, Integer, Date, BigInteger, Numeric, String, Text


from db import Base


class CMIndexData(Base):
    """
    SQLAlchemy model for the 'CMIndexData' table.
    """
    __tablename__ = "cm_index_data"
    id = Column(Integer, primary_key=True, index=True)
    index_name = Column(String(1024))
    index_date = Column(Date)

    open_index_value = Column(Numeric(20, 2))
    high_index_value = Column(Numeric(20, 2))
    low_index_value = Column(Numeric(20, 2))
    closing_index_value = Column(Numeric(20, 2))
    points_change = Column(Numeric(20, 2))
    percentage_change = Column(Numeric(10, 3))
    volume = Column(BigInteger)
    turnover_in_crores = Column(Numeric(20, 2))
    p_e = Column(Numeric(20, 2))
    p_b = Column(Numeric(20, 2))
    div_yield = Column(Numeric(20, 2))

    __table_args__ = (
        Index('ix_cm_index_data_index_name_index_date',
              'index_name', 'index_date'),
    )
