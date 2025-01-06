from sqlalchemy import Column, Index, Integer, Date, Text, BigInteger, Numeric, String, Text


from db import Base


class FOUDIFFBhavData(Base):
    """
    SQLAlchemy model for the 'udiff_bhavdata' table.
    """
    __tablename__ = "fo_udiff_bhavdata"
    id = Column(Integer, primary_key=True, index=True)
    trade_date = Column(Date, index=True)  # TradDt
    biz_date = Column(Date)  # BizDt
    sgmt = Column(String(50))  # Sgmt
    src = Column(String(50))  # Src
    fin_instrm_tp = Column(String(10), index=True)  # FinInstrmTp
    fin_instrm_id = Column(Integer)  # FinInstrmId
    isin = Column(String(50))  # ISIN
    tckr_symb = Column(String(50), index=True)  # TckrSymb
    scty_srs = Column(String(50))  # SctySrs
    # our defined column calculate this using func get_instrumet_start_day in load start_days
    start_date = Column(Date, index=True)
    xpry_date = Column(Date, index=True)  # XpryDt
    fininstrm_actl_xpry_date = Column(Date)  # FininstrmActlXpryDt
    strk_price = Column(Numeric(20, 2))  # StrkPric
    optn_tp = Column(Text)  # OptnTp
    fin_instrm_nm = Column(String(500))  # FinInstrmNm
    open_price = Column(Numeric(20, 2))  # OpnPric
    high_price = Column(Numeric(20, 2))  # HghPric
    low_price = Column(Numeric(20, 2))  # LwPric
    close_price = Column(Numeric(20, 2))  # ClsPric
    last_price = Column(Numeric(20, 2))  # LastPric
    prev_closing_price = Column(Numeric(20, 2))  # PrvsClsgPric
    undrlyg_price = Column(Numeric(20, 2))  # UndrlygPric
    sttlm_price = Column(Numeric(20, 2))  # SttlmPric
    opn_intrst = Column(Numeric(20, 2), index=True)  # OpnIntrst
    chng_in_opn_intrst = Column(Numeric(20, 2), index=True)  # ChngInOpnIntrst
    total_trade_volume = Column(BigInteger)  # TtlTradgVol
    ttl_trf_val = Column(Numeric(20, 2))  # TtlTrfVal
    ttl_nb_of_txs_exctd = Column(BigInteger)  # TtlNbOfTxsExctd
    ssn_id = Column(String(50))  # SsnId
    new_brd_lot_qty = Column(BigInteger)  # NewBrdLotQty
    rmks = Column(Text)  # Rmks
    rsvd_01 = Column(Text)  # Rsvd01
    rsvd_02 = Column(Text)  # Rsvd02
    rsvd_03 = Column(Text)  # Rsvd03
    rsvd_04 = Column(Text)  # Rsvd04

    __table_args__ = (
        Index('ix_xpry_tckr_type', 'xpry_date', 'tckr_symb', 'fin_instrm_tp'),
    )
