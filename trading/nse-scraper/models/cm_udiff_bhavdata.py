from sqlalchemy import Column, Integer, Date, Text, BigInteger, Numeric, String, Text
# import urlparse

from db import Base
# import urllib.parse 
# print(urllib.parse)

class CMUDIFFBhavData(Base):
    """
    SQLAlchemy model for the 'udiff_bhavdata' table.
    """
    __tablename__ = "cm_udiff_bhavdata"
    id = Column(Integer, primary_key=True, index=True)
    trade_date = Column(Date)  # TradDt
    biz_date = Column(Date)  # BizDt
    sgmt = Column(String(50))  # Sgmt
    src = Column(String(50))  # Src
    fin_instrm_tp = Column(Text)  # FinInstrmTp
    fin_instrm_id = Column(Integer) # FinInstrmId
    isin = Column(String(50)) # ISIN
    tckr_symb = Column(String(50))  # TckrSymb
    scty_srs = Column(String(50))  # SctySrs
    xpry_date = Column(Date)  # XpryDt
    fininstrm_actl_xpry_date = Column(Date)  # FininstrmActlXpryDt
    strk_price = Column(Numeric(20, 2))  # StrkPric
    optn_tp = Column(Text)  # OptnTp
    fin_instrm_nm = Column(Text)  # FinInstrmNm
    open_price = Column(Numeric(20, 2))  # OpnPric
    high_price = Column(Numeric(20, 2))  # HghPric
    low_price = Column(Numeric(20, 2))  # LwPric
    close_price = Column(Numeric(20, 2))  # ClsPric
    last_price = Column(Numeric(20, 2))  # LastPric
    prev_closing_price = Column(Numeric(20, 2))  # PrvsClsgPric
    undrlyg_price = Column(Numeric(20, 2))  # UndrlygPric
    sttlm_price = Column(Numeric(20, 2))  # SttlmPric
    opn_intrst = Column(Text)  # OpnIntrst
    chng_in_opn_intrst = Column(Text)  # ChngInOpnIntrst
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