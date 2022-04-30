from sqlalchemy import Column
from sqlalchemy.dialects.mssql import DECIMAL, INTEGER
from tj_worker.utils import db


class factSwaps(db.Base):
    __tablename__ = "fact_swap"
    __table_args__ = {"schema": "tj"}
    swap_idx = Column(INTEGER, primary_key=True, autoincrement=True)
    transact_idx = Column(INTEGER, primary_key=False, autoincrement=False)
    swap_number = Column(INTEGER, primary_key=False, autoincrement=False)
    pair_idx = Column(INTEGER, primary_key=False, autoincrement=False)
    amount0_in = Column(DECIMAL(36, 18), nullable=False)
    amount0_out = Column(DECIMAL(36, 18), nullable=False)
    amount1_in = Column(DECIMAL(36, 18), nullable=False)
    amount1_out = Column(DECIMAL(36, 18), nullable=False)
    amount_usd = Column(DECIMAL(36, 18), nullable=False)
