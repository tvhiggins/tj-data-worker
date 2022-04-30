from sqlalchemy import Column
from sqlalchemy.dialects.mssql import BINARY, INTEGER, NVARCHAR
from tj_worker.utils import db


class dimPairs(db.Base):
    __tablename__ = "dim_pairs"
    __table_args__ = {"schema": "tj"}
    pair_idx = Column(INTEGER, primary_key=True, autoincrement=True)
    id = Column(BINARY(20), nullable=False)
    name = Column(NVARCHAR(200), nullable=False)
    token0_idx = Column(INTEGER, nullable=False)
    token1_idx = Column(INTEGER, nullable=False)
