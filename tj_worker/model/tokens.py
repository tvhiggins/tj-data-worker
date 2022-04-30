from sqlalchemy import Column
from sqlalchemy.dialects.mssql import BINARY, INTEGER, NVARCHAR
from tj_worker.utils import db


class dimTokens(db.Base):
    __tablename__ = "dim_tokens"
    __table_args__ = {"schema": "chain"}
    token_idx = Column(INTEGER, primary_key=True, autoincrement=True)
    id = Column(BINARY(20), nullable=False)
    symbol = Column(NVARCHAR(10), nullable=False)
    name = Column(NVARCHAR(None), nullable=False)
