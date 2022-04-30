from sqlalchemy import Column
from sqlalchemy.dialects.mssql import BINARY, INTEGER
from tj_worker.utils import db


class dimTransactions(db.Base):
    __tablename__ = "dim_transactions"
    __table_args__ = {"schema": "chain"}
    transact_idx = Column(INTEGER, primary_key=True, autoincrement=True)
    id = Column(BINARY(32), nullable=False)
    block_number = Column(INTEGER, primary_key=False, nullable=False)
