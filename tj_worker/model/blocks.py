from sqlalchemy import Column
from sqlalchemy.dialects.mssql import DATETIME2, INTEGER
from tj_worker.utils import db


class dimBlocks(db.Base):
    __tablename__ = "dim_blocks"
    __table_args__ = {"schema": "chain"}
    block_number = Column(INTEGER, primary_key=True, autoincrement=False)
    timestamp_unix = Column(INTEGER, nullable=False)
    timestamp = Column(DATETIME2, nullable=False)
