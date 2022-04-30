from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from tj_worker.utils import settings

Base = declarative_base()


def get_db_session() -> scoped_session:
    connection_url = URL.create(
        "mssql+pyodbc",
        username=settings.DB_USERNAME,
        password=settings.DB_PASSWORD,
        host=settings.DB_SERVER,
        port=1433,
        database=settings.DB_NAME,
        query={
            "driver": "ODBC Driver 17 for SQL Server",
        },
    )

    engine = create_engine(connection_url)

    return scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def get_non_scoped_db_session() -> create_engine:
    connection_url = URL.create(
        "mssql+pyodbc",
        username=settings.DB_USERNAME,
        password=settings.DB_PASSWORD,
        host=settings.DB_SERVER,
        port=1433,
        database=settings.DB_NAME,
        query={
            "driver": "ODBC Driver 17 for SQL Server",
        },
    )

    return create_engine(connection_url)
