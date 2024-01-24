from tj_worker import swap_etl, swap_getter
from tj_worker.utils import db, settings


def truncate_tables():
    if "dev" not in settings.DB_NAME:
        return
    db_session = db.get_db_session()
    db_session.execute("TRUNCATE TABLE tj.fact_swap")
    db_session.commit()
    db_session.execute("TRUNCATE TABLE tj.dim_pairs")
    db_session.commit()
    db_session.execute("DELETE FROM chain.dim_tokens")
    db_session.commit()
    db_session.execute("TRUNCATE TABLE chain.dim_blocks")
    db_session.commit()


def test_run_etl_loop(test_container_name):
    GetSwaps = swap_getter.SwapGetter(testing=True, azure_storage_container=test_container_name)
    GetSwaps.run_loop()
    GetSwaps.clear_existing_files()

    truncate_tables()
    InsertSwaps = swap_etl.SwapETL(testing=True, azure_storage_container=test_container_name)
    InsertSwaps.run_loop()

    InsertSwaps.clear_existing_files()
