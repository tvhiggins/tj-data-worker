import codecs

import pandas as pd
from sqlalchemy import exc, func
from tj_worker.model import blocks, pairs, swaps, tokens, transactions
from tj_worker.utils import db

from ..utils import log

logger = log.setup_custom_logger(name=__file__)


def get_last_inserted_block_number():
    db_session = db.get_db_session()
    max_block_number = db_session.query(func.max(swaps.factSwaps.block_number)).first()[0]
    db_session.remove()

    if max_block_number is None:
        max_block_number = 0

    return max_block_number


def get_pair_ids_to_dict():

    pairs_dict = dict()

    db_session = db.get_db_session()

    data = db_session.query(pairs.dimPairs.id, pairs.dimPairs.pair_idx).all()

    for row in data:

        pairs_dict["0x" + codecs.encode(row.id, "hex_codec").decode("ascii")] = row.pair_idx

    db_session.remove()
    return pairs_dict


def get_token_ids_to_dict():

    tokens_dict = dict()

    db_session = db.get_db_session()

    data = db_session.query(tokens.dimTokens.id, tokens.dimTokens.token_idx).all()

    for row in data:
        tokens_dict["0x" + codecs.encode(row.id, "hex_codec").decode("ascii")] = row.token_idx

    db_session.remove()
    return tokens_dict


def insert_dim_tokens(token_object: tokens.dimTokens) -> int:
    db_session = db.get_db_session()

    exists = db_session.query(tokens.dimTokens).where(tokens.dimTokens.id == token_object.id).first()

    if exists is None:
        db_session.add(token_object)

    db_session.commit()
    token_idx = db_session.query(tokens.dimTokens.token_idx).where(tokens.dimTokens.id == token_object.id).first()[0]

    db_session.remove()

    return token_idx


def insert_dim_pairs(pair_object: pairs.dimPairs) -> int:
    db_session = db.get_db_session()

    exists = db_session.query(pairs.dimPairs).where(pairs.dimPairs.id == pair_object.id).first()

    if exists is None:
        db_session.add(pair_object)

    db_session.commit()
    pair_idx = db_session.query(pairs.dimPairs.pair_idx).where(pairs.dimPairs.id == pair_object.id).first()[0]

    db_session.remove()

    return pair_idx


def insert_dim_blocks(blocks_df: pd.DataFrame, check_integrity: bool = False, check_integrity_count: int = 0):

    if check_integrity:
        block_numbers = blocks_df["block_number"].tolist()

        blocks_numbers_to_delete = list()
        db_session = db.get_db_session()

        for i in range(0, len(block_numbers), 1000):
            to_check = block_numbers[i : i + 1000]
            data = db_session.query(blocks.dimBlocks).where(blocks.dimBlocks.block_number.in_(to_check)).all()
            blocks_numbers_to_delete.extend([row.block_number for row in data])

        db_session.remove()

        blocks_df = blocks_df[~blocks_df["block_number"].isin(blocks_numbers_to_delete)]

    non_scoped_db_session = db.get_non_scoped_db_session()

    conn = non_scoped_db_session.connect()

    try:
        blocks_df.to_sql(
            blocks.dimBlocks.__tablename__,
            schema=blocks.dimBlocks.__table_args__["schema"],
            con=conn,
            method="multi",
            index=False,
            if_exists="append",
            chunksize=100,
        )
    except exc.IntegrityError as e:
        if check_integrity < 2:
            logger.info("Integrity Error, retrying....")
            check_integrity_count += 1
            insert_dim_blocks(blocks_df=blocks_df, check_integrity=True, check_integrity_count=check_integrity_count)
        else:
            logger.error(e)
            logger.error(blocks_df)
            exit(1)

    conn.close()
    non_scoped_db_session.dispose()


def get_max_transact_idx() -> int:
    db_session = db.get_db_session()
    row = db_session.query(func.max(transactions.dimTransactions.transact_idx)).first()

    db_session.remove()

    if row[0] is None:
        return 0
    else:
        return row[0]


def get_max_swap_idx() -> int:
    db_session = db.get_db_session()
    row = db_session.query(func.max(swaps.factSwaps.swap_idx)).first()

    db_session.remove()

    if row[0] is None:
        return 0
    else:
        return row[0]


def insert_fact_swaps(swaps_df: pd.DataFrame):

    non_scoped_db_session = db.get_non_scoped_db_session()
    conn = non_scoped_db_session.connect()

    swaps_df.to_sql(
        swaps.factSwaps.__tablename__,
        schema=swaps.factSwaps.__table_args__["schema"],
        con=conn,
        method="multi",
        index=False,
        if_exists="append",
        chunksize=100,
    )

    conn.close()
    non_scoped_db_session.dispose()
