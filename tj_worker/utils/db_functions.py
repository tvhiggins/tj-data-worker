import codecs

import pandas as pd
from sqlalchemy import exc, func
from tj_worker.model import blocks, pairs, swaps, tokens, transactions
from tj_worker.utils import db

from ..utils import log

logger = log.setup_custom_logger(name=__file__)


def get_last_block_number():
    db_session = db.get_db_session()
    max_transact_idx = db_session.query(func.max(swaps.factSwaps.transact_idx)).first()[0]

    if max_transact_idx is None:
        db_session.remove()
        return 8973570

    max_block_number = (
        db_session.query(func.max(transactions.dimTransactions.block_number))
        .where(transactions.dimTransactions.transact_idx == max_transact_idx)
        .first()[0]
    )

    if max_block_number is None:
        db_session.remove()
        return 8973570

    db_session.remove()
    return max_block_number


def delete_last_block():
    max_block_number = get_last_block_number()

    min_transact_idx_to_delete = None
    min_swap_idx_to_delete = None

    db_session = db.get_db_session()

    min_transact_idx_to_delete = (
        db_session.query(func.min(transactions.dimTransactions.transact_idx))
        .where(transactions.dimTransactions.block_number >= max_block_number)
        .first()[0]
    )

    if min_transact_idx_to_delete is not None:
        min_swap_idx_to_delete = (
            db_session.query(func.min(swaps.factSwaps.swap_idx))
            .where(swaps.factSwaps.transact_idx >= min_transact_idx_to_delete)
            .first()[0]
        )

    logger.info("Deleting block_numbers >= {b}".format(b=max_block_number))

    db_session.query(blocks.dimBlocks).where(blocks.dimBlocks.block_number >= max_block_number).delete()

    if min_transact_idx_to_delete is not None:
        db_session.query(transactions.dimTransactions).where(
            transactions.dimTransactions.transact_idx >= min_transact_idx_to_delete
        ).delete()
    if min_swap_idx_to_delete is not None:
        db_session.query(swaps.factSwaps).where(swaps.factSwaps.swap_idx >= min_swap_idx_to_delete).delete()

    db_session.commit()

    db_session.remove()


def get_pair_ids_to_dict():

    pairs_dict = dict()

    db_session = db.get_db_session()

    data = db_session.query(pairs.dimPairs.id, pairs.dimPairs.pair_idx).all()

    for row in data:

        pairs_dict[codecs.encode(row.id, "hex_codec")] = row.pair_idx

    db_session.remove()
    return pairs_dict


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


def insert_dim_blocks(list_of_objects: list):
    df = pd.DataFrame(list_of_objects)

    non_scoped_db_session = db.get_non_scoped_db_session()
    conn = non_scoped_db_session.connect()

    try:
        df.to_sql(
            blocks.dimBlocks.__tablename__,
            schema=blocks.dimBlocks.__table_args__["schema"],
            con=conn,
            method="multi",
            index=False,
            if_exists="append",
            chunksize=100,
        )
    except exc.IntegrityError as e:
        logger.error(e)
        logger.error(list_of_objects)
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


def get_transacts_to_dict(transact_idx_greater_than=0) -> dict:
    transact_dict = dict()

    db_session = db.get_db_session()
    data = (
        db_session.query(transactions.dimTransactions.id, transactions.dimTransactions.transact_idx)
        .where(transactions.dimTransactions.transact_idx > transact_idx_greater_than)
        .all()
    )

    for row in data:
        transact_dict[row.id] = row.transact_idx

    db_session.remove()
    return transact_dict


def insert_dim_transacts(list_of_objects: list):
    df = pd.DataFrame(list_of_objects)

    non_scoped_db_session = db.get_non_scoped_db_session()
    conn = non_scoped_db_session.connect()

    df.to_sql(
        transactions.dimTransactions.__tablename__,
        schema=transactions.dimTransactions.__table_args__["schema"],
        con=conn,
        method="multi",
        index=False,
        if_exists="append",
        chunksize=100,
    )

    conn.close()
    non_scoped_db_session.dispose()


def insert_fact_swaps(list_of_objects: list):
    df = pd.DataFrame(list_of_objects)

    non_scoped_db_session = db.get_non_scoped_db_session()
    conn = non_scoped_db_session.connect()

    df.to_sql(
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
