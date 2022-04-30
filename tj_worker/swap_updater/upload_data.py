import codecs
from datetime import datetime

from tj_worker.model import pairs, tokens
from tj_worker.utils import db_functions, log

from . import thegraph

logger = log.setup_custom_logger(name=__file__)


class DataUploader(object):
    """Insert parsed data from thegraph.com into database.

    Tables inserted into:
            chain.dim_blocks
            chain.dim_transactions
            tj.fact_swaps

    Attributes:
        map_transact_id_to_idx (dict): Map transaction_id to its transact_idx. The idx is an integer from the
                                            identity column of chain.dim_transactions. The Idx for each
                                            transaction is needed for inserting swap records. Transact_Idx
                                            is a FK in the tj.fact_swaps table.

        max_transact_idx (int): The previous max transact_idx value in chain.dim_transactions.
                                    This is used when querying the newly inserted transaction idx values
                                    in order to populate map_transact_id_to_idx. This allows the query to be
                                    able to filter WHERE transact_idx > max_transact_idx.

        map_block_to_transact_id (dict): Maps block_number to a list of transaction_ids. This is used when clearing
                                            stale data. Stale data is deleted according to block number, so given a
                                            block number, it is known which data transaction_ids in
                                            map_transact_id_to_idx can be removed.

    Example Usage:
        GraphAPI = thegraph.GraphAPI()
        ParseData.parse_all_data(data=data)

    """

    def __init__(self):
        self.max_transact_idx = db_functions.get_max_transact_idx()
        self.map_block_to_transact_id = dict()
        self.map_transact_id_to_idx = dict()

        self.Pairs = PairMaintainer()

    def upload_data(self, dim_blocks_objects: list, dim_transaction_objects: list, fact_swap_objects: list):
        """Insert block, transaction, and swap data into database

        Args:
            dim_blocks_objects (list): List of dictionaries to insert into chain.dim_blocks
            dim_transaction_objects (list): List of dictionaries to insert into chain.dim_transactions
            fact_swap_objects (list): List of dictionaries to insert into tj.fact_swap
        """
        logger.info(
            "Length of objects: blocks={b}  transacts={t}  swaps={s}".format(
                b=len(dim_blocks_objects), t=len(dim_transaction_objects), s=len(fact_swap_objects)
            )
        )
        self.dim_blocks_objects = dim_blocks_objects
        self.dim_transaction_objects = dim_transaction_objects
        self.fact_swap_objects = fact_swap_objects

        self._upload_blocks()
        self._upload_transactions()
        self._upload_swaps()

    def _upload_blocks(self):
        """Insert dim_blocks_objects list into database table chain.dim_blocks"""
        if len(self.dim_blocks_objects) == 0:
            return

        start_time = datetime.utcnow()

        db_functions.insert_dim_blocks(list_of_objects=self.dim_blocks_objects)

        insert_duration = datetime.utcnow() - start_time
        logger.info("Insert duration= {t}".format(t=insert_duration))

        # add to mapping dictionary
        for block_number in [row["block_number"] for row in self.dim_blocks_objects]:
            if block_number not in self.map_block_to_transact_id:
                self.map_block_to_transact_id[block_number] = list()

    def _upload_transactions(self):
        """Insert dim_transaction_objects list into database table chain.dim_transactions"""
        if len(self.dim_transaction_objects) == 0:
            return

        for transact in self.dim_transaction_objects:
            self.map_block_to_transact_id[transact["block_number"]].append(transact["id"])

        start_time = datetime.utcnow()

        db_functions.insert_dim_transacts(list_of_objects=self.dim_transaction_objects)

        insert_duration = datetime.utcnow() - start_time
        logger.info("Insert duration= {t}".format(t=insert_duration))

        map_transact_id_to_idx = db_functions.get_transacts_to_dict(transact_idx_greater_than=self.max_transact_idx)

        self.max_transact_idx = max(list(map_transact_id_to_idx.values()))

        self.map_transact_id_to_idx.update(map_transact_id_to_idx)

    def _upload_swaps(self):
        """Insert fact_swap_objects list into database table tj.fact_swaps

        If the swap's pair doesnt exist in database, use PairMaintainer to insert the
        pair into tj.dim_pairs.

        """
        if len(self.fact_swap_objects) == 0:
            return

        # for each dictionary in fact_swap_objects, remove unnecessaary data and
        # ensure pair_id exists in database
        for swap in self.fact_swap_objects:
            swap["transact_idx"] = self.map_transact_id_to_idx[swap["transact_id"]]
            del swap["transact_id"]
            del swap["swap_id"]

            if swap["pair_id"] not in self.Pairs.map_pair_ids_to_idxs:
                self.Pairs.add_pair(pair_id=swap["pair_id"])

            swap["pair_idx"] = self.Pairs.map_pair_ids_to_idxs[swap["pair_id"]]

            del swap["pair_id"]

        start_time = datetime.utcnow()

        # insert swap data into tj.fact_swaps
        db_functions.insert_fact_swaps(list_of_objects=self.fact_swap_objects)

        insert_duration = datetime.utcnow() - start_time
        logger.info("Insert duration= {t}".format(t=insert_duration))

    def _clear_data(self, block_number_less_than: int) -> list:
        """Clear mapping dicts older than block_number_less_than

        Args:
            block_number_less_than (int): block number
        """

        """_summary_

        Args:
            block_number_less_than (int): _description_


        """

        transact_ids_to_clear = list()
        for block_number in [x for x in self.map_block_to_transact_id.keys() if x < block_number_less_than]:
            transact_ids_to_clear.extend(self.map_block_to_transact_id[block_number])
            del self.map_block_to_transact_id[block_number]

        for transact_id in transact_ids_to_clear:
            del self.map_transact_id_to_idx[transact_id]

        logger.info(
            "Length of maps: transact_id_to_idx={t}  block_to_transact_id={b}".format(
                t=len(self.map_transact_id_to_idx), b=len(self.map_block_to_transact_id)
            )
        )


class PairMaintainer(object):
    """Maintain dictionary of pair data from database. Insert Pair data from thegraph.com.

    map_pair_ids_to_idxs is a dictionary that stores pair data from the database table tj.dim_pairs.

    Example Usage:
        Pairs = PairMaintainer()

        if pair_id not in Pairs.map_pair_ids_to_idxs:
            Pairs.add_pair(pair_id=swap["pair_id"])

    """

    def __init__(self):
        self.update_map_pair_ids()

    def update_map_pair_ids(self):
        """Update map_pair_ids_to_idxs dictionary with data from tj.dim_pairs table."""
        self.map_pair_ids_to_idxs = db_functions.get_pair_ids_to_dict()

    def add_pair(self, pair_id: bytes):
        """Given a pair_id, query thegraph.com and insert the pair into tj.dim_pairs and chain.dim_tokens.

        For pair data returned by API, ensure underlying tokens exist in chain.dim_tokens, if tokens
        do not exist, insert into chain.dim_tokens

        Args:
            pair_id (bytes): id for pair returned by thegraph.com
        """

        pair_str = "0x" + pair_id.decode()
        logger.info("Adding pair id = {id}".format(id=pair_str))

        # Get pair information from API
        with thegraph.GraphAPI() as g:
            data = g.get_pair(id=pair_str)

        # Parse raw data
        token0_id = codecs.decode(str.encode(data["token0"]["id"][2:]), "hex_codec")
        token0_object = tokens.dimTokens(id=token0_id, symbol=data["token0"]["symbol"], name=data["token0"]["name"])
        token1_id = codecs.decode(str.encode(data["token1"]["id"][2:]), "hex_codec")
        token1_object = tokens.dimTokens(id=token1_id, symbol=data["token1"]["symbol"], name=data["token1"]["name"])
        pair_id = codecs.decode(str.encode(data["id"][2:]), "hex_codec")

        # insert tokens into chain.dim_tokens
        token0_idx = db_functions.insert_dim_tokens(token_object=token0_object)
        token1_idx = db_functions.insert_dim_tokens(token_object=token1_object)

        # insert pair data in tj.dim_pairs
        pair_object = pairs.dimPairs(id=pair_id, name=data["name"], token0_idx=token0_idx, token1_idx=token1_idx)
        db_functions.insert_dim_pairs(pair_object=pair_object)

        logger.info("Inserted Pair Name = {n}".format(n=data["name"]))

        self.update_map_pair_ids()
