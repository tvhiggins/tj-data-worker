import codecs
from datetime import datetime

from tj_worker.utils import log

logger = log.setup_custom_logger(name=__file__)


class DataParser(object):
    """Parse and transform raw data from thegraph.com into structured data
    that is ready for inserting into database.

    After parsing and transforming is complete, the below variables contain
    the structured data ready to be used to insert into database:

        dim_blocks_objects: list of chain.dim_block records
        dim_transaction_objects: list of chain.dim_transaction records
        fact_swap_objects: list of tj.fact_swap records

    A dictionary of parsed data is stored in map_block_to_transact_id_to_swap_id. This
    dictionary ensures we don't parse duplicate data. The structure for this dictionary
    is as follows:

        map_block_to_transact_id_to_swap_id[block_number][transact_id][swap_id]

    Example Usage:
        GraphAPI = thegraph.GraphAPI()
        ParseData.parse_all_data(data=data)

    """

    def __init__(self):
        self.map_block_to_transact_id_to_swap_id = dict()

        self.dim_blocks_objects = list()
        self.dim_transaction_objects = list()
        self.fact_swap_objects = list()

    def parse_all_data(self, data: list):
        """Loop through each transaction of thegraph.com data

        Args:
            data (list): Raw data returned by thegraph.com
        """
        for transact_data in data:
            self._parse_transaction(transact_data=transact_data)

    def _parse_transaction(self, transact_data: dict):
        """Parse raw transaction data and append to dim_blocks_objects and dim_transaction_objects

        Args:
            transact_data (dict): Raw transaction data from thegraph.com
        """
        transact_id = codecs.decode(str.encode(transact_data["id"][2:]), "hex_codec")
        block_number = int(transact_data["blockNumber"])
        timestamp_unix = int(transact_data["timestamp"])
        timestamp = datetime.utcfromtimestamp(timestamp_unix)

        # if block_number has not previously been parsed, append to dim_blocks_objects
        if block_number not in self.map_block_to_transact_id_to_swap_id:
            self.map_block_to_transact_id_to_swap_id[block_number] = dict()

            self.dim_blocks_objects.append(
                {
                    "block_number": block_number,
                    "timestamp_unix": timestamp_unix,
                    "timestamp": timestamp,
                }
            )

        if len(transact_data["swaps"]) == 0:
            return

        # if transact_id has not previously been parsed, append to dim_transaction_objects
        if transact_id not in self.map_block_to_transact_id_to_swap_id[block_number]:
            self.map_block_to_transact_id_to_swap_id[block_number][transact_id] = dict()
            self.dim_transaction_objects.append({"id": transact_id, "block_number": block_number})

        # Parse each swap record for transaction
        for swap_data in transact_data["swaps"]:
            self._parse_swap(swap_data=swap_data, block_number=block_number, transact_id=transact_id)

    def _parse_swap(self, swap_data: dict, block_number: int, transact_id: bytes):
        """Parse raw swap_data dictionary and append to fact_swap_objects.

        Args:
            swap_data (dict): Raw data from thegraph.com
            block_number (int): Block number of the swap_data
            transact_id (bytes): Transaction ID of the swap_data
        """

        swap_id = str.encode(swap_data["id"][2:])
        pair_id = str.encode(swap_data["pair"]["id"][2:])
        swap_number = int(swap_data["id"][swap_data["id"].find("-") + 1:])

        # if swap_id hasn't been parsed, add to fact_swap_objects
        if swap_id not in self.map_block_to_transact_id_to_swap_id[block_number][transact_id]:
            self.map_block_to_transact_id_to_swap_id[block_number][transact_id][swap_id] = True
            self.fact_swap_objects.append(
                {
                    "transact_id": transact_id,
                    "swap_number": swap_number,
                    "swap_id": swap_id,
                    "pair_id": pair_id,
                    "amount0_in": swap_data["amount0In"],
                    "amount0_out": swap_data["amount0Out"],
                    "amount1_in": swap_data["amount1In"],
                    "amount1_out": swap_data["amount1Out"],
                    "amount_usd": swap_data["amountUSD"],
                }
            )

    def _clear_data(self, block_number_less_than: int):
        """
        Clear parsed data and mapping dicts older than block_number_less_than

        Args:
            block_number_less_than (int): block number
        """
        self.dim_blocks_objects = list()
        self.dim_transaction_objects = list()
        self.fact_swap_objects = list()

        for block_number in list(self.map_block_to_transact_id_to_swap_id.keys()):
            if block_number < block_number_less_than:
                del self.map_block_to_transact_id_to_swap_id[block_number]
