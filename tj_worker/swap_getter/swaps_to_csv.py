import os

from tj_worker.utils import log

from ..utils import csv_functions

logger = log.setup_custom_logger(name=__file__)


class SwapParserToCSV(object):
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

    def __init__(self, local_file_path: str):
        self.max_block_number_processed = 0
        self.local_file_path = local_file_path

    def parse_all_data(self, data: list):
        """Loop through each transaction of thegraph.com data

        Args:
            data (list): Raw data returned by thegraph.com
        """
        if len(data) == 0:
            return

        # if data is max length, dont process last block as it could be incomplete
        self.max_block_number_processed = max([int(row["blockNumber"]) for row in data])
        if len(data) == 100:
            self.max_block_number_processed -= 1

        file_data = dict()

        for transact_data in data:

            if int(transact_data["blockNumber"]) > self.max_block_number_processed:
                continue

            data = self._parse_transaction(transact_data=transact_data)

            file_name = self.block_number_to_filename(block_number=transact_data["blockNumber"])
            if file_name not in file_data:
                file_data[file_name] = list()

            file_data[file_name].extend(data)

        file_headers = [
            "transact_id",
            "block_number",
            "timestamp_unix",
            "swap_number",
            "pair_id",
            "amount0In",
            "amount0Out",
            "amount1In",
            "amount1Out",
            "amountUSD",
        ]
        for file_name in file_data:
            csv_functions.append_list_of_lists_to_csv(
                full_filepath=file_name, data=file_data[file_name], headers=file_headers
            )

    def block_number_to_filename(self, block_number: str):
        file_name = "swaps_raw_" + block_number.zfill(10) + ".csv"
        return os.path.join(self.local_file_path, file_name)

    def _parse_transaction(self, transact_data: dict):
        """Parse raw transaction data and append to dim_blocks_objects and dim_transaction_objects

        Args:
            transact_data (dict): Raw transaction data from thegraph.com
        """
        block_number = int(transact_data["blockNumber"])
        timestamp_unix = int(transact_data["timestamp"])

        swaps = list()
        # Parse each swap record for transaction
        for swap_data in transact_data["swaps"]:
            swap_number_position = swap_data["id"].find("-") + 1
            swap_record = [
                transact_data["id"],
                block_number,
                timestamp_unix,
                int(swap_data["id"][swap_number_position:]),
                swap_data["pair"]["id"],
                swap_data["amount0In"],
                swap_data["amount0Out"],
                swap_data["amount1In"],
                swap_data["amount1Out"],
                swap_data["amountUSD"],
            ]
            swaps.append(swap_record)

        return swaps

    def _clear_data(self, block_number_less_than: int):
        """
        Clear parsed data and mapping dicts older than block_number_less_than

        Args:
            block_number_less_than (int): block number
        """
        pass
