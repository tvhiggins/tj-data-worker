import os
from datetime import datetime

import pandas as pd

from ..utils import azure_storage, csv_functions, data_classes, log
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

    def __init__(self, local_file_path, azure_storage_container: str):
        self.azure_storage_container = azure_storage_container

        self.local_file_path = local_file_path
        self.files_to_upload = data_classes.ListofFiles()
        self.Pairs = PairMaintainer(
            local_file_path=local_file_path, azure_storage_container=self.azure_storage_container
        )
        self.swap_df_dtypes = {
            "transact_id": str,
            "block_number": int,
            "timestamp_unix": int,
            "swap_number": int,
            "pair_id": str,
            "amount0In": str,
            "amount0Out": str,
            "amount1In": str,
            "amount1Out": str,
            "amountUSD": str,
        }

    def set_files_to_upload(self):
        self.files_to_upload = data_classes.ListofFiles()
        file_names = [fn for fn in os.listdir(self.local_file_path) if "swaps_raw" in fn]
        file_names.sort()

        for file in file_names:
            file_item = data_classes.FileItem(file_name=file, full_local_path=os.path.join(self.local_file_path, file))
            self.files_to_upload.addFile(file=file_item)

    def upload_files(self):
        if len(self.files_to_upload._items) == 0:
            return

        logger.info("Uploading Files: {f}".format(f=len(self.files_to_upload._items)))

        master_df = pd.DataFrame()
        file_to_upload = self.files_to_upload._items[-1]

        for file in self.files_to_upload._items:
            file_df = csv_functions.read_csv_to_dataframe(
                full_filepath=file.full_local_path, dtype=self.swap_df_dtypes
            )
            master_df = pd.concat([master_df, file_df], ignore_index=True)

        csv_functions.write_dataframe_to_csv(file_to_upload.full_local_path, df=master_df)

        azure_storage.upload_localfile(
            local_file_path=file_to_upload.full_local_path,
            upload_filename=file_to_upload.file_name,
            container_name=self.azure_storage_container,
        )
        logger.info("Uploaded: {f}".format(f=file_to_upload.file_name))

        for file in self.files_to_upload._items:
            csv_functions.remove_file(full_filepath=file.full_local_path)

        pair_ids = list(set(master_df["pair_id"].tolist()))

        self.Pairs.upload_pairs(pair_ids=pair_ids)


class PairMaintainer(object):
    """Maintain dictionary of pair data from database. Insert Pair data from thegraph.com.

    map_pair_ids_to_idxs is a dictionary that stores pair data from the database table tj.dim_pairs.

    Example Usage:
        Pairs = PairMaintainer()

        if pair_id not in Pairs.map_pair_ids_to_idxs:
            Pairs.add_pair(pair_id=swap["pair_id"])

    """

    def __init__(self, local_file_path, azure_storage_container: str):
        self.azure_storage_container = azure_storage_container
        self.pair_ids_uploaded = dict()
        self.local_file_path = local_file_path
        self.pair_file = data_classes.FileItem(
            file_name="pairs.csv",
            full_local_path=os.path.join(local_file_path, "pairs.csv"),
            headers=[
                "pair_id",
                "name",
                "token0_id",
                "token0_symbol",
                "token0_name",
                "token1_id",
                "token1_symbol",
                "token1_name",
            ],
        )
        self._download_master_pair_file()
        self._set_pair_ids_uploaded()
        csv_functions.remove_file(full_filepath=self.pair_file.full_local_path)

    def upload_pairs(self, pair_ids: list):

        pairs_to_add = [x for x in pair_ids if x not in self.pair_ids_uploaded]

        if len(pairs_to_add) == 0:
            return
        logger.info(
            "Pairs to Add: {p} | Existing Pairs: {e}".format(p=len(pairs_to_add), e=len(self.pair_ids_uploaded))
        )
        self._download_master_pair_file()

        for pair_id in pairs_to_add:
            self.add_pair(pair_id=pair_id)

        azure_storage.delete_blob(file_name=self.pair_file.file_name, container_name=self.azure_storage_container)
        azure_storage.upload_localfile(
            local_file_path=self.pair_file.full_local_path,
            upload_filename=self.pair_file.file_name,
            container_name=self.azure_storage_container,
        )
        logger.info("Uploaded: {f}".format(f=self.pair_file.file_name))

        csv_functions.remove_file(full_filepath=self.pair_file.full_local_path)
        logger.info("Removed: {f}".format(f=self.pair_file.full_local_path))

    def _download_master_pair_file(self):

        csv_functions.write_empty_file(full_filepath=self.pair_file.full_local_path, headers=self.pair_file.headers)

        azure_storage.download_blobs(
            container_name=self.azure_storage_container,
            blobname_starts_with=self.pair_file.file_name,
            blobname_ends_with="",
            blobname_contains="",
            destination_folder=self.local_file_path,
        )

    def _set_pair_ids_uploaded(self):
        """Update map_pair_ids_to_idxs dictionary with data from tj.dim_pairs table."""
        try:
            df = csv_functions.read_csv_to_dataframe(full_filepath=self.pair_file.full_local_path)
            pair_ids = list(set(df["pair_id"].tolist()))
            self.pair_ids_uploaded = {p: True for p in pair_ids}
            logger.info("Len of pair_ids existing = {}".format(len(self.pair_ids_uploaded)))
        except pd.errors.EmptyDataError:
            self.pair_ids_uploaded = dict()

    def add_pair(self, pair_id: str):
        """Given a pair_id, query thegraph.com and insert the pair into tj.dim_pairs and chain.dim_tokens.

        For pair data returned by API, ensure underlying tokens exist in chain.dim_tokens, if tokens
        do not exist, insert into chain.dim_tokens

        Args:
            pair_id (bytes): id for pair returned by thegraph.com
        """

        request_start = datetime.utcnow()

        # Get pair information from API
        with thegraph.GraphAPI() as g:
            data = g.get_pair(id=pair_id)

        duration = datetime.utcnow() - request_start
        row_data = [
            data["id"],
            data["name"],
            data["token0"]["id"],
            data["token0"]["symbol"],
            data["token0"]["name"],
            data["token1"]["id"],
            data["token1"]["symbol"],
            data["token1"]["name"],
        ]
        csv_functions.append_list_to_csv(full_filepath=self.pair_file.full_local_path, data=row_data)
        self.pair_ids_uploaded[data["id"]] = True
        logger.info("Token Added = {name} | Request Duration = {d}".format(name=data["name"], d=duration))
