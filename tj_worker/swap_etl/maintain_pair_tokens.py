import codecs
import os

import pandas as pd
from tj_worker.model import pairs, tokens

from ..swap_etl import whitelist_tokens
from ..utils import azure_storage, csv_functions, data_classes, db_functions, log

logger = log.setup_custom_logger(name=__file__)


class PairTokenMaintainer(object):
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
        self.reset()

    def reset(self):
        self.map_token_id_to_idx = db_functions.get_token_ids_to_dict()
        self.map_pair_id_to_idx = db_functions.get_pair_ids_to_dict()
        pairs_master_df = self._get_pairs_df()
        self._insert_tokens(pairs_df=pairs_master_df)
        self._insert_pairs(pairs_df=pairs_master_df)

    def _get_pairs_df(self):

        self._download_master_pair_file()

        pairs_df = csv_functions.read_csv_to_dataframe(full_filepath=self.pair_file.full_local_path)

        csv_functions.remove_file(self.pair_file.full_local_path)

        valid_token_ids = [x["address"].lower() for x in whitelist_tokens.tokens]

        pairs_df = pairs_df[pairs_df["token0_id"].isin(valid_token_ids) & pairs_df["token1_id"].isin(valid_token_ids)]

        if pairs_df.shape[0] == 0:
            logger.error("Pairs file not found")
            exit()

        return pairs_df.drop_duplicates()

    def _insert_tokens(self, pairs_df: pd.DataFrame):
        token0_df = pairs_df[["token0_id", "token0_symbol", "token0_name"]]
        token0_df = token0_df.rename(columns={"token0_id": "id", "token0_symbol": "symbol", "token0_name": "name"})

        token1_df = pairs_df[["token1_id", "token1_symbol", "token1_name"]]
        token1_df = token1_df.rename(columns={"token1_id": "id", "token1_symbol": "symbol", "token1_name": "name"})

        token_df = pd.concat([token0_df, token1_df], ignore_index=True)

        token_df = token_df.drop_duplicates()

        token_dict = token_df.to_dict("records")

        for each in token_dict:
            if each["id"] in self.map_token_id_to_idx:
                continue
            token_id = codecs.decode(str.encode(each["id"][2:]), "hex_codec")
            token_object = tokens.dimTokens(id=token_id, symbol=each["symbol"], name=each["name"])
            self.map_token_id_to_idx[each["id"]] = db_functions.insert_dim_tokens(token_object=token_object)

    def _insert_pairs(self, pairs_df: pd.DataFrame):
        pair_df_limited = pairs_df[["pair_id", "name", "token0_id", "token1_id"]]

        pair_dict = pair_df_limited.to_dict("records")

        for each in pair_dict:
            if each["pair_id"] in self.map_pair_id_to_idx:
                continue
            pair_id_encoded = codecs.decode(str.encode(each["pair_id"][2:]), "hex_codec")
            pair_object = pairs.dimPairs(
                id=pair_id_encoded,
                name=each["name"],
                token0_idx=self.map_token_id_to_idx[each["token0_id"]],
                token1_idx=self.map_token_id_to_idx[each["token1_id"]],
            )
            self.map_pair_id_to_idx[each["pair_id"]] = db_functions.insert_dim_pairs(pair_object=pair_object)

    def _download_master_pair_file(self):

        csv_functions.write_empty_file(full_filepath=self.pair_file.full_local_path, headers=self.pair_file.headers)

        azure_storage.download_blobs(
            container_name=self.azure_storage_container,
            blobname_starts_with=self.pair_file.file_name,
            blobname_ends_with="",
            blobname_contains="",
            destination_folder=self.local_file_path,
        )

    @property
    def valid_pair_ids(self) -> list:
        return [pair_id for pair_id in self.map_pair_id_to_idx]
