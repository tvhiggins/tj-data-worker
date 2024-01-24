from datetime import datetime

import numpy as np
import pandas as pd

from ..swap_etl import maintain_pair_tokens
from ..utils import csv_functions, data_classes, db_functions, log

logger = log.setup_custom_logger(name=__file__)


class BlockSwapMaintainer(object):
    """Maintain dictionary of pair data from database. Insert Pair data from thegraph.com.

    map_pair_ids_to_idxs is a dictionary that stores pair data from the database table tj.dim_pairs.

    Example Usage:
        Pairs = PairMaintainer()

        if pair_id not in Pairs.map_pair_ids_to_idxs:
            Pairs.add_pair(pair_id=swap["pair_id"])

    """

    def __init__(self, local_file_path, azure_storage_container: str):
        self.local_file_path = local_file_path
        self._max_block_uploaded = 0
        self.max_block_uploaded = db_functions.get_last_inserted_block_number()
        self._last_block_candidate = 0
        self.MaintainPairTokens = maintain_pair_tokens.PairTokenMaintainer(
            local_file_path=self.local_file_path, azure_storage_container=azure_storage_container
        )

        self.swap_df_dtypes = {
            "transact_id": str,
            "block_number": int,
            "timestamp_unix": int,
            "swap_number": int,
            "pair_id": str,
            "amount0In": float,
            "amount0Out": float,
            "amount1In": float,
            "amount1Out": float,
            "amountUSD": float,
        }
        self.reset()

    def reset(self):
        self.master_blocks_df = pd.DataFrame()
        self.master_swaps_df = pd.DataFrame()

    def get_max_block_uploaded(self):
        return self._max_block_uploaded

    def set_max_block_uploaded(self, value):
        if value > self._max_block_uploaded:
            self._max_block_uploaded = value

    def add_to_master_dfs(self, file: data_classes.FileItem):

        file_df = csv_functions.read_csv_to_dataframe(full_filepath=file.full_local_path, dtype=self.swap_df_dtypes)

        self._last_block_candidate = file_df["block_number"].max()

        file_df = file_df[file_df["pair_id"].isin(self.MaintainPairTokens.valid_pair_ids)]

        file_swap_df = self.get_clean_file_swap_df(file_df=file_df, max_block_uploaded=self.max_block_uploaded)
        file_block_df = self.get_clean_file_block_df(file_df=file_df)

        self.master_swaps_df = pd.concat([self.master_swaps_df, file_swap_df], ignore_index=True)
        self.master_blocks_df = pd.concat([self.master_blocks_df, file_block_df], ignore_index=True)

    def insert_master_blocks_df(self):
        def convert_unix_to_timestamp(timestamp_unix: int):
            return datetime.utcfromtimestamp(timestamp_unix)

        if self.master_blocks_df.shape[0] == 0:
            return

        self.master_blocks_df["timestamp"] = pd.Series(
            map(convert_unix_to_timestamp, self.master_blocks_df["timestamp_unix"])
        )

        logger.info("Inserting {c} rows into dim_blocks...".format(c=self.master_blocks_df.shape[0]))
        db_functions.insert_dim_blocks(blocks_df=self.master_blocks_df)

    def insert_master_swaps_df(self):
        def add_pair_idx(pair_id: str):
            return self.MaintainPairTokens.map_pair_id_to_idx[pair_id]

        if self.master_swaps_df.shape[0] == 0:
            return

        self.master_swaps_df["pair_idx"] = pd.Series(map(add_pair_idx, self.master_swaps_df["pair_id"]))

        self.master_swaps_df = self.master_swaps_df.drop(columns=["pair_id"])

        logger.info("Inserting {c} rows into fact_swap...".format(c=self.master_swaps_df.shape[0]))
        db_functions.insert_fact_swaps(swaps_df=self.master_swaps_df)

        if self._last_block_candidate > 0:
            self.max_block_uploaded = self._last_block_candidate

    @staticmethod
    def get_clean_file_swap_df(file_df: pd.DataFrame, max_block_uploaded: int = 0) -> pd.DataFrame:
        swap_columns = ["block_number", "pair_id", "amount0In", "amount0Out", "amount1In", "amount1Out", "amountUSD"]
        file_swap_df = file_df.drop(columns=[col for col in file_df if col not in swap_columns])

        file_swap_df = file_swap_df[
            (
                (file_swap_df.amount0In > 0)
                & (file_swap_df.amount0Out == 0)
                & (file_swap_df.amount1In == 0)
                & (file_swap_df.amount1Out > 0)
            )
            | (
                (file_swap_df.amount0In == 0)
                & (file_swap_df.amount0Out > 0)
                & (file_swap_df.amount1In > 0)
                & (file_swap_df.amount1Out == 0)
            )
        ]

        file_swap_df = file_swap_df[file_swap_df.block_number > max_block_uploaded]

        file_swap_df["isSell"] = np.where(file_swap_df.amount0In > 0, 1, 0)
        file_swap_df = file_swap_df.groupby(by=["block_number", "pair_id", "isSell"], as_index=False).sum()
        file_swap_df = file_swap_df.drop(columns=[col for col in file_swap_df if col not in swap_columns])

        file_swap_df = file_swap_df.rename(
            columns={
                "amount0In": "amount0_in",
                "amount0Out": "amount0_out",
                "amount1In": "amount1_in",
                "amount1Out": "amount1_out",
                "amountUSD": "amount_usd",
            }
        )
        return file_swap_df

    @staticmethod
    def get_clean_file_block_df(file_df: pd.DataFrame) -> pd.DataFrame:
        block_columns = ["block_number", "timestamp_unix"]
        file_block_df = file_df.drop(columns=[col for col in file_df if col not in block_columns])
        file_block_df = file_block_df.drop_duplicates()

        return file_block_df

    # creating a property object
    max_block_uploaded = property(get_max_block_uploaded, set_max_block_uploaded)
