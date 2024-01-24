import os
import sys
from time import sleep

import pandas as pd

from ..swap_etl import maintain_block_swaps
from ..utils import azure_storage, csv_functions, data_classes, log

logger = log.setup_custom_logger(name=__file__)


class SwapETL(object):
    """Query traderjoe swap data in a loop and save to azure blob storage / SFTP.

    Example Usage:
        GetSwaps = SwapUpdater()
        GetSwaps.run_loop()

    """

    def __init__(self, testing: bool = False, azure_storage_container: str = "swapdata"):
        self.azure_storage_container = azure_storage_container

        dir_name = os.path.dirname(__file__).replace(os.getcwd() + "/", "")
        self.local_file_path = os.path.join(dir_name, "data")
        if not os.path.exists(self.local_file_path):
            os.mkdir(self.local_file_path)

        self.raw_file_dir = os.path.join(self.local_file_path, "raw")
        if not os.path.exists(self.raw_file_dir):
            os.mkdir(self.raw_file_dir)

        self.testing = testing

        self.files_to_process = data_classes.ListofFiles()

        self.MaintainBlockSwaps = maintain_block_swaps.BlockSwapMaintainer(
            local_file_path=self.local_file_path,
            azure_storage_container=self.azure_storage_container,
        )

    def run_loop(self):
        """In a loop, retrieve swap data from GraphAPI and insert into database

        Before loop starts, the last block number in the database is deleted
        to ensure the last block wasn't only partially inserted into database.

        Args:
            batch_size (int, optional): Number of API queries to perform before inserting
                                        into database. Defaults to 10.
        """
        i = 0

        while True:
            i += 1
            self.clear_existing_files()

            self._download_files_to_process()

            self._set_files_to_process()

            self._process_files()

            self._maintain_files()

            if self.testing:
                break

    def clear_existing_files(self):
        file_names = [fn for fn in os.listdir(self.local_file_path) if ".csv" in fn]
        for file in file_names:
            csv_functions.remove_file(full_filepath=os.path.join(self.local_file_path, file))

        file_names = [fn for fn in os.listdir(self.raw_file_dir) if ".csv" in fn]
        for file in file_names:
            csv_functions.remove_file(full_filepath=os.path.join(self.raw_file_dir, file))

    def _download_files_to_process(self):

        azure_storage.download_block_blobs(
            container_name=self.azure_storage_container,
            blobname_starts_with="swaps_raw",
            blobname_ends_with=".csv",
            destination_folder=self.raw_file_dir,
            limit=50,
            block_number_greater_than=self.MaintainBlockSwaps.max_block_uploaded,
        )

    def _set_files_to_process(self):
        self.files_to_process = data_classes.ListofFiles()
        file_names = [fn for fn in os.listdir(self.raw_file_dir) if "swaps_raw" in fn]
        file_names.sort()

        for file in file_names:
            file_item = data_classes.FileItem(file_name=file, full_local_path=os.path.join(self.raw_file_dir, file))
            self.files_to_process.addFile(file=file_item)

        if len(self.files_to_process._items) > 0:
            logger.info(
                "Files to Process = {f} | Last file = {l}".format(
                    f=len(self.files_to_process._items), l=self.files_to_process._items[-1].file_name
                )
            )
        else:
            logger.info("No Files to Process")
            sleep(15)

    def _process_files(self):
        self.MaintainBlockSwaps.reset()

        for i, file in enumerate(self.files_to_process._items):
            if i % 10 == 0:
                logger.info(
                    "{i} of {l} | Processing file {f}".format(
                        i=i + 1, l=len(self.files_to_process._items), f=file.file_name
                    )
                )
            self.MaintainBlockSwaps.add_to_master_dfs(file=file)

        self.MaintainBlockSwaps.insert_master_blocks_df()

        self.MaintainBlockSwaps.insert_master_swaps_df()

    def _maintain_files(self):
        if len(self.files_to_process._items) == 0:
            return

        combined_csv = self._combine_files()

        blob_storage_full_path = os.path.join("processed", combined_csv.file_name)

        logger.info("Uploading File: {f}".format(f=blob_storage_full_path))
        azure_storage.upload_localfile(
            container_name=self.azure_storage_container,
            local_file_path=combined_csv.full_local_path,
            upload_filename=blob_storage_full_path,
        )

        logger.info("Deleting {d} files from azure storage...".format(d=len(self.files_to_process._items)))
        for file in self.files_to_process._items:
            azure_storage.delete_blob(file_name=file.file_name, container_name=self.azure_storage_container)

    def _combine_files(self) -> data_classes.FileItem:

        # combine all files in the list
        combined_csv = pd.concat([pd.read_csv(f.full_local_path) for f in self.files_to_process._items])

        # export to csv
        combined_csv_name_full_path = os.path.join(self.local_file_path, self.files_to_process._items[-1].file_name)
        combined_file = data_classes.FileItem(
            file_name=self.files_to_process._items[-1].file_name, full_local_path=combined_csv_name_full_path
        )
        combined_csv.to_csv(combined_file.full_local_path, index=False, encoding="utf-8")

        return combined_file


def run():
    InsertSwaps = SwapETL()
    logger.info("Initializing....")
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        InsertSwaps.run_loop()
    except (KeyboardInterrupt):
        sys.exit()
