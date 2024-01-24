import os
import re
import sys
from datetime import datetime
from time import sleep

from tj_worker.utils import log

from ..swap_getter import swaps_to_csv, thegraph, upload_data
from ..utils import azure_storage, csv_functions

logger = log.setup_custom_logger(name=__file__)


class SwapGetter(object):
    """Query traderjoe swap data in a loop and save to azure blob storage / SFTP.

    Example Usage:
        GetSwaps = SwapGetter()
        GetSwaps.run_loop()

    """

    def __init__(self, testing: bool = False, azure_storage_container: str = "swapdata"):
        self.azure_storage_container = azure_storage_container

        dir_name = os.path.dirname(__file__).replace(os.getcwd() + "/", "")
        self.local_file_path = os.path.join(dir_name, "data")
        if not os.path.exists(self.local_file_path):
            os.mkdir(self.local_file_path)

        self.testing = testing
        self.GraphAPI = thegraph.GraphAPI()

        self.UploadData = upload_data.DataUploader(
            local_file_path=self.local_file_path,
            azure_storage_container=self.azure_storage_container,
        )
        self.SwapsToCSV = swaps_to_csv.SwapParserToCSV(local_file_path=self.local_file_path)

        self.data_current_timestamp = datetime.utcnow()

        self.clear_existing_files()

    def run_loop(self):
        """In a loop, retrieve swap data from GraphAPI and insert into database

        Before loop starts, the last block number in the database is deleted
        to ensure the last block wasn't only partially inserted into database.

        Args:
            batch_size (int, optional): Number of API queries to perform before inserting
                                        into database. Defaults to 10.
        """

        self._set_current_block_number()

        i = 0

        while True:
            i += 1
            self._get_data(block_number=self.current_block_number)
            self._upload_data()
            self._set_current_block_number()
            self._sleep_time()
            if self.testing and i > 3:
                break

    def clear_existing_files(self):
        file_names = [fn for fn in os.listdir(self.local_file_path)]
        for file in file_names:
            csv_functions.remove_file(full_filepath=os.path.join(self.local_file_path, file))

    def _set_current_block_number(self):
        """Set current_block_number to the latest block_number processed

        If the ParseData class is empty, query the database to get the
        last block number

        If the ParseData class is not empty, use ParseData.map_block_to_transact_id_to_swap_id
        to get the last block_number parsed
        """
        if self.SwapsToCSV.max_block_number_processed == 0:
            initial_block = 8973570
            max_block_uploaded = self._get_last_uploaded_block()
            logger.info("Last block uploaded from file = {b}".format(b=max_block_uploaded))
            self.current_block_number = max(max_block_uploaded + 1, initial_block)
        else:
            self.current_block_number = self.SwapsToCSV.max_block_number_processed + 1

    def _sleep_time(self):
        def delay(timestamp: datetime, seconds_to_now: int = 30):
            sec_diff = datetime.utcnow() - timestamp
            sleep_time = int(seconds_to_now - sec_diff.total_seconds())
            return sleep_time

        sleep_time = delay(timestamp=self.data_current_timestamp)

        if sleep_time > 0:
            self._upload_data(override_flag=True)
            sleep_time = delay(timestamp=self.data_current_timestamp)
            logger.info("Sleeping for {s}".format(s=sleep_time))
            sleep(sleep_time)

    def _get_data(self, block_number: int):
        """Retrieve data from GraphAPI and parse data via ParseData class.

        Args:
            block_number (int): The minimum block number to query
        """
        request_start = datetime.utcnow()
        data = self.GraphAPI.get_transactions(block_number=block_number)

        duration = datetime.utcnow() - request_start

        if len(data) > 0:
            self.data_current_timestamp = datetime.utcfromtimestamp(int(data[-1]["timestamp"]))
        else:
            self.data_current_timestamp = datetime.utcnow()

        timestamp_formatted = self.data_current_timestamp.strftime("%Y-%m-%dT%H:%M:%S")

        logger.info(
            "Block Number = {b} | len(data) = {l} | Request Duration = {t} | Block Time = {bt}".format(
                b=block_number, l=len(data), t=duration, bt=timestamp_formatted
            )
        )

        self.SwapsToCSV.parse_all_data(data=data)

    def _upload_data(self, threshold_count: int = 3000, override_flag: bool = False):
        self.UploadData.set_files_to_upload()

        if len(self.UploadData.files_to_upload._items) >= threshold_count or override_flag or self.testing:
            self.UploadData.upload_files()

    def _get_last_uploaded_block(self):
        blocks_uploaded = azure_storage.get_blob_names(
            container_name=self.azure_storage_container,
            blobname_starts_with="swaps_raw",
            blobname_ends_with=".csv",
            limit=10,
        )

        for file in reversed(blocks_uploaded):
            try:
                return int(re.search(r"\d+", file).group())
            except AttributeError:
                pass

        blocks_uploaded = azure_storage.get_blob_names(
            container_name=self.azure_storage_container,
            blobname_starts_with="processed/swaps_raw",
            blobname_ends_with=".csv",
            limit=10,
        )

        for file in reversed(blocks_uploaded):
            try:
                return int(re.search(r"\d+", file).group())
            except AttributeError:
                pass
        return 0


def run():
    GetSwaps = SwapGetter()
    logger.info("Initializing....")
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        GetSwaps.run_loop()
    except (KeyboardInterrupt):
        sys.exit()
