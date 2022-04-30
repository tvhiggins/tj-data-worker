import sys
from datetime import datetime

from tj_worker.utils import db_functions, log

from ..swap_updater import parse_data, thegraph, upload_data

logger = log.setup_custom_logger(name=__file__)


class SwapUpdater(object):
    """Query traderjoe swap data in a loop and update the avax db.

    Example Usage:
        UpdateSwaps = SwapUpdater()
        UpdateSwaps.run_loop()

    """

    def __init__(self, testing: bool = False):
        self.testing = testing
        self.GraphAPI = thegraph.GraphAPI()
        self.ParseData = parse_data.DataParser()
        self.UploadData = upload_data.DataUploader()

    def run_loop(self, batch_size: int = 10):
        """In a loop, retrieve swap data from GraphAPI and insert into database

        Before loop starts, the last block number in the database is deleted
        to ensure the last block wasn't only partially inserted into database.

        Limit to two uploads if self.testing.

        Args:
            batch_size (int, optional): Number of API queries to perform before inserting
                                        into database. Defaults to 10.
        """

        # in case the previous block didnt get updated completely, delete its records
        db_functions.delete_last_block()
        self._set_current_block_number()

        upload_count = 0
        batch_count = 0
        start_batch_time = datetime.utcnow()

        while True:
            batch_count += 1

            sys.stdout.flush()
            self._get_data(block_number=self.current_block_number)

            self._set_current_block_number()

            if batch_count >= batch_size:
                self._upload_data()
                self._clear_data_cache()

                batch_count = 0

                insert_duration = datetime.utcnow() - start_batch_time
                logger.info("Batch duration= {t}".format(t=insert_duration))

                start_batch_time = datetime.utcnow()

                upload_count += 1

            if self.testing and upload_count > 1:
                break

    def _set_current_block_number(self):
        """Set current_block_number to the latest block_number processed

        If the ParseData class is empty, query the database to get the
        last block number

        If the ParseData class is not empty, use ParseData.map_block_to_transact_id_to_swap_id
        to get the last block_number parsed
        """
        if len(self.ParseData.map_block_to_transact_id_to_swap_id) == 0:
            self.current_block_number = db_functions.get_last_block_number() + 1
        else:
            self.current_block_number = max(list(self.ParseData.map_block_to_transact_id_to_swap_id.keys()))

    def _get_data(self, block_number: int):
        """Retrieve data from GraphAPI and parse data via ParseData class.

        Args:
            block_number (int): The minimum block number to query
        """
        logger.info("Getting data for block_number = {b}".format(b=block_number))
        data = self.GraphAPI.get_transactions(block_number=block_number)
        logger.info("Parsing data. Length = {l}".format(l=len(data)))
        self.ParseData.parse_all_data(data=data)

    def _upload_data(self):
        """Insert parsed data into database.

        Take results from ParseData class and pass to UploadData class to
        insert into the database.
        """

        self.UploadData.upload_data(
            dim_blocks_objects=self.ParseData.dim_blocks_objects,
            dim_transaction_objects=self.ParseData.dim_transaction_objects,
            fact_swap_objects=self.ParseData.fact_swap_objects,
        )

    def _clear_data_cache(self):
        """Delete old data from ParseData and UploadData.

        This prevents memory build up of data that is no longer needed. All data
        less than the current_block_number can be removed.
        """
        self.ParseData._clear_data(block_number_less_than=self.current_block_number)
        self.UploadData._clear_data(block_number_less_than=self.current_block_number)


def run():
    UpdateSwaps = SwapUpdater()
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        UpdateSwaps.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
