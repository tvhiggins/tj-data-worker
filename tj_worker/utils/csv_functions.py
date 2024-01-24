import csv
import os

import pandas as pd
from tj_worker.utils import log

logger = log.setup_custom_logger(name=__file__)


def append_list_of_lists_to_csv(full_filepath, data, headers: list = None):
    """Write dictionary to CSV file"""

    if not os.path.exists(full_filepath):
        with open(full_filepath, "a") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, delimiter=",")
            if headers is not None:
                writer.writerow(headers)

    with open(full_filepath, "a") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, delimiter=",")
        writer.writerows(data)


def append_list_to_csv(full_filepath, data, headers: list = None):
    """Write dictionary to CSV file"""

    if not os.path.exists(full_filepath):
        with open(full_filepath, "a") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, delimiter=",")
            if headers is not None:
                writer.writerow(headers)

    with open(full_filepath, "a") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, delimiter=",")
        writer.writerow(data)


def remove_file(full_filepath):
    if os.path.exists(full_filepath):
        os.remove(full_filepath)
    else:
        logger.info("File does not exist: {f}".format(f=full_filepath))


def read_csv_to_dataframe(full_filepath, dtype: dict = None):
    if dtype is None:
        return pd.read_csv(full_filepath)
    else:
        return pd.read_csv(full_filepath, dtype=dtype)


def write_dataframe_to_csv(full_filepath: str, df: pd.DataFrame, index: bool = False, append: bool = False):
    if append:
        df.to_csv(full_filepath, quoting=csv.QUOTE_NONNUMERIC, index=index, mode="a")
    else:
        df.to_csv(full_filepath, quoting=csv.QUOTE_NONNUMERIC, index=index)


def write_empty_file(full_filepath, headers: list = None):
    """Write CSV file"""
    if headers is None:
        with open(full_filepath, "w"):
            pass
    else:
        with open(full_filepath, "w") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, delimiter=",")
            writer.writerow(headers)
