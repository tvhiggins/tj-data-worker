import os

from azure.core import exceptions
from azure.storage.blob import BlobServiceClient, ContainerClient
from tj_worker.utils import log, settings
import re

logger = log.setup_custom_logger(name=__file__)


def get_blob_service_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONN_STR)


def get_container_client(blob_service_client: BlobServiceClient, container_name: str) -> ContainerClient:
    return blob_service_client.get_container_client(container_name)


def upload_localfile(local_file_path: str, upload_filename: str, container_name: str):
    blob_service_client = get_blob_service_client()

    # get client container
    blob_client_instance = blob_service_client.get_blob_client(
        container=container_name, blob=upload_filename, snapshot=None
    )

    try:
        with open(local_file_path, "rb") as data:
            blob_client_instance.upload_blob(data)
    except exceptions.ResourceExistsError:
        # blob already in folder
        logger.error("blob already in output folder. Deleting existing. blob={}".format(upload_filename))

        # delete existing
        blob_client_instance.delete_blob()

        # Retry
        upload_localfile(
            local_file_path=local_file_path, upload_filename=upload_filename, container_name=container_name
        )


def append_localfile_to_azure(local_file_path: str, upload_filename: str, container_name: str):
    # get gts-internal azure account
    blob_service_client = get_blob_service_client()

    # get client container within gts-internal account
    blob_client_instance = blob_service_client.get_blob_client(
        container=container_name, blob=upload_filename, snapshot=None
    )

    # write local file to client's processed folder in azure storage
    try:
        with open(local_file_path, "rb") as data:
            blob_client_instance.upload_blob(data, blob_type="AppendBlob")
    except exceptions.ResourceExistsError:
        # blob already in folder
        logger.error("blob already in output folder. Deleting existing. blob={}".format(upload_filename))

        # delete existing
        blob_client_instance.delete_blob()
        with open(local_file_path, "rb") as data:
            blob_client_instance.upload_blob(data, blob_type="AppendBlob")


def append_blob_sample(local_file_path: str, upload_filename: str, container_name: str):

    # Instantiate a new BlobServiceClient using a connection string
    from azure.storage.blob import BlobServiceClient

    blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONN_STR)

    # Instantiate a new ContainerClient
    container_client = blob_service_client.get_container_client(container_name)

    # Instantiate a new BlobClient
    blob_client = container_client.get_blob_client("test.csv")

    # Upload content to the Page Blob
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, blob_type="AppendBlob")

    # Download Append Blob
    with open(upload_filename, "wb") as my_blob:
        download_stream = blob_client.download_blob()
        my_blob.write(download_stream.readall())

    # Delete Append Blob
    # blob_client.delete_blob()


def download_blobs(
    container_name: str,
    blobname_starts_with="",
    blobname_ends_with=".csv",
    blobname_contains="",
    destination_folder="",
):
    blob_service_client = get_blob_service_client()
    container_client = get_container_client(blob_service_client=blob_service_client, container_name=container_name)

    blob_list = container_client.list_blobs(name_starts_with=blobname_starts_with)

    blobs_to_download = list()
    for blob in blob_list:

        if blob.name.endswith(blobname_ends_with) and blobname_contains in blob.name:

            blobs_to_download.append(blob.name)

    for i, blob in enumerate(blobs_to_download):
        if i % 10 == 0:
            logger.info("{i} of {l} | Downloading file {f}".format(i=i + 1, l=len(blobs_to_download), f=blob))

        blob_client_instance = blob_service_client.get_blob_client(container_name, blob, snapshot=None)
        local_filepath = os.path.join(destination_folder, blob)
        with open(local_filepath, "wb") as my_blob:
            blob_data = blob_client_instance.download_blob()
            blob_data.readinto(my_blob)


def download_block_blobs(
    container_name: str,
    blobname_starts_with="",
    blobname_ends_with=".csv",
    blobname_contains="",
    destination_folder="",
    limit: int = 10000000,
    block_number_greater_than: int = 0,
):
    blob_service_client = get_blob_service_client()
    container_client = get_container_client(blob_service_client=blob_service_client, container_name=container_name)

    blob_list = container_client.list_blobs(name_starts_with=blobname_starts_with)

    blobs_to_download = list()
    for blob in blob_list:

        if not blob.name.endswith(blobname_ends_with) or blobname_contains not in blob.name:
            continue

        try:
            block_number = int(re.search(r"\d+", blob.name).group())
        except AttributeError:
            continue

        if block_number > block_number_greater_than:
            blobs_to_download.append(blob.name)

        if len(blobs_to_download) >= limit:
            break

    for i, blob in enumerate(blobs_to_download):
        if i % 10 == 0:
            logger.info("{i} of {l} | Downloading file {f}".format(i=i + 1, l=len(blobs_to_download), f=blob))
        blob_client_instance = blob_service_client.get_blob_client(container_name, blob, snapshot=None)
        local_filepath = os.path.join(destination_folder, blob)
        with open(local_filepath, "wb") as my_blob:
            blob_data = blob_client_instance.download_blob()
            blob_data.readinto(my_blob)


def get_blob_names(
    container_name: str, blobname_starts_with="", blobname_ends_with=".csv", blobname_contains="", limit: int = None
):
    blob_service_client = get_blob_service_client()
    container_client = get_container_client(blob_service_client=blob_service_client, container_name=container_name)

    blob_list = container_client.list_blobs(name_starts_with=blobname_starts_with)

    blob_results = list()
    for blob in blob_list:
        if blob.name.endswith(blobname_ends_with) and blobname_contains in blob.name:
            blob_results.append(blob.name)

    if limit:
        blob_results = blob_results[-limit:]
    return blob_results


def delete_all_blobs(container_name: str):
    blob_service_client = get_blob_service_client()
    container_client = get_container_client(blob_service_client=blob_service_client, container_name=container_name)

    blob_list = container_client.list_blobs()

    for blob in blob_list:
        delete_blob(file_name=blob.name, container_name=container_name)


def delete_blob(file_name: str, container_name: str):
    # get gts-internal azure account
    blob_service_client = get_blob_service_client()

    # get client container within gts-internal account
    blob_client_instance = blob_service_client.get_blob_client(container=container_name, blob=file_name, snapshot=None)

    try:
        # delete existing
        blob_client_instance.delete_blob()
    except exceptions.ResourceNotFoundError:
        pass
