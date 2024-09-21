from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv, find_dotenv
import os


_ = load_dotenv(find_dotenv())


class BlobStorageOperations:
    def __init__(self):
        # Define your connection string, container name, and blob name
        self.account_url = os.environ.get('AZ_ACCT_URL')
        self.container_name = "plain-text"
        self.download_file_dir = "data"
        self.account_key = os.environ["STORAGE_ACCT_KEY"]

    def download_blob(self, blob_name: str = None):
        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient(self.account_url, credential=self.account_key)

        # Get the ContainerClient
        container_client = blob_service_client.get_container_client(self.container_name)

        # Get the BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the blob to a local file
        with open(os.path.join(self.download_file_dir, blob_name), "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        print(f"Downloaded blob '{blob_name}' to '{self.download_file_dir}'")
