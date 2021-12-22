""" Connector and methods accessing S3 """
import os
import logging

import boto3


class S3BucketConnector():
    """
    Class for interacting with S3 Buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str) -> None:
        """
        Constructor for S3BucketConnector

        Args:
            access_key (str): key for accessing S3
            secret_key (str): secret key for accessing S3
            endpoint_url (str): endpoint url to S3 API
            bucket (str): name of the S3 bucket
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name="s3", endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)
    def list_files_by_prefix(self, prefix: str) -> list:
        """
        Lists all objects in the S3 bucket with a prefix

        Args:
            prefix (str): prefix on the S3 bucket that should be filtered with

        Returns:
            file_list: list of all file names containing the prefix in the key
        """
        file_list = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return file_list
    def read_csv(self):
        """[summary]
        """
    def read_parquet(self):
        """[summary]
        """
    def csv_to_s3(self):
        """[summary]
        """
    def parquet_to_s3(self):
        """[summary]
        """
    