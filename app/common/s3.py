""" Connector and methods accessing S3 """
import os
import logging
from io import BytesIO, StringIO
import boto3

import pandas as pd

from app.common.constants import S3FileTypes
from app.common.custom_exceptions import WrongFormatException


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
    
    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for putting objects to S3 Bucket

        Args:
            out_buffer (StringIO or BytesIO): Buffer that should be written
            key (str): name of the file
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)

        return True

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
    
    def read_csv(self, key: str, encoding: str="utf-8", sep: str=","):
        """
        Reads a csv file from S3 Bucket and returns a dataframe

        Args:
            key (str): key of the file that should be read
            encoding (str, optional): encoding of the data inside the file. Defaults to "utf-8".
            sep (str, optional): seperator of the csv. Defaults to ",".

        Returns:
            [pandas.DataFrame]: Pandas DataFrame that contains the data of the csv file
        """
        self._logger.info(f'Reading file {self.endpoint_url}/{self._bucket.name}/{key}')
        csv_obj = self._bucket.Object(key=key).get().get("Body").read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, delimiter=sep)

        return data_frame
    
    def read_parquet(self, key: str):
        """
        Reads a parquet file from S3 Bucket and returns a dataframe

        Args:
            key (str): key of the file that should be read

        Returns:
            [pandas.DataFrame]: Pandas DataFrame that contains the data of the parquet file
        """
        self._logger.info(f'Reading file {self.endpoint_url}/{self._bucket.name}/{key}')
        prq_obj = self._bucket.Object(key=key).get().get("Body").read()
        data = BytesIO(prq_obj)
        data_frame = pd.read_parquet(data)

        return data_frame
    
    def to_s3(self, df: pd.DataFrame, key: str, file_format: str):
        """
        Writes pandas.DataFrame to S3 Bucket in given(csv|parquet) format

        Args:
            df (pd.DataFrame): pandas DataFrame that needs to be written
            key (str): target name of the file
            file_format (str): target file format (csv|parquet)
        """
        if df.empty:
            self._logger.info('The DataFrame is empty! No file will be written.')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info("The file format %s is not "
                          "supported to be written to S3!", file_format)
        raise WrongFormatException
