""" Connector and methods accessing S3 """
import os
import logging
from io import BytesIO, StringIO
import pandas_gbq

import pandas as pd

from app.common.constants import S3FileTypes
from app.common.custom_exceptions import WrongFormatException


class BigQueryConnector():
    """
    Class for interacting with bigquery
    """
    def __init__(self, project_id: str, dataset_name: str, table_name: str) -> None:
        """
        Constructor for S3BucketConnector

        Args:
            access_key (str): key for accessing S3
            secret_key (str): secret key for accessing S3
            endpoint_url (str): endpoint url to S3 API
            bucket (str): name of the S3 bucket
        """
        self._logger = logging.getLogger(__name__)
        self.table_id = f"{dataset_name}.{table_name}"
        self.project_id = project_id


    def to_bq(self, data: pd.DataFrame):
        """
        Writes pandas.DataFrame to Bigquery

        Args:
            data (pd.DataFrame): pandas DataFrame that needs to be written)
        """
        if data.empty:
            self._logger.info('The DataFrame is empty! No file will be written.')
            return None
        else:
            out_buffer = StringIO()
            data.to_csv(out_buffer, index=False)
            pandas_gbq.to_gbq(data, self.table_id, project_id=self.project_id, if_exists='append')
            return 1
        self._logger.info("The file format %s is not "
                          "supported to be written to S3!", file_format)
        raise WrongFormatException
