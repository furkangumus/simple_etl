""" Report ETL Component """
import logging

from typing import NamedTuple
from app.common.s3 import S3BucketConnector

class SourceConfig(NamedTuple):
    """Class for source configuration data

    Args:
        src_first_extract_date (str): determines the date for extarcting from the source
        src_columns (list): list of column names of the source
        src_col_date (list): column name for date in source
        src_col_isin (str): column name for isin in source
        src_col_time (str): column name for time in source
        src_col_start_price (str): column name for starting price in source
        src_col_min_price (str): column name for minimum price in source
        src_col_max_price (str): column name for maximum price in source
        src_col_traced_vol (str): column name for traded volumne in source
    """
    
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traced_vol: str
    
    
class DestinationConfig(NamedTuple):
    """Class for destination/target configuration data

    Args:
        dest_col_isin (str): column name for isin in destination/target
        dest_col_date (str): column name for date in destination/target
        dest_col_op_price (str): column name for opening price in destination/target
        dest_col_cls_price (str): column name for closing price in destination/target
        dest_col_min_price (str): column name for minimum price in destination/target
        dest_col_max_price (str): column_name for maximum price in destination/target
        dest_col_daily_trd_vol (str): column name for daily traded volume in destination/target
        dest_col_chg_prev_cls (str): column name for change to previous day's closing price in destination/target
        dest_key (str): basic key of destination/target file
        dest_key_date_format (str): date format of destination/target file key
        dest_format (str): file format of the destination/taarget file
    """
    dest_col_isin: str
    dest_col_date: str
    dest_col_op_price: str
    dest_col_cls_price: str
    dest_col_min_price: str
    dest_col_max_price: str
    dest_col_daily_trd_vol: str
    dest_col_chg_prev_cls: str
    dest_key: str
    dest_key_date_format: str
    dest_format: str
    
class ReportETL():
    """
    Reads the Xetra data, transforms and writes the transformed data to destination in parquet format.
    """
    def __init__(self, src_bucket: S3BucketConnector,
                 dest_bucket: S3BucketConnector, meta_key: str,
                 src_args: SourceConfig, dest_args: DestinationConfig) -> None:
        """
        Constructor for ReportETL

        Args:
            src_bucket (S3BucketConnector): connection to source S3 Bucket
            dest_bucket (S3BucketConnector): connection to destination/target S3 Bucket
            meta_key (str): key of meta file
            src_args (SourceConfig): NamedTuple class with source configuration data
            dest_args (DestinationConfig): NamedTuple class with destination/target configuration data
        """
        self._logger = logging.getLogger(__name__)
        
        self.src_bucket = src_bucket
        self.dest_bucket = dest_bucket
        self.meta_key = meta_key
        self.src_args = src_args
        self.dest_args = dest_args
        
        self.extract_date = None
        self.extract_date_list = None
        self.meta_update_list = None
        
        def extract(self):
            pass
        
        def transform_to_report(self):
            pass
        
        def load(self):
            pass
        
        def etl_report(self):
            pass
    