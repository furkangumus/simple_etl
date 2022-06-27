""" Report ETL Component """
from datetime import datetime
import logging
from typing import NamedTuple

import pandas as pd

from app.common.meta_process import MetaProcess
from app.common.s3 import S3BucketConnector
from app.common.bq import BigQueryConnector

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
        src_col_traded_vol (str): column name for traded volumne in source
    """

    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


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
        dest_col_chg_prev_cls (str): column name for change to previous day's
                                     closing price in destination/target
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
    Reads the Xetra data, transforms and writes the transformed data
    to destination in parquet format.
    """
    def __init__(self, src_bucket: S3BucketConnector,
                 dest_bucket: S3BucketConnector=None, meta_key: str=None,
                 src_args: SourceConfig=None, dest_args: DestinationConfig=None) -> None:
        """
        Constructor for ReportETL

        Args:
            src_bucket (S3BucketConnector): connection to source S3 Bucket
            dest_bucket (S3BucketConnector): connection to destination/target S3 Bucket
            meta_key (str): key of meta file
            src_args (SourceConfig): NamedTuple class with source configuration data
            dest_args (DestinationConfig): NamedTuple class with destination/target
                                        configuration data
        """
        self._logger = logging.getLogger(__name__)

        self.src_bucket = src_bucket
        self.dest_bucket = dest_bucket
        self.meta_key = meta_key
        self.src_args = src_args
        self.dest_args = dest_args
        self.bq_conn = BigQueryConnector(project_id='circular-unity-dl18405', dataset_name='project2', table_name='stock_market')

        self.extract_date, self.extract_date_list = MetaProcess\
            .return_date_list(
                self.src_args.src_first_extract_date,
                self.meta_key,
                self.dest_bucket
            )
        self.meta_update_list = [
            d 
            for d in self.extract_date_list
            if d >= self.extract_date
        ]

    def extract(self):
        """
        Reads the source data and concatenates them to one Pandas DataFrame

        Returns:
            df: Pandas.DataFrame with the extracted data.
        """
        self._logger.info("Extracting source files started...")
        files = [
            object_name
            for dt in self.extract_date_list
            for object_name in self.src_bucket.list_files_by_prefix(dt)
        ]
        if not files:
            df = pd.DataFrame()
        else:
            df = pd.concat(
                [
                    self.src_bucket.read_csv(object_name)
                    for object_name in files
                ],
                ignore_index=True
            )
        self._logger.info("Extracting source files finished...")
        return df

    def transform_to_report(self, df: pd.DataFrame):
        """
        Applies the necessary transformations to create desired report

        Args:
            df (pd.DataFrame): Data that will be used to create report

        Returns:
            df: transformed data (report)
        """
        if df.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return df
        self._logger.info('Applying transformations to report source data for report 1 started...')
        # Filtering necessary source columns
        df = df.loc[:, self.src_args.src_columns]
        # Removing rows with missing values
        df.dropna(inplace=True)
        # Calculating opening price per ISIN and day
        df[self.dest_args.dest_col_op_price] = df\
            .sort_values(by=[self.src_args.src_col_time])\
                .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price]\
                    .transform('first')
        # Calculating closing price per ISIN and day
        df[self.dest_args.dest_col_cls_price] = df\
            .sort_values(by=[self.src_args.src_col_time])\
                .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price]\
                        .transform('last')
        # Renaming columns
        df.rename(columns={
            self.src_args.src_col_min_price: self.dest_args.dest_col_min_price,
            self.src_args.src_col_max_price: self.dest_args.dest_col_max_price,
            self.src_args.src_col_traded_vol: self.dest_args.dest_col_daily_trd_vol
            }, inplace=True)
        # Aggregating per ISIN and day -> opening price, closing price,
        # minimum price, maximum price, traded volume
        df = df.groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date], as_index=False)\
                .agg({
                    self.dest_args.dest_col_op_price: 'min',
                    self.dest_args.dest_col_cls_price: 'min',
                    self.dest_args.dest_col_min_price: 'min',
                    self.dest_args.dest_col_max_price: 'max',
                    self.dest_args.dest_col_daily_trd_vol: 'sum'})
        # Change of current day's closing price compared to the
        # previous trading day's closing price in %
        df[self.dest_args.dest_col_chg_prev_cls] = df\
            .sort_values(by=[self.src_args.src_col_date])\
                .groupby([self.src_args.src_col_isin])[self.dest_args.dest_col_op_price]\
                    .shift(1)
        df[self.dest_args.dest_col_chg_prev_cls] = (
            df[self.dest_args.dest_col_op_price] \
            - df[self.dest_args.dest_col_chg_prev_cls]
            ) / df[self.dest_args.dest_col_chg_prev_cls ] * 100
        # Rounding to 2 decimals
        df = df.round(decimals=2)
        # Removing the day before extract_date
        df = df[df.Date >= self.extract_date].reset_index(drop=True)
        self._logger.info('Applying transformations to report source data finished...')
        return df

    def load(self, df: pd.DataFrame):
        """
        Saves a Pandas DataFrame to the target system

        Args:
            df (pd.DataFrame): Pandas DataFrame to be written
        """
        # Creating target key
        target_key = (
            f'{self.dest_args.dest_key}'
            f'{datetime.today().strftime(self.dest_args.dest_key_date_format)}.'
            f'{self.dest_args.dest_format}'
        )
        # Write to the destination
        #self.dest_bucket.to_s3(df, target_key, self.dest_args.dest_format)
        self.bq_conn.to_bq(df)
        self._logger.info('Report for <%s> successfully written.', 
                          datetime.today().strftime('%Y-%m-%d'))
        # update metafile
        #MetaProcess.update_meta_file(self.meta_update_list, self.meta_key, self.dest_bucket)
        self._logger.info('Report meta file succesfully updated.')
        
        return True
        

    def etl_report(self):
        """
        Manage the ETL process to create report
        """
        # Extract
        df = self.extract()
        # Transform
        df = self.transform_to_report(df)
        # Load
        self.load(df)
        
        return True
    
