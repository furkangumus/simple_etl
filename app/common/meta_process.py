""" Methods for processing the meta file """

import collections
from datetime import datetime, time, timedelta
import pandas as pd
from pandas._libs import missing
from app.common.constants import MetaProcessFormat
from app.common.custom_exceptions import WrongMetaFileException
from app.common.s3 import S3BucketConnector


class MetaProcess():
    """
    Class for working with the meta file
    """
    @staticmethod
    def update_meta_file(extracted_dates: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Updates the meta file with processed dates and todays date as processed date.

        Args:
            extracted_dates (list): a list of dates that are extracted from the source
            meta_key (str): name of the metafile in S3 Bucket
            s3_bucket_meta (S3BucketConnector): S3BucketConnector for bucket with the meta file
        """
        # Creating an empty dataframe using the meta file column names
        meta_columns = [MetaProcessFormat.META_SOURCE_DATE_COL.value,
                        MetaProcessFormat.META_PROCESS_COL.value]
        new_df = pd.DataFrame(columns=meta_columns)
        # Filling the date column with the extracted_dates
        new_df[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extracted_dates
        # Filling the processed column
        new_df[MetaProcessFormat.META_PROCESS_COL.value] = \
            datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # if meta file exists then union dataframes (old | new)
            old_df = s3_bucket_meta.read_csv(meta_key)
            if collections.Counter(old_df.columns) != collections.Counter(new_df.columns):
                raise WrongMetaFileException
            all_df = pd.concat([old_df, new_df])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists
            all_df = new_df
        # Writing to S3
        s3_bucket_meta.to_s3(all_df, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        
        return True

    @staticmethod
    def return_date_list(sdate: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Creates a list of dates based on the input sdate and the already
        processed dates in the meta file

        Args:
            sdate (str): the earliest date the data should be processed
            meta_key (str): name of the meta file on the S3 Bucket
            s3_bucket_meta (S3BucketConnector): S3BucketConnector for the bucket with the meta file
            
        Returns
            min_date (str): first date that should be processed
            return_date_list (list): list of all dates from min_date till today
        """
        start = datetime.strptime(sdate,
                                  MetaProcessFormat.META_DATE_FORMAT.value)\
                                      .date() - timedelta(days=1)
        today = datetime.today().date()
        try:
            # If meta file exists create return_date_list using the content of the meta file
            # Reading meta file
            meta_df = s3_bucket_meta.read_csv(meta_key)
            # Creating a list of dates from sdate until today
            dates = [start + timedelta(days=x) for x in range(0, (today-start).days + 1)]
            # Creating set of all dates in meta file
            src_dates = set(pd.to_datetime(
                meta_df[MetaProcessFormat.META_SOURCE_DATE_COL.value]
            ).dt.date)
            missing_dates = set(dates[1:]) - src_dates
            if missing_dates:
                # determine the earliest date that should be extracted
                min_date = min(missing_dates) - timedelta(days=1)
                # Create a list of dates from min_date to today
                return_min_date = (min_date + timedelta(days=1))\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_date_list = [
                    dt.strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                    for dt in dates if dt >= min_date
                ]
            else:
                # setting values for the earliest date and the list of dates
                return_date_list = []
                return_min_date = datetime(2200, 1, 1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # There is no existing meta file
            # creating a date list from sdate - 1 to today
            return_min_date = sdate
            return_date_list = [
                (start + timedelta(days=x))\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value) 
                for x in range(0, (today-start).days + 1)
            ]
            
        return return_min_date, return_date_list
