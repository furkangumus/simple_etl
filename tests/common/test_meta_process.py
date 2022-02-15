"""MetaFileProcess Tests
"""
from datetime import date, datetime, time, timedelta
from io import StringIO
import os
import unittest

import boto3
from moto import mock_s3
import pandas as pd

from app.common.custom_exceptions import WrongMetaFileException
from app.common.constants import MetaProcessFormat
from app.common.meta_process import MetaProcess
from app.common.s3 import S3BucketConnector

class TestMetaProcessMethods(unittest.TestCase):
    """
    Testing the MetaProcess class
    """

    def setUp(self) -> None:
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self._mock_s3 = mock_s3()
        self._mock_s3.start()
        # defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-west-2.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating s3 access keys and environmental variables
        os.environ[self.s3_access_key] = 'ACCESS-KEY1'
        os.environ[self.s3_secret_key] = 'SECRET-KEY1'
        # Creating bucket on the mocked s3
        self._s3 = boto3.resource(service_name='s3', endpoint_url = self.s3_endpoint_url)
        self._s3.create_bucket(Bucket=self.s3_bucket_name,
                               CreateBucketConfiguration={
                                   'LocationConstraint': 'eu-west-2'
                               })
        self._bucket_meta = self._s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self._bucket_conn_meta = S3BucketConnector(self.s3_access_key,
                                                   self.s3_secret_key,
                                                   self.s3_endpoint_url,
                                                   self.s3_bucket_name)
        self.dates = [(datetime.today().date()-timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]

    def tearDown(self) -> None:
        # mocking s3 connection stop
        self._mock_s3.stop()

    def test_update_meta_file_new_meta_file(self):
        """
        Tests the update_meta_file method
        when there is no meta file which means the very first execution.
        """
        # Expected results
        exp_date_list = ['2021-12-10', '2021-12-06']
        exp_proc_date_list = [datetime.today().date()]*2
        # Test Init.
        meta_key = 'meta.csv'
        # Method execution
        MetaProcess.update_meta_file(exp_date_list, meta_key, self._bucket_conn_meta)
        # Read meta file
        data = self._bucket_meta.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        meta_df = pd.read_csv(out_buffer)
        act_date_list = list(meta_df[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        act_proc_date_list = list(
            pd.to_datetime(meta_df[MetaProcessFormat.META_PROCESS_COL.value]).dt.date)
        # Test after method execution
        self.assertEqual(exp_date_list, act_date_list)
        self.assertEqual(exp_proc_date_list, act_proc_date_list)
        # Cleanup after test
        self._bucket_meta.delete_objects(
            Delete={
                'Objects':[
                    {'Key': meta_key}
                ]
            }
        )

    def test_update_meta_file_empty_date_list(self):
        """
        Tests the update_meta_file method
        when the argument extract_dates is empty
        """
        # Expected results
        exp_result = True
        exp_log = 'The DataFrame is empty! No file will be written.'
        # Test Init.
        date_list = []
        meta_key = 'meta.csv'
        # Method Execution
        with self.assertLogs() as logm:
            result = MetaProcess.update_meta_file(date_list, meta_key, self._bucket_conn_meta)
            # Log test after method execution
            self.assertIn(exp_log, logm.output[1])
        # Test after method execution
        self.assertEqual(exp_result, result)

    def test_update_meta_file_ok(self):
        """
        Tests the update_meta_file method
        when there is an existing meta file
        """
        # Expected results
        old_date_list = ['2021-12-10', '2021-12-11']
        new_date_list = ['2022-01-01', '2022-01-02']
        exp_date_list = old_date_list + new_date_list
        old_process_date = (datetime.today()-timedelta(days=14))\
            .strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        new_process_date = datetime.today()\
            .strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        exp_proc_date_list = [old_process_date] * 2 + [new_process_date] * 2
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{old_date_list[0]},'
            f'{old_process_date}\n'
            f'{old_date_list[1]},'
            f'{old_process_date}\n'
        )
        self._bucket_meta.put_object(Body=meta_content, Key=meta_key)
        # Method Execution
        MetaProcess.update_meta_file(new_date_list, meta_key, self._bucket_conn_meta)
        # Read meta_file
        data = self._bucket_meta.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        meta_df = pd.read_csv(out_buffer)
        act_date_list = list(meta_df[
            MetaProcessFormat.META_SOURCE_DATE_COL.value
        ])
        act_proc_date_list = list(meta_df[
            MetaProcessFormat.META_PROCESS_COL.value
        ])
        # Test after method execution
        self.assertEqual(exp_date_list, act_date_list)
        self.assertEqual(exp_proc_date_list, act_proc_date_list)
        # Cleanup after test
        self._bucket_meta.delete_objects(
            Delete={
                'Objects':[
                    {'Key': meta_key}
                ]
            }
        )

    def test_update_meta_file_wrong(self):
        """
        Tests the update_meta_file method
        when there is an existing meta file(wrong)
        """
        # Expected results
        old_date_list = ['2021-12-10', '2021-12-11']
        new_date_list = ['2022-01-01', '2022-01-02']
        old_process_date = (datetime.today()-timedelta(days=14))\
            .strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value}ss,'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{old_date_list[0]},'
            f'{old_process_date}\n'
            f'{old_date_list[1]},'
            f'{old_process_date}\n'
        )
        self._bucket_meta.put_object(Body=meta_content, Key=meta_key)
        # Method Execution
        with self.assertRaises(WrongMetaFileException):
            MetaProcess.update_meta_file(new_date_list, meta_key, self._bucket_conn_meta)
        # Cleanup after test
        self._bucket_meta.delete_objects(
            Delete={
                'Objects':[
                    {'Key': meta_key}
                ]
            }
        )

    def test_return_date_list_no_meta_file(self):
        """
        Tests the return_date_list method
        when there is no meta file
        """
        # Expected results
        exp_date_list = [
            (datetime.today().date() - timedelta(days=day))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(4)
        ]
        exp_min_date = (datetime.today().date() - timedelta(days=2))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        # Test init
        sdate = exp_min_date
        meta_key = 'meta.csv'
        # Method Execution
        act_min_date, act_date_list = MetaProcess.return_date_list(sdate, meta_key,
                                                                   self._bucket_conn_meta)
        # Test after method execution
        self.assertEqual(set(exp_date_list), set(act_date_list))
        self.assertEqual(exp_min_date, act_min_date)

    def test_return_date_list_ok(self):
        """
        Tests the return_date_list method
        when there is a valid meta file
        """
        # Expected results
        today = datetime.today().date()
        date_diff = lambda day: today - timedelta(days=day)
        exp_min_date = [
            date_diff(1).strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            date_diff(2).strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            date_diff(7).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        ]
        exp_date_list = [
            [
                date_diff(day).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(3)
            ],
            [
                date_diff(day).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(4)
            ],
            [
                date_diff(day).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(9)
            ],
        ]
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[3]},{self.dates[0]}\n'
            f'{self.dates[4]},{self.dates[0]}\n'
        )
        self._bucket_meta.put_object(Body=meta_content, Key=meta_key)
        sdate_list = [
            self.dates[1],
            self.dates[4],
            self.dates[7]
        ]
        # Method Execution
        for ind, sdate in enumerate(sdate_list):
            act_min_date, act_date_list = MetaProcess\
                .return_date_list(sdate, meta_key, self._bucket_conn_meta)
            # Test after method execution
            self.assertEqual(set(exp_date_list[ind]), set(act_date_list))
            self.assertEqual(exp_min_date[ind], act_min_date)

        # Cleanup after test
        self._bucket_meta.delete_objects(
            Delete={
                'Objects':[
                    {'Key': meta_key}
                ]
            }
        )

    def test_return_date_list_wrong(self):
        """
        Tests the return_date_list method
        when there is an invalid meta file
        """
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value}ss,'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[3]},{self.dates[0]}\n'
            f'{self.dates[4]},{self.dates[0]}\n'
        )
        self._bucket_meta.put_object(Body=meta_content, Key=meta_key)
        sdate = self.dates[1]
        # Method Execution
        with self.assertRaises(KeyError):
            act_min_date, act_date_list = MetaProcess\
                .return_date_list(sdate, meta_key, self._bucket_conn_meta)
        # Cleanup after test
        self._bucket_meta.delete_objects(
            Delete={
                'Objects':[
                    {'Key': meta_key}
                ]
            }
        )

    def test_return_date_list_empty(self):
        """
        Tests the return_date_list method
        when there are no dates to be returned
        """
        # Expected results
        exp_min_date = '2200-01-01'
        exp_date_list = []
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[0]},{self.dates[0]}\n'
            f'{self.dates[1]},{self.dates[0]}\n'
        )
        self._bucket_meta.put_object(Body=meta_content, Key=meta_key)
        sdate = self.dates[0]
        # Method Execution
        act_min_date, act_date_list = MetaProcess\
                .return_date_list(sdate, meta_key, self._bucket_conn_meta)
        # Test after method execution
        self.assertEqual(set(exp_date_list), set(act_date_list))
        self.assertEqual(exp_min_date, act_min_date)
        # Cleanup after test
        self._bucket_meta.delete_objects(
            Delete={
                'Objects':[
                    {'Key': meta_key}
                ]
            }
        )    

if __name__ == '__main__':
    unittest.main()
