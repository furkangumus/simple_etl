""" TestS3BucketConnectorMethods """
from io import BytesIO, StringIO
import os
import unittest

import boto3
from moto import mock_s3
import pandas as pd
from app.common.custom_exceptions import WrongFormatException

from app.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """Testing the S3BucketConnector class
    """
    def setUp(self) -> None:
        """Setting up the environment
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
        self._bucket = self._s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self._bucket_conn = S3BucketConnector(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name)

    def tearDown(self) -> None:
        """Executing after unittests
        """
        # mocking s3 connection stop
        self._mock_s3.stop()
    
    def test_list_files_by_prefix_ok(self):
        """Test the list_files_by_prefix method for getting 2 objects
        as list on the mocked s3 bucket
        """
        # Expected Results
        exp_prefix = 'prefix/'
        exp_key1 = f'{exp_prefix}test1.csv'
        exp_key2 = f'{exp_prefix}test2.csv'
        # Test Init
        csv_content = """col1,col2
        valA,valB
        """
        self._bucket.put_object(Body=csv_content, Key=exp_key1)
        self._bucket.put_object(Body=csv_content, Key=exp_key2)
        # Method Execution
        result_list = self._bucket_conn.list_files_by_prefix(prefix=exp_prefix)
        # Tests after method execution
        self.assertEqual(len(result_list), 2)
        self.assertIn(exp_key1, result_list)
        self.assertIn(exp_key2, result_list)
        # Cleanup after tests
        self._bucket.delete_objects(
            Delete={
                'Objects':[
                    {'Key': exp_key1},
                    {'Key': exp_key2},
                ]
            }
        )
    
    def test_list_files_by_prefix_wrong(self):
        """Test the list_files_by_prefix method in case of a wrong or not existing prefix
        """
        # Expected Results
        exp_prefix = 'no-prefix/'
        # Method Execution
        result_list = self._bucket_conn.list_files_by_prefix(prefix=exp_prefix)
        # Tests after method execution
        self.assertTrue(not result_list)
    
    def test_read_csv_ok(self):
        """
        Tests the read_csv method for
        reading an .csv file from the mocked s3 bucket
        """
        # Expected Results
        exp_key = "test.csv"
        exp_col1 = 'col1'
        exp_col2 = 'col2'
        exp_val1 = 'val1'
        exp_val2 = 'val2'
        exp_log = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{exp_key}'
        # Test Init.
        csv_content = f'{exp_col1},{exp_col2}\n{exp_val1},{exp_val2}'
        self._bucket.put_object(Body=csv_content, Key=exp_key)
        # Method Execution
        with self.assertLogs() as logm:
            result_df = self._bucket_conn.read_csv(exp_key)
            # Log test after method execution
            self.assertIn(exp_log, logm.output[0])
        # Test after methor execution
        self.assertEqual(result_df.shape[0], 1)
        self.assertEqual(result_df.shape[1], 2)
        self.assertEqual(result_df[exp_col1][0], exp_val1)
        self.assertEqual(result_df[exp_col2][0], exp_val2)
        # Cleanup after test
        self._bucket.delete_objects(
            Delete={
                'Objects':[
                    { 'Key': exp_key }
                ]
            }
        )

    def test_to_s3_empty(self):
        """
        Tests the to_s3() method with an empty
        DataFrame as input.
        """
        # Expected results
        exp_return = None
        exp_log = 'The DataFrame is empty! No file will be written.'
        # Test init.
        empty_df = pd.DataFrame()
        key = 'empty.csv'
        file_format = 'csv'
        # Method Execution
        with self.assertLogs() as logm:
            result = self._bucket_conn.to_s3(empty_df, key, file_format)
            # log test after method execution
            self.assertIn(exp_log, logm.output[0])
        # Test after method execution
        self.assertEqual(exp_return, result)
        
    def test_to_s3_csv(self):
        """
        Tests the to_s3() method
        if writing csv is successful
        """
        # Expected Results
        exp_result = True
        exp_df = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        exp_key = 'test.csv'
        exp_log = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{exp_key}'
        # Test Init.
        file_format = 'csv'
        # Method execution
        with self.assertLogs() as logm:
            result = self._bucket_conn.to_s3(exp_df, exp_key, file_format)
            # Log test after method execution
            self.assertIn(exp_log, logm.output[0])
        # Test after method execution
        data = self._bucket.Object(key=exp_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        result_df = pd.read_csv(out_buffer)
        self.assertEqual(exp_result, result)
        self.assertTrue(exp_df.equals(result_df))
        # cleanup after test execution
        self._bucket.delete_objects(
            Delete={
                'Objects':[
                    {'Key':exp_key}
                ]
            }
        )
    
    def test_to_s3_parquet(self):
        """
        Tests the to_s3() method
        if writing parquet is successful
        """
        # Expected Results
        exp_result = True
        exp_df = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        exp_key = 'test.parquet'
        exp_log = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{exp_key}'
        # Test Init.
        file_format = 'parquet'
        # Method execution
        with self.assertLogs() as logm:
            result = self._bucket_conn.to_s3(exp_df, exp_key, file_format)
            # Log test after method execution
            self.assertIn(exp_log, logm.output[0])
        # Test after method execution
        data = self._bucket.Object(key=exp_key).get().get('Body').read()
        out_buffer = BytesIO(data)
        result_df = pd.read_parquet(out_buffer)
        self.assertEqual(exp_result, result)
        self.assertTrue(exp_df.equals(result_df))
        # cleanup after test execution
        self._bucket.delete_objects(
            Delete={
                'Objects':[
                    {'Key':exp_key}
                ]
            }
        )
        
    def test_to_s3_wrong_format(self):
        """
        Tests the to_s3() method
        if writing parquet is successful
        """
        # Expected Results
        exp_result = True
        exp_df = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        exp_key = 'test.parquet'
        exp_file_format = 'json' # json is not a supported format
        exp_log = f'The file format {exp_file_format} is not supported to be written'
        exp_exception = WrongFormatException
        # Test Init.
        
        # Method execution
        with self.assertLogs() as logm:
            with self.assertRaises(exp_exception):
                self._bucket_conn.to_s3(exp_df, exp_key, exp_file_format)
            # Log test after method execution
            self.assertIn(exp_log, logm.output[0])

if __name__ == "__main__":
    unittest.main()
    