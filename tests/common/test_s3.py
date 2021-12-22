""" TestS3BucketConnectorMethods """
import os
import unittest

import boto3
from moto import mock_s3

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
if __name__ == "__main__":
    unittest.main()
    