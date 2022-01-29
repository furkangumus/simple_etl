"""TestETLMethods"""
from io import BytesIO
import os
import unittest
from unittest import mock
from unittest.mock import patch
from datetime import datetime
import boto3
from numpy import extract
import pandas as pd
from moto import mock_s3

from app.common.s3 import S3BucketConnector
from app.common.meta_process import MetaProcess
from app.transformers.report_transformer import ReportETL, SourceConfig, DestinationConfig

class TestETLMethods(unittest.TestCase):
    """
    Testing the ReportETL class
    """

    def setUp(self) -> None:
        """
        Setting up the testing environment
        """
        # mocking s3 connection start
        self._mock_s3 = mock_s3()
        self._mock_s3.start()
        # defining class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-west-2.amazonaws.com'
        self.s3_bucket_name_src = 'src-bucket'
        self.s3_bucket_name_dst = 'dst-bucket'
        self.meta_key = 'meta_key'
        # Creating s3 access keys and environmental variables
        os.environ[self.s3_access_key] = 'ACCESS-KEY1'
        os.environ[self.s3_secret_key] = 'SECRET-KEY1'
        # Creating bucket on the mocked s3
        self._s3 = boto3.resource(service_name='s3', endpoint_url = self.s3_endpoint_url)
        self._s3.create_bucket(Bucket=self.s3_bucket_name_src,
                               CreateBucketConfiguration={
                                   'LocationConstraint': 'eu-west-2'
                               })
        self._s3.create_bucket(Bucket=self.s3_bucket_name_dst,
                               CreateBucketConfiguration={
                                   'LocationConstraint': 'eu-west-2'
                               })
        self._bucket_src = self._s3.Bucket(self.s3_bucket_name_src)
        self._bucket_dst = self._s3.Bucket(self.s3_bucket_name_dst)
        # Creating a testing instance
        self._bucket_conn_src = S3BucketConnector(self.s3_access_key,
                                                  self.s3_secret_key,
                                                  self.s3_endpoint_url,
                                                  self.s3_bucket_name_src)
        self._bucket_conn_dst = S3BucketConnector(self.s3_access_key,
                                                  self.s3_secret_key,
                                                  self.s3_endpoint_url,
                                                  self.s3_bucket_name_dst)
        # creating source and target configuration
        conf_dict_src = {
            'src_first_extract_date': '2021-12-01',
            'src_columns': ['ISIN', 'Mnemonic', 'Date', 'Time',
                            'StartPrice', 'EndPrice', 'MinPrice',
                            'MaxPrice', 'TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }

        conf_dict_dst = {
            'dest_col_isin': 'isin',
            'dest_col_date': 'date',
            'dest_col_op_price': 'opening_price_eur',
            'dest_col_cls_price': 'closing_price_eur',
            'dest_col_min_price': 'minimum_price_eur',
            'dest_col_max_price': 'maximum_price_eur',
            'dest_col_daily_trd_vol': 'daily_traded_volume',
            'dest_col_chg_prev_cls': 'change_prev_closing_%',
            'dest_key': 'report1/daily_report1_',
            'dest_key_date_format': '%Y%m%d_%H%M%S',
            'dest_format': 'parquet'
        }
        self.source_config = SourceConfig(**conf_dict_src)
        self.destination_config = DestinationConfig(**conf_dict_dst)
        # Creating source files on mocked s3
        columns_src = ['ISIN', 'Mnemonic', 'Date', 'Time',
                        'StartPrice', 'EndPrice', 'MinPrice',
                        'MaxPrice', 'TradedVolume']
        data = [
            ['AT0000A0E9W5', 'SANT', '2021-12-15', '12:00', 20.19, 18.45, 18.20, 20.33, 877],
            ['AT0000A0E9W5', 'SANT', '2021-12-16', '15:00', 18.27, 21.19, 18.27, 21.34, 987],
            ['AT0000A0E9W5', 'SANT', '2021-12-17', '13:00', 20.21, 18.27, 18.21, 20.42, 633],
            ['AT0000A0E9W5', 'SANT', '2021-12-17', '14:00', 18.27, 21.19, 18.27, 21.34, 455],
            ['AT0000A0E9W5', 'SANT', '2021-12-18', '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
            ['AT0000A0E9W5', 'SANT', '2021-12-18', '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
            ['AT0000A0E9W5', 'SANT', '2021-12-19', '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
            ['AT0000A0E9W5', 'SANT', '2021-12-19', '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
            ['AT0000A0E9W5', 'SANT', '2021-12-19', '09:00', 24.22, 22.21, 22.21, 25.01, 1523]
        ]
        self.src_df = pd.DataFrame(data, columns=columns_src)
        self._bucket_conn_src.to_s3(self.src_df.loc[0:0],
        '2021-12-15/2021-12-15_BINS_XETR12.csv','csv')
        self._bucket_conn_src.to_s3(self.src_df.loc[1:1],
        '2021-12-16/2021-12-16_BINS_XETR15.csv','csv')
        self._bucket_conn_src.to_s3(self.src_df.loc[2:2],
        '2021-12-17/2021-12-17_BINS_XETR13.csv','csv')
        self._bucket_conn_src.to_s3(self.src_df.loc[3:3],
        '2021-12-17/2021-12-17_BINS_XETR14.csv','csv')
        self._bucket_conn_src.to_s3(self.src_df.loc[4:4],
        '2021-12-18/2021-12-18_BINS_XETR07.csv','csv')
        self._bucket_conn_src.to_s3(self.src_df.loc[5:5],
        '2021-12-18/2021-12-18_BINS_XETR08.csv','csv')
        self._bucket_conn_src.to_s3(self.src_df.loc[6:6],
        '2021-12-19/2021-12-19_BINS_XETR07.csv','csv')
        self._bucket_conn_src.to_s3(self.src_df.loc[7:7],
        '2021-12-19/2021-12-19_BINS_XETR08.csv','csv')
        self._bucket_conn_src.to_s3(self.src_df.loc[8:8],
        '2021-12-19/2021-12-19_BINS_XETR09.csv','csv')
        columns_report = [
            'ISIN', 'Date', 'opening_price_eur', 'closing_price_eur',
            'minimum_price_eur', 'maximum_price_eur', 'daily_traded_volume', 
            'change_prev_closing_%'
        ]
        data_report = [
            ['AT0000A0E9W5', '2021-12-17', 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
            ['AT0000A0E9W5', '2021-12-18', 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
            ['AT0000A0E9W5', '2021-12-19', 23.58, 24.22, 22.21, 25.01, 3586, 14.58]
        ]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self) -> None:
        """Executing after unittests
        """
        # mocking s3 connection stop
        self._mock_s3.stop()

    def test_extract_no_files(self):
        """
        Tests the extract method when
        there are no files to be extracted
        """
        # Test init
        extract_date = '2200-01-02'
        extract_date_list = []
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            report_etl = ReportETL(self._bucket_conn_src, 
                                   self._bucket_conn_dst,
                                   self.meta_key, 
                                   self.source_config, 
                                   self.destination_config)
            resulted_df = report_etl.extract()
        # Test after method execution
        self.assertTrue(resulted_df.empty)
    
    def test_extract_files(self):
        """
        Tests the extract method when
        there are files to be extracted
        """
        # Expected results
        exp_df = self.src_df.loc[1:].reset_index(drop=True)
        # Test init
        extract_date = '2021-12-17'
        extract_date_list = ['2021-12-16', '2021-12-17', 
                             '2021-12-18', '2021-12-19', '2021-12-20']
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            report_etl = ReportETL(self._bucket_conn_src, 
                                   self._bucket_conn_dst,
                                   self.meta_key, 
                                   self.source_config, 
                                   self.destination_config)
            resulted_df = report_etl.extract()
        # Test after method execution
        self.assertTrue(exp_df.equals(resulted_df))

    def test_transform_report_emptydf(self):
        """
        Tests the transform_to_report method with
        an empty data frame as input argument
        """
        # Expected results
        exp_log = "The dataframe is empty. No transformations will be applied."
        # Test init.
        extract_date = '2021-12-09'
        extract_date_list = ['2021-12-08', '2021-12-09', '2021-12-10']
        input_df = pd.DataFrame()
        # Method Execution
        with patch.object(MetaProcess, 'return_date_list',
                          return_value=[extract_date, extract_date_list]):
            report_etl = ReportETL(self._bucket_conn_src, 
                                   self._bucket_conn_dst,
                                   self.meta_key, 
                                   self.source_config, 
                                   self.destination_config)
            with self.assertLogs() as logm:
                result_df = report_etl.transform_to_report(input_df)
                # Log test after method execution
                self.assertIn(exp_log, logm.output[0])
        # Test after method execution
        self.assertTrue(result_df.empty)

    def test_transform_report_ok(self):
        """
        Tests the transform_to_report method with
        a valid data frame as input argument
        """
        # Expected results
        exp_log1 = "Applying transformations to report source data for report 1 started..."
        exp_log2 = "Applying transformations to report source data finished..."
        exp_df = self.df_report
        # Test Init
        extract_date = '2021-12-17'
        extract_date_list = [
            '2021-12-16', '2021-12-17',
            '2021-12-18', '2021-12-19'
        ]
        input_df = self.src_df.loc[1:8].reset_index(drop=True)
        # Method Execution
        with patch.object(MetaProcess, 'return_date_list',
                        return_value=[extract_date, extract_date_list]):
            report_etl = ReportETL(self._bucket_conn_src, 
                                   self._bucket_conn_dst,
                                   self.meta_key, 
                                   self.source_config, 
                                   self.destination_config)
            with self.assertLogs() as logm:
                result_df = report_etl.transform_to_report(input_df)
                # Log test after method execution
                self.assertIn(exp_log1, logm.output[0])
                self.assertIn(exp_log2, logm.output[1])
        # Test after method execution
        self.assertTrue(exp_df.equals(result_df))

    def test_load(self):
        """
        Tests the load method
        """
        # Expected results
        exp_log1 = f'Report for <{datetime.today().strftime("%Y-%m-%d")}> successfully written.'
        exp_log2 = "Report meta file succesfully updated."
        exp_df = self.df_report
        exp_meta = [
            '2021-12-17', '2021-12-18', '2021-12-19'
        ]
        # Test init.
        extract_date = '2021-12-17'
        extract_date_list = [
            '2021-12-16', '2021-12-17',
            '2021-12-18', '2021-12-19'
        ]
        input_df = self.df_report
        # Method execution
        with patch.object(MetaProcess, 'return_date_list',
                        return_value=[extract_date, extract_date_list]):
            report_etl = ReportETL(self._bucket_conn_src, 
                                   self._bucket_conn_dst,
                                   self.meta_key, 
                                   self.source_config, 
                                   self.destination_config)
            with self.assertLogs() as logm:
                result_df = report_etl.load(input_df)
                # Log test after method execution
                print(logm.output)
                self.assertIn(exp_log1, logm.output[1])
                self.assertIn(exp_log2, logm.output[4])
        # test after method execution
        dest_file = self._bucket_conn_dst.list_files_by_prefix(self.destination_config.dest_key)[0]
        result_df = self._bucket_conn_dst.read_parquet(dest_file)
        self.assertTrue(exp_df.equals(result_df))
        meta_file = self._bucket_conn_dst.list_files_by_prefix(self.meta_key)[0]
        result_meta_df = self._bucket_conn_dst.read_csv(meta_file)
        self.assertEqual(list(result_meta_df['source_date']), exp_meta)
        
        # Cleanup after test
        self._bucket_dst.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': dest_file
                    },
                    {
                        'Key': meta_file
                    }
                ]
            }
        )

    def test_etl_report(self):
        """
        Tests the etl_report method
        """
        # Expected results
        exp_df = self.df_report
        exp_meta = [
            '2021-12-17', '2021-12-18', '2021-12-19'
        ]
        # Test init.
        extract_date = '2021-12-17'
        extract_date_list = [
            '2021-12-16', '2021-12-17',
            '2021-12-18', '2021-12-19'
        ]
        # Method execution
        with patch.object(MetaProcess, 'return_date_list',
                        return_value=[extract_date, extract_date_list]):
            report_etl = ReportETL(self._bucket_conn_src, 
                                   self._bucket_conn_dst,
                                   self.meta_key, 
                                   self.source_config, 
                                   self.destination_config)
            report_etl.etl_report()
        # test after method execution
        dest_file = self._bucket_conn_dst.list_files_by_prefix(self.destination_config.dest_key)[0]
        result_df = self._bucket_conn_dst.read_parquet(dest_file)
        self.assertTrue(exp_df.equals(result_df))
        meta_file = self._bucket_conn_dst.list_files_by_prefix(self.meta_key)[0]
        result_meta_df = self._bucket_conn_dst.read_csv(meta_file)
        self.assertEqual(list(result_meta_df['source_date']), exp_meta)
        
        # Cleanup after test
        self._bucket_dst.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': dest_file
                    },
                    {
                        'Key': meta_file
                    }
                ]
            }
        )

    
if __name__ == '__main__':
    unittest.main()