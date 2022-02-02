"""Integration Test ReportETLMethods"""
import os
import unittest
from io import BytesIO
from datetime import datetime, timedelta

import boto3
import pandas as pd

from app.common.s3 import S3BucketConnector
from app.common.constants import MetaProcessFormat
from app.transformers.report_transformer import ReportETL, SourceConfig, DestinationConfig

class IntTestReportETLMethods(unittest.TestCase):
    """
    Integration testing the ReportETL class.
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-west-2.amazonaws.com'
        self.s3_bucket_name_src = 'simple-int-test-src'
        self.s3_bucket_name_trg = 'simple-int-test-trg'
        self.meta_key = 'meta_file.csv'
        # Creating the source and target bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.src_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket = self.s3.Bucket(self.s3_bucket_name_trg)
        # Creating S3BucketConnector testing instances
        self.s3_bucket_src = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name_src)
        self.s3_bucket_trg = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name_trg)
        # Creating a list of dates
        self.dates = [(datetime.today().date() - timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]
        # Creating source and target configuration
        conf_dict_src = {
            'src_first_extract_date': '2022-01-30',
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
        self.target_config = DestinationConfig(**conf_dict_dst)
        # Creating source files on mocked s3
        columns_src = ['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice',
        'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
        data = [['AT0000A0E9W5', 'SANT', self.dates[5], '12:00', 20.19, 18.45, 18.20, 20.33, 877],
                ['AT0000A0E9W5', 'SANT', self.dates[4], '15:00', 18.27, 21.19, 18.27, 21.34, 987],
                ['AT0000A0E9W5', 'SANT', self.dates[3], '13:00', 20.21, 18.27, 18.21, 20.42, 633],
                ['AT0000A0E9W5', 'SANT', self.dates[3], '14:00', 18.27, 21.19, 18.27, 21.34, 455],
                ['AT0000A0E9W5', 'SANT', self.dates[2], '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
                ['AT0000A0E9W5', 'SANT', self.dates[2], '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
                ['AT0000A0E9W5', 'SANT', self.dates[1], '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
                ['AT0000A0E9W5', 'SANT', self.dates[1], '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
                ['AT0000A0E9W5', 'SANT', self.dates[1], '09:00', 24.22, 22.21, 22.21, 25.01, 1523]]
        self.df_src = pd.DataFrame(data, columns=columns_src)
        self.s3_bucket_src.to_s3(self.df_src.loc[0:0],
        f'{self.dates[5]}/{self.dates[5]}_BINS_XETR12.csv','csv')
        self.s3_bucket_src.to_s3(self.df_src.loc[1:1],
        f'{self.dates[4]}/{self.dates[4]}_BINS_XETR15.csv','csv')
        self.s3_bucket_src.to_s3(self.df_src.loc[2:2],
        f'{self.dates[3]}/{self.dates[3]}_BINS_XETR13.csv','csv')
        self.s3_bucket_src.to_s3(self.df_src.loc[3:3],
        f'{self.dates[3]}/{self.dates[3]}_BINS_XETR14.csv','csv')
        self.s3_bucket_src.to_s3(self.df_src.loc[4:4],
        f'{self.dates[2]}/{self.dates[2]}_BINS_XETR07.csv','csv')
        self.s3_bucket_src.to_s3(self.df_src.loc[5:5],
        f'{self.dates[2]}/{self.dates[2]}_BINS_XETR08.csv','csv')
        self.s3_bucket_src.to_s3(self.df_src.loc[6:6],
        f'{self.dates[1]}/{self.dates[1]}_BINS_XETR07.csv','csv')
        self.s3_bucket_src.to_s3(self.df_src.loc[7:7],
        f'{self.dates[1]}/{self.dates[1]}_BINS_XETR08.csv','csv')
        self.s3_bucket_src.to_s3(self.df_src.loc[8:8],
        f'{self.dates[1]}/{self.dates[1]}_BINS_XETR09.csv','csv')
        columns_report = ['ISIN', 'Date', 'opening_price_eur', 'closing_price_eur',
        'minimum_price_eur', 'maximum_price_eur', 'daily_traded_volume', 'change_prev_closing_%']
        data_report = [['AT0000A0E9W5', self.dates[3], 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
                       ['AT0000A0E9W5', self.dates[2], 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
                       ['AT0000A0E9W5', self.dates[1], 23.58, 24.22, 22.21, 25.01, 3586, 14.58]]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self):
        for key in self.src_bucket.objects.all():
            key.delete()
        for key in self.trg_bucket.objects.all():
            key.delete()

    def test_int_etl_report1_no_metafile(self):
        """
        Integration test for the etl_report1 method
        """
        # Expected results
        df_exp = self.df_report
        meta_exp = [self.dates[3], self.dates[2], self.dates[1], self.dates[0]]
        # Method execution
        report_etl = ReportETL(self.s3_bucket_src, self.s3_bucket_trg,
                             self.meta_key, self.source_config, self.target_config)
        report_etl.etl_report()
        # Test after method execution
        trg_file = self.s3_bucket_trg.list_files_by_prefix(self.target_config.dest_key)[0]
        df_result =  self.s3_bucket_trg.read_parquet(trg_file)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_bucket_trg.list_files_by_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)

if __name__ == '__main__':
    unittest.main()