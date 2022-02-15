""" Runs the Report ETL application """
import argparse
import logging
import logging.config

import yaml

from app.common.s3 import S3BucketConnector
from app.transformers.report_transformer import ReportETL, SourceConfig, DestinationConfig

def main():
    """
    Entry point to run report ETL job.
    """
    arg_parser = argparse.ArgumentParser(description="Run the Report ETL job.")
    arg_parser.add_argument('config', help='a configuration file in YAML format.')
    
    args = arg_parser.parse_args()
    # Parsing YAML
    config = yaml.safe_load(open(args.config))
    
    # Configure Logging
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector class instances
    # for source and destination
    src_s3_connector = S3BucketConnector(
        access_key=s3_config['access_key'],
        secret_key=s3_config['secret_key'],
        endpoint_url=s3_config['src_endpoint_url'],
        bucket=s3_config['src_bucket']
    )
    dest_s3_connector = S3BucketConnector(
        access_key=s3_config['access_key'],
        secret_key=s3_config['secret_key'],
        endpoint_url=s3_config['dest_endpoint_url'],
        bucket=s3_config['dest_bucket']
    )
    # reading source configuration
    source_config = SourceConfig(**config['source'])
    # reading destination configuration
    destination_config = DestinationConfig(**config['destination'])
    # reading meta configuration
    meta_config = config['meta']
    # creating ReportETL class instance
    logger.info('Report ETL job started.')
    report_etl = ReportETL(
        src_bucket=src_s3_connector,
        dest_bucket=dest_s3_connector,
        meta_key=meta_config['meta_key'],
        src_args=source_config,
        dest_args=destination_config
    )
    # running etl job
    report_etl.etl_report()
    logger.info('Report ETL job finished.')
    

if __name__ == "__main__":
    main()
