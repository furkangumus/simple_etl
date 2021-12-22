""" Runs the Report ETL application """
import logging
import logging.config

import yaml

def main():
    """
    Entry point to run report ETL job.
    """
    # Parsing YAML
    config_path = "/home/furkan/Desktop/projects/simple_etl/simple_etl/configs/report_config.yaml"
    config = yaml.safe_load(open(config_path))
    
    # Configure Logging
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")
    

if __name__ == "__main__":
    main()
