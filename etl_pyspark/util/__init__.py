import logging

from etl_pyspark.resources import reports


def load_queries():
    return reports.report_queries


def init_logger():
    log_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(filename)s %(funcName)s line:%(lineno)d  %(message)s',
        '[%Y-%m-%d %H:%M:%S]'
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)