import logging
from logging.handlers import TimedRotatingFileHandler


def init_logger(base_dir):
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(funcName)s line:%(lineno)d  %(message)s',
                                     '[%Y-%m-%d %H:%M:%S]')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    file_handler = TimedRotatingFileHandler(f'{base_dir}/logs/etl-pyspark.log',
                                           when="d",
                                           interval=1,
                                           backupCount=5)

    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)
