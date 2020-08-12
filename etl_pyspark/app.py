import logging
import json

from etl_pyspark.sparker import get_spark_session, execute_report


def load_queries():
    with open("etl_pyspark/resources/reports.json") as reports_file:
        reports = json.load(reports_file)
    return reports


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


def run_job(report_name, batch_month):
    init_logger()

    logging.info(f'Initializing Spark Session for {report_name}')
    spark = get_spark_session(report_name=report_name)
    logging.info(f'Opening reports file for {report_name}')
    reports = load_queries()
    execute_report(spark, reports, report_name, batch_month)
