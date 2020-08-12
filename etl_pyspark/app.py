import logging

from etl_pyspark.util.sparker import get_spark_session, execute_report
from etl_pyspark.util import init_logger, load_queries


def run_job(report_name, batch_month):
    init_logger()

    logging.info(f'Initializing Spark Session for {report_name}')
    spark = get_spark_session(report_name=report_name)
    logging.info(f'Opening reports file for {report_name}')
    reports = load_queries()
    execute_report(spark, reports, report_name, batch_month)
