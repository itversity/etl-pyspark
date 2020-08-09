import json, sys, logging

from util import init_logger
from util.sparker import get_spark_session, execute_report


def main():
    if os.path.exists('jobs.zip'):
        sys.path.insert(0, 'etl-pyspark.zip')
    else:
        sys.path.insert(0, './jobs')
    init_logger('.')
    report_name = sys.argv[1]
    batch_month = int(sys.argv[2])
    logging.info(f'Initializing Spark Session for {report_name}')
    spark = get_spark_session(report_name=report_name)
    logging.info(f'Opening reports file for {report_name}')
    with open("reports.json") as reports_file:
        reports = json.load(reports_file)
    execute_report(spark, reports, report_name, batch_month)


if __name__ == '__main__':
    main()