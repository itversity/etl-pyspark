import json, sys

from util import init_logger
from util.sparker import get_spark_session, execute_report


def main():
    init_logger('.')
    report_name = sys.argv[1]
    batch_date = sys.argv[2]
    spark = get_spark_session()
    with open("reports.json") as reports_file:
        reports = json.load(reports_file)
    execute_report(spark, reports, report_name, batch_date)


if __name__ == '__main__':
    main()