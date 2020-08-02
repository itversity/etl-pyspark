import logging
from pyspark.sql import SparkSession


def get_spark_session():
    spark = SparkSession. \
        builder. \
        master('local'). \
        appName('ETL Pyspark'). \
        enableHiveSupport(). \
        getOrCreate()
    return spark


def execute_query(spark, report_name, query_name, query):
    try:
        logging.info(f'''Running query {query_name} for {report_name}''')
        spark.sql(query['query'])
    except Exception as e:
        logging.error(e)
        raise


def execute_report(spark, reports, report_name):
    report_queries = reports[report_name]
    for query in report_queries:
        for key in query:
            execute_query(spark, report_name, key, query[key])