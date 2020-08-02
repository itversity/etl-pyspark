import logging
from pyspark.sql import SparkSession


def get_spark_session():
    spark = SparkSession. \
        builder. \
        master('local'). \
        appName('ETL Pyspark'). \
        enableHiveSupport(). \
        getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    return spark


def execute_query(spark, report_name, query_name, query, batch_date):
    try:
        logging.info(f'''Running query {query_name} for {report_name}''')
        statement = query['process.statement'].format(batch_date=batch_date)
        spark.sql(statement)
    except Exception as e:
        logging.error(e)
        raise


def execute_report(spark, reports, report_name, batch_date):
    logging.info(f'''Processing data for report {report_name}''')
    report_queries = reports[report_name]
    for query in report_queries:
        for key in query:
            execute_query(spark, report_name, key, query[key], batch_date)