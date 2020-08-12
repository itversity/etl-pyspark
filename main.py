import importlib
import sys


sys.path.insert(0, 'dist/etl-pyspark.zip')

report_name = sys.argv[1]
batch_month = int(sys.argv[2])
job_module = importlib.import_module('etl_pyspark.app')
job_method = getattr(job_module, 'run_job')
job_method(report_name, batch_month)