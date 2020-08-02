# ETL Pyspark

* Clone GitHub Repository
* Create virtual environment specific to this project
* Install dependencies
* Activate Virtual environment
* Launch spark-sql and create these 2 tables.
```sql
CREATE TABLE t (d DATE) LOCATION 'file:/Users/itversity/Projects/Internal/etl-pyspark/t';
CREATE TABLE ts (t TIMESTAMP ) LOCATION 'file:/Users/itversity/Projects/Internal/etl-pyspark/ts';
```
* Make sure to create logs folder
* Run using `spark-submit app.py REPORT_1`