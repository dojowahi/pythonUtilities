from pyspark.sql import *
from os.path import expanduser, join, abspath
import sys
from datetime import datetime, timedelta
import json
import time

warehouse_location = abspath('/tmp/warehouse/my_dim_test/')
spark = SparkSession.builder.appName('Extract_EDW_Data').config("spark.sql.warehouse.dir",warehouse_location).config("spark.hadoop.orc.overwrite.output.file", "true").enableHiveSupport().getOrCreate()

db_name = 'okimgood'
oracle_host = 'myhot.example.com'
oracle_port = 1531
oracle_user = 'haha'
oracle_pwd = 'hoho'

url = 'jdbc:oracle:thin:{0}/{1}@{2}:{3}/{4}'.format(oracle_user, oracle_pwd, oracle_host, oracle_port, db_name)

query = """(select ORDERS_KEY, LAST_UPDATE_DATE, (LAST_UPDATE_DATE - TO_DATE('19700101000000', 'yyyymmddhh24miss'))*24*3600 as DATE_INT from O.ORDERS_FACT where EVENT_DATE_KEY in (20191004))"""

df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", query) \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("numPartitions", "36") \
    .option('partitionColumn', "DATE_INT") \
    .option("lowerBound", 1570105075) \
    .option("upperBound", 1570105675) \
    .option("fetchsize", 10000) \
    .load()

df.write.option("compression", "zlib").format("orc").mode("Overwrite").save('s3://mybucket/O.ORDER_FACT_fact_edw_extract_test')

