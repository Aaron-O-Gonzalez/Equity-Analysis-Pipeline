from parse_csv import parse_csv
from parse_json import parse_json
from pyspark.sql import SparkSession
from typing import List
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, DecimalType

common_event = StructType([ \
    StructField("trade_dt",DateType(),True), \
    StructField("rec_type",StringType(),True), \
    StructField("symbol",StringType(),True), \
    StructField("exchange", StringType(), True), \
    StructField("event_tm", TimestampType(), True), \
    StructField("event_seq_nb", IntegerType(), True), \
    StructField("arrival_tm", TimestampType(), True), \
    StructField("trade_pr", DecimalType(), True), \
    StructField("bid_pr", DecimalType(), True), \
    StructField("bid_size", IntegerType(), True), \
    StructField("ask_pr", DecimalType(), True), \
    StructField("ask_size", IntegerType(), True), \
    StructField("partition", StringType(), False)
  ])

access_key = '<storage-access-key>'

spark = SparkSession.builder.master('local').appName('app').config("fs.azure.account.key.exchangedata5000.blob.core.windows.net", access_key).config("spark.hadoop.fs.azure.account.key.exchangedata5000.blob.core.windows.net", access_key).getOrCreate()
sc = spark.sparkContext


csv_raw = sc.textFile('wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/data/csv/*/*')
parsed = csv_raw.map(lambda line:parse_csv(line))
csv_data = spark.createDataFrame(parsed, schema=common_event)

json_raw = sc.textFile('wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/data/json/*/*')
parsed = json_raw.map(lambda line:parse_json(line))
json_data = spark.createDataFrame(parsed, schema=common_event)

trade_data = csv_data.union(json_data)

output_dir = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/partitioned_trade_data/"
trade_data.write.partitionBy("partition").parquet(output_dir)
