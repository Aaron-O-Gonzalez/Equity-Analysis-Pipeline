from parse_csv import parse_csv
from parse_json import parse_json
from pyspark.sql import SparkSession
from typing import List
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, DecimalType
from pre_process_data import latest_quote_record, latest_trade_record

'''Create a common schema that unifies both JSON and CSV files'''
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

access_key = '<access_key>'

spark = SparkSession.builder.master('local').appName('app').config("fs.azure.account.key.<storage-account-name>.blob.core.windows.net", access_key).config("spark.hadoop.fs.azure.account.key.<storage-account-name>.blob.core.windows.net", access_key).getOrCreate()
sc = spark.sparkContext

csv_raw = sc.textFile('wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/data/csv/*/*')
parsed = csv_raw.map(lambda line:parse_csv(line))
csv_data = spark.createDataFrame(parsed, schema=common_event)

json_raw = sc.textFile('wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/data/json/*/*')
parsed = json_raw.map(lambda line:parse_json(line))
json_data = spark.createDataFrame(parsed, schema=common_event)

trade_data = csv_data.union(json_data)

output_dir = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/partitioned_trade_data/"
trade_data.write.partitionBy("partition").mode("overwrite").parquet(output_dir)


'''Read in the trade and quote data from the Blob that was previously generated'''
trade_output_dir = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/partitioned_trade_data/partition=T/"
trade_common = spark.read.parquet(trade_output_dir)

quote_output_dir = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/partitioned_trade_data/partition=Q/"
quote_common = spark.read.parquet(quote_output_dir)

'''Filter trade and quote records to contain the latest arrival_tm record for a
unique combination of trade_dt, symbol, exchange, event_tm, and event_seq_nb
'''
trade_latest = latest_trade_record(trade_common)
trade_latest_output_dir = 'wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/latest_trade/'
quote_latest = latest_quote_record(quote_common)
quote_latest_output_dir = 'wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/latest_quote/'

'''Write the pre-processed data into new directories partitioned by trade date'''
trade_latest.write.partitionBy("trade_dt").mode("overwrite").parquet(trade_latest_output_dir)
quote_latest.write.partitionBy("trade_dt").mode("overwrite").parquet(quote_latest_output_dir)
