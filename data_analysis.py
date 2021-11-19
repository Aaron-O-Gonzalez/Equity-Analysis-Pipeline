from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from os.path import abspath
import datetime
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, DecimalType

access_key = '<access_key>'

spark = SparkSession \
        .builder.master('local') \
        .appName('app') \
        .config("fs.azure.account.key.<storage-account-name>.blob.core.windows.net", access_key) \
        .config("spark.hadoop.fs.azure.account.key.<storage-account-name>.blob.core.windows.net", access_key) \
        .enableHiveSupport() \
        .getOrCreate()

sc = spark.sparkContext

#Read trade data from 8-06-2020 time period and create a temp view for SQL query
trade_dir = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/latest_trade/trade_dt=2020-08-06/"
df = spark.read.parquet(trade_dir)

df = df.select('rec_type','symbol', 'exchange', 'event_tm', 'event_seq_nb', 'trade_pr')
df.createOrReplaceTempView("tmp_trade_moving_avg")

#Using the trade data from 8-06-2020
sql_query = """SELECT rec_type, symbol, exchange, event_tm, event_seq_nb, trade_pr, AVG(trade_pr) OVER (
        PARTITION BY symbol 
        ORDER BY CAST(event_tm AS timestamp) 
        RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW
     ) AS mov_avg_pr FROM tmp_trade_moving_avg"""

mov_avg_df = spark.sql(sql_query)
mov_avg_df.createOrReplaceTempView("tmp_trade_moving_avg")
#mov_avg_df.write.saveAsTable("temp_trade_moving_avg")

'''Calculate the earlier date using datetime function and retrieve records from this date'''
previous_trade_dir = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/latest_trade/trade_dt=2020-08-05/"
df = spark.read.parquet(previous_trade_dir)

df = df.select('rec_type','symbol', 'exchange', 'event_tm', 'event_seq_nb', 'trade_pr')
df.createOrReplaceTempView("tmp_last_trade")

sql_query = """WITH cte AS (SELECT rec_type, symbol, exchange, event_tm, event_seq_nb, AVG(trade_pr) OVER (
                            PARTITION BY symbol 
                            ORDER BY CAST(event_tm AS timestamp) 
                            RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW
                            ) AS last_mov_avg_pr, 
                            RANK () OVER (PARTITION BY symbol ORDER BY event_tm DESC) time_rank
                            FROM tmp_last_trade)

               SELECT rec_type, symbol, event_tm, event_seq_nb, last_mov_avg_pr 
               FROM cte WHERE 
               time_rank =1"""  

latest_trade_df = spark.sql(sql_query)
latest_trade_df.createOrReplaceTempView("tmp_last_trade")
#latest_trade_df.write.saveAsTable("latest_trade")

'''Generate a union with common schema'''
quote_dir = 'wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/latest_quote/trade_dt=2020-08-06'
quote_df = spark.read.parquet(quote_dir)

quote_union = quote_df.unionByName(mov_avg_df, allowMissingColumns=True)
quote_union.createOrReplaceTempView("quote_union")


'''Obtain most recent trade value and filter out for quote records'''
sql_query= """WITH cte AS (SELECT rec_type, symbol, event_tm, event_seq_nb,
                            arrival_tm, bid_pr, bid_size, ask_pr, ask_size, trade_pr,
                            mov_avg_pr, sum(case when trade_pr is null then 0 else 1 end) over (PARTITION BY symbol ORDER BY event_tm) as event_partition
                            FROM quote_union
                            ORDER BY event_tm)

              SELECT rec_type, symbol, event_tm, event_seq_nb, arrival_tm, 
              bid_pr, bid_size, ask_pr, ask_size, trade_pr, first_value(trade_pr) over (PARTITION BY symbol, event_partition ORDER BY event_tm) as last_trade_pr,
              first_value(mov_avg_pr) over (PARTITION BY symbol,event_partition ORDER BY event_tm) as last_mov_avg_pr
              FROM cte
              ORDER BY event_tm"""

ordered_df = spark.sql(sql_query)
ordered_df.createOrReplaceTempView("quote_updated")

filter_quote_records = """SELECT symbol, event_tm, event_seq_nb, arrival_tm, bid_pr,
                          bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
                          FROM quote_updated
                          WHERE rec_type = 'Q'"""
quote_records_updated = spark.sql(filter_quote_records)
quote_records_updated.createOrReplaceTempView("quote_records_updated")

'''Create dataframe which selects the last record from the previous day's trading
and performs a left join on the update quote df'''
sql_query = """SELECT symbol, last_mov_avg_pr as close_pr FROM tmp_last_trade"""
last_trade_records = spark.sql(sql_query)

cond = [quote_records_updated.symbol ==  last_trade_records.symbol]
quote_join = quote_records_updated.join(broadcast(last_trade_records), on=cond, how='left').select('event_tm',quote_records_updated['symbol'],'event_seq_nb','arrival_tm','bid_pr','bid_size','ask_pr','ask_size','last_trade_pr','last_mov_avg_pr','close_pr')
quote_join.createOrReplaceTempView("quote_join")

quote_final = spark.sql("""
select event_tm, symbol, event_seq_nb,
bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr,
bid_pr - close_pr as bid_pr_mv, ask_pr - close_pr as ask_pr_mv
from quote_join
ORDER BY event_tm""")

quote_final.write.mode("overwrite").parquet("wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/quote-trade-analytical/date=2020-08-06")


