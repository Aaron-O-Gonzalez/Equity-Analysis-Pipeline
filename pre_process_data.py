from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as f

'''The quote and trade records previously written to Blob storage share a common schema
but not all fields are being used. The following functions will select the columns relevant
to the record type and also filter out the records with the most recent arrival time (arrival_tm).
Each record is grouped by a unique identifier composed of trade date (trade_dt), symbol, 
exchange, event time (event_tm), and event sequence number (event_seq_nb) '''

def latest_trade_record(trade_records):
    trade= trade_records.select("rec_type","trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm","trade_pr")
    trade_grouped = trade.join(trade.groupBy('trade_dt','symbol','exchange','event_tm', 'event_seq_nb').agg(f.max('arrival_tm').alias('arrival_tm')), on = 'arrival_tm', how = 'leftsemi')
    trade_grouped = trade_grouped.select("rec_type","trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm","trade_pr")
    return trade_grouped

def latest_quote_record(quote_records):
    quote = quote_records.select("rec_type","trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm","bid_pr","bid_size","ask_pr","ask_size")
    quote_grouped = quote.join(quote.groupBy('trade_dt','symbol','exchange','event_tm', 'event_seq_nb').agg(f.max('arrival_tm').alias('arrival_tm')), on = 'arrival_tm', how = 'leftsemi')
    quote_grouped = quote_grouped.select("rec_type","trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm","bid_pr","bid_size","ask_pr","ask_size")
    return quote_grouped



