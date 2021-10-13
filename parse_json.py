import json
from pyspark.sql import SparkSession
from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, DecimalType


def parse_json(line):
    record = json.loads(line)
    record_type = record['event_type']

    if record_type == "Q":
        trade_date = datetime.strptime(record['trade_dt'], '%Y-%m-%d')
        rec_type = record_type
        symbol = record['symbol']
        exchange = record['exchange']
        event_time = datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f')
        event_number = int(record['event_seq_nb'])
        arrival_time = datetime.strptime(record['file_tm'], '%Y-%m-%d %H:%M:%S.%f')
        trade_pr = None
        bid_price = Decimal(record['bid_pr'])
        bid_size = int(record['bid_size'])
        ask_price = Decimal(record['ask_pr'])
        ask_size = int(record['ask_size'])
        partition = record_type
        event = [trade_date, rec_type, symbol, exchange, event_time, event_number, arrival_time, trade_pr, bid_price, bid_size, ask_price, ask_size, partition]
        return event

    elif record_type == 'T':
        trade_date = datetime.strptime(record['trade_dt'], '%Y-%m-%d')
        rec_type = record_type
        symbol = record['symbol']
        exchange = record['exchange']
        event_time = datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f')
        event_number = int(record['event_seq_nb'])
        arrival_time = datetime.strptime(record['file_tm'], '%Y-%m-%d %H:%M:%S.%f')
        trade_pr = Decimal(record['price'])
        bid_price = None
        bid_size = None
        ask_price = None
        ask_size = None
        partition = record_type
        event = [trade_date, rec_type, symbol, exchange, event_time, event_number, arrival_time, trade_pr, bid_price, bid_size, ask_price, ask_size, partition]
        return event

    else:
        trade_date = None
        rec_type = 'B'
        symbol = None
        exchange = None
        event_time = None
        event_number = None
        arrival_time = None
        trade_pr = None
        bid_price = None
        bid_size = None
        ask_price = None
        ask_size = None
        partition = 'B'
        event = [trade_date, rec_type, symbol, exchange, event_time, event_number, arrival_time, trade_pr, bid_price, bid_size, ask_price, ask_size, partition]
        return event

