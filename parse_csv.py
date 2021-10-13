from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, DecimalType
from datetime import datetime
from decimal import Decimal

def parse_csv(line):
    record_type_pos = 2
    record = line.split(',')

    if record[record_type_pos] == 'Q':
        trade_date = datetime.strptime(record[0], '%Y-%m-%d')
        record_type = record[record_type_pos]
        symbol = record[3]
        exchange = record[6]
        event_time = datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f')
        event_number = int(record[5])
        arrival_time = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f')
        trade_price = None
        bid_price = Decimal(record[7])
        bid_size = int(record[8])
        ask_price = Decimal(record[9])
        ask_size = int(record[10])
        partition = record[record_type_pos]
        event = [trade_date, record_type, symbol, exchange, event_time, event_number, arrival_time, trade_price, bid_price, bid_size, ask_price, ask_size, partition]
        return event

    elif record[record_type_pos] == 'T':
        trade_date = datetime.strptime(record[0], '%Y-%m-%d')
        record_type = record[record_type_pos]
        symbol = record[3]
        exchange = record[6]
        event_time = datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f')
        event_number = int(record[5])
        arrival_time = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f')
        trade_price = Decimal(record[7])
        bid_price = None
        bid_size = None
        ask_price = None
        ask_size = None
        partition = record[record_type_pos]
        event = [trade_date, record_type, symbol, exchange, event_time, event_number, arrival_time, trade_price, bid_price, bid_size, ask_price, ask_size, partition]
        return event

    else:
        trade_date = None
        record_type = 'B'
        symbol = None
        exchange = None
        event_time = None
        event_number = None
        arrival_time = None
        trade_price = None
        bid_price = None
        bid_size = None
        ask_price = None
        ask_size = None
        partition = 'B'
        event = [trade_date, record_type, symbol, exchange, event_time, event_number, arrival_time, trade_price, bid_price, bid_size, ask_price, ask_size, partition]
        return event

