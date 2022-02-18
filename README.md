# Equity Market Analysis

The following project entails a pipeline which collects both JSON- and CSV-format stock data from an Azure storage container, unifies the data under a common schema, pre-processes the data, and performs analytical ETL. An overview of the pipeline is provided in the figure below:



![Data flow diagram for equity market analysis](https://github.com/Aaron-O-Gonzalez/Equity-Data-Analysis/blob/main/EquityDataAnalysis_Flowchart.png)



## Part 1: Data Ingestion and Parsing

The data is stored in either CSV or JSON files. Each record is categorized as either a trade "T" or quote "Q" record. 

Each "T" record contains the following fields: **trade date (trade_dt)**, **record_type (denoted as either T or Q)**, **symbol**, **exchange**, **event_time (event_tm)**, **event sequence number (event_seq_nb)**, **arrival_time (arrival_tm)**, **trade_price (trade_pr)**

Each "Q" record contains the following fields: **trade date (trade_dt)**, **record_type (denoted as either T or Q)**, **symbol**, **exchange**, **event_time (event_tm)**, **event sequence number (event_seq_nb)**, **arrival_time (arrival_tm)**,  **bid_price**,  **bid_size**, **ask_price**, **ask_size**.

Both records are located in the **data** folder, with the subdirectories being either the **csv** or **json** formats. A copy of this folder has been added to this repository and can be saved into an Azure storage account container for reproducibility. Prior to copying the file, the user needs to generate an Azure Storage Account and Container. After installing <em> azcopy </em>, the user can use Azure Active Directory for single sign-in as follows:

```azcopy login --tenant-id=<tenant-id>```

 The file can then be transferred to Azure using the following steps:

```azcopy copy "<file_dir>/data" "https://<storage-account>account.blob.core.windows.net/<container-name>/"```

### (a) Configuration

To ensure that the file is properly read from the Blob storage, the user needs to acquire the Storage Account **access keys**, which will be used for configuring the Spark Session:

```access_key = <access_key>```

```spark = SparkSession.builder.master('local').appName('app').config("fs.azure.account.key.<storage-account-name>.blob.core.windows.net", access_key).config("spark.hadoop.fs.azure.account.key.<storage-account-name>.blob.core.windows.net", access_key).getOrCreate()```

In addition to this configuration, the user must supply additional jar files, attached in this repository.

### (b) Reading and Parsing Data

The trade data will be read from both CSV and JSON files and loaded into a Spark RDD. Using either **parse_csv** or **parse_json** functions, the files will be parsed and partitioned based on their record as either being "T" or "Q". Any records that do are not "T" nor "Q" will be categorized as type "B" and be in their own separate partition. The parsed data is then converted into a Spark Dataframe with the following schema:

| Field        | Data Type     |
| ------------ | ------------- |
| trade_dt     | DateType      |
| rec_type     | StringType    |
| symbol       | StringType    |
| exchange     | StringType    |
| event_tm     | TimestampType |
| event_seq_nb | IntegerType   |
| arrival_tm   | TimestampType |
| trade_pr     | DecimalType   |
| bid_pr       | DecimalType   |
| bid_size     | IntegerType   |
| ask_pr       | DecimalType   |
| ask_size     | IntegerType   |
| partition    | StringType    |

*Note that "T" records do not contain bid_pr, bid_size, ask_pr, and ask_size, while "Q" records do not contain "trade_pr". These fields accept null values.

### (c) Combining Data

Once quote and trade data are parsed from their respective CSV or JSON files, they will be unified into one data frame with the common schema. 

### (d) Writing Data to Parquet

The dataframe containing both "Q" and "T" records now contains the respective partitions that is used for writing to an output directory:

```output_dir = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/partitioned_trade_data/"```

```trade_data.write.partitionBy("partition").parquet(<output_dir>)```



## Part 2: Data Pre-processing

Each "T" or "Q" record can has a unique identifier that is composed of **trade_dt**, **symbol**,**exchange**, **event_tm**, and **event_seq_nb**. However, several records may be repeated, as the **arrival_tm** can be at an earlier/later time. Further, as mentioned in Part 1, there are fields in both "T" and "Q" records that are empty. Thus, this section of the pipeline uses the **latest_quote_record** and **latest_trade_record** functions to eliminate unncessary fields and to retain only the record with the most up-to-date arrival_tm. 

Once the "T" and "Q" records are pre-processed, they will be partitioned by **trade_dt** and separately written to their respective output folders in the user storage container.

## Part 3: Analytical ETL

The trade and quote records that were stored from Step 2 can now be read into separate dataframes by date. 

Starting with the trade data for **86/2020** the dataframe is partitioned by trade symbol, and a moving price average is calculated at 30 minute intervals. 

```sql_query = """SELECT rec_type, symbol, exchange, event_tm, event_seq_nb, trade_pr, AVG(trade_pr) OVER ( ```
```        PARTITION BY symbol ```
```        ORDER BY CAST(event_tm AS timestamp) ```
```        RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW ```
```     ) AS mov_avg_pr FROM tmp_trade_moving_avg""" ```

The trade 









