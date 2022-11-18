# Delta Lake with Python
Delta Lake is an open-source storage framework that enables a Lakehouse architecture with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs. 

## Table of Contents
- [Reads and Writes with Delta Lake](#reads-and-writes-with-delta-lake)
    - [Read data from pandas DataFrame](#read-data-from-pandas-dataframe)
    - [Read data using Apache Spark](#read-data-using-apache-spark)
    - [Save DataFrame in Delta Lake format](#save-dataframe-in-delta-lake-format)
    - [Streaming reads (Delta table as streaming source)](#streaming-reads-delta-table-as-streaming-source)
    - [Streaming writes (Delta table as a sink)](#streaming-writes-delta-table-as-a-sink)
- [Convert Parquet to Delta lake](#convert-parquet-to-delta-lake)
    - [Convert Parquet table to Delta Lake format in place](#convert-parquet-table-to-delta-lake-format-in-place)
- [Working with Delta Tables](#working-with-delta-tables)
- [Delta Lake DDL/DML](#delta-lake-ddldml)
    - [Delete rows that match a predicate condition](#delete-rows-that-match-a-predicate-condition)
    - [Update rows that match a predicate condition](#update-rows-that-match-a-predicate-condition)
    - [Upsert (update + insert) using MERGE](#upsert-update--insert-using-merge)
    - [Insert with Deduplication using MERGE](#insert-with-deduplication-using-merge)
- [Time Travel](#time-travel)
    - [View transaction log (aka Delta Log)](#view-transaction-log-aka-delta-log)
    - [Query historical versions of Delta Lake tables](#query-historical-versions-of-delta-lake-tables)
    - [Find changes between 2 versions of a table](#find-changes-between-2-versions-of-a-table)
    - [Rollback a table by version or timestamp](#rollback-a-table-by-version-or-timestamp)
- [Utility Methods](#utility-methods)
    - [Run Spark SQL queries in Python](#run-spark-sql-queries-in-python)
    - [Compact old files with Vacuum](#compact-old-files-with-vacuum)
    - [Clone a Delta Lake table](#clone-a-delta-lake-table)
    - [Get DataFrame representation of a Delta Lake table](#get-dataframe-representation-of-a-delta-lake-table)
    - [Run SQL queries on Delta Lake tables](#run-sql-queries-on-delta-lake-tables)
- [Performance Optimization](#performance-optimization)
    - [Compact data files with Optimize and Z-Order](#compact-data-files-with-optimize-and-z-order)
    - [Auto-optimize tables](#auto-optimize-tables)
    - [Cache frequently queried data in Delta Cache](#cache-frequently-queried-data-in-delta-cache)

# Reads and Writes with Delta Lake
### Read data from pandas DataFrame
```python
df = spark.createDataFrame(pdf)
# where pdf is a pandas DF
# then save DataFrame in Delta Lake format as shown below
```

### Read data using Apache Spark
```python
# read by path
df = (spark.read.format("parquet"|"csv"|"json"|etc.)
 .load("/path/to/delta_table"))
# read table from Hive metastore
df = spark.table("events")
```

### Save DataFrame in Delta Lake format
```python
(df.write.format("delta")
 .mode("append"|"overwrite")
 .partitionBy("date") # optional
 .option("mergeSchema", "true") # option - evolve schema
 .saveAsTable("events") | .save("/path/to/delta_table")
)
```

### Streaming reads (Delta table as streaming source)
```python
# by path or by table name
df = (spark.readStream
 .format("delta")
 .schema(schema)
 .table("events") | .load("/delta/events")
)
```

### Streaming writes (Delta table as a sink)
```python
streamingQuery = (
df.writeStream.format("delta")
 .outputMode("append"|"update"|"complete")
 .option("checkpointLocation", "/path/to/checkpoints")
 .trigger(once=True|processingTime="10 seconds")
 .table("events") | .start("/delta/events")
)
```

# Convert Parquet to Delta Lake
### Convert Parquet table to Delta Lake format in place
```python
deltaTable = DeltaTable.convertToDelta(spark,
"parquet.`/path/to/parquet_table`")

partitionedDeltaTable = DeltaTable.convertToDelta(spark,
"parquet.`/path/to/parquet_table`", "part int")
```

# Working with Delta Tables
```python
# A DeltaTable is the entry point for interacting with
tables programmatically in Python — for example, to
perform updates or deletes.

from delta.tables import *
deltaTable = DeltaTable.forName(spark, tableName)
deltaTable = DeltaTable.forPath(spark,
delta.`path/to/table`)
```

# Delta Lake DDL/DML
> Recap - SQL Commands <br>
DDL: Data Definition Language <br>
DML: Data Manipulation Language <br>
DCL: Data Control Language <br>
TCL: Transaction Control Language

| DDL         | DML          | DCL      | TCL              |   
| ---         | ---          | ---      | ---              |
| Create      | Select       | Grant    | Commit           |   
| Alter       | Insert       | Revoke   | Rollback         |   
| Drop        | Update       |          | Savepoint        |   
| Rename      | Delete       |          | Set Transaction  |   
| Truncate    | Merge        |          |                  |   
| Comment     | Call         |          |                  |   
| Comment     | Explain Plan |          |                  |   
|             | Lock Table   |          |                  |  

### Delete rows that match a predicate condition
```python
# predicate using SQL formatted string
deltaTable.delete("date < '2017-01-01'")
# predicate using Spark SQL functions
deltaTable.delete(col("date") < "2017-01-01")
```

### Update rows that match a predicate condition
```python
# predicate using SQL formatted string
deltaTable.update(condition = "eventType = 'clk'",
 set = { "eventType": "'click'" } )
# predicate using Spark SQL functions
deltaTable.update(condition = col("eventType") == "clk",
 set = { "eventType": lit("click") } )
```

### Upsert (update + insert) using MERGE
```python
# Available options for merges [see documentation for
details]:
.whenMatchedUpdate(...) | .whenMatchedUpdateAll(...) |
.whenNotMatchedInsert(...) | .whenMatchedDelete(...)
(deltaTable.alias("target").merge(
 source = updatesDF.alias("updates"),
 condition = "target.eventId = updates.eventId")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsert(
 values = {
 "date": "updates.date",
 "eventId": "updates.eventId",
 "data": "updates.data",
 "count": 1
 }
 ).execute()
)
```

### Insert with Deduplication using MERGE
Data deduplication: eliminating a dataset's redundant data

```python
(deltaTable.alias("logs").merge(
 newDedupedLogs.alias("newDedupedLogs"),
 "logs.uniqueId = newDedupedLogs.uniqueId")
 .whenNotMatchedInsertAll()
.execute()
)
```

# Time Travel
### View transaction log (aka Delta Log)
```python
fullHistoryDF = deltaTable.history()
```

### Query historical versions of Delta Lake tables
```python
# choose only one option: versionAsOf, or timestampAsOf
df = (spark.read.format("delta")
 .option("versionAsOf", 0)
 .option("timestampAsOf", "2020-12-18")
 .load("/path/to/delta_table"))
```

### Find changes between 2 versions of a table
```python
df1 = spark.read.format("delta").load(pathToTable)
df2 = spark.read.format("delta").option("versionAsOf",
2).load("/path/to/delta_table")
df1.exceptAll(df2).show()
```

### Rollback a table by version or timestamp
```python
deltaTable.restoreToVersion(0)
deltaTable.restoreToTimestamp('2020-12-01')
````

# Utility Methods
### Run Spark SQL queries in Python
```python
spark.sql("SELECT * FROM tableName")
spark.sql("SELECT * FROM delta.`/path/to/delta_table`")
spark.sql("DESCRIBE HISTORY tableName")
```

### Compact old files with Vacuum
```python
deltaTable.vacuum() # vacuum files older than default
retention period (7 days)
deltaTable.vacuum(100) # vacuum files not required by
versions more than 100 hours old
```

### Clone a Delta Lake table
```python
deltaTable.clone(target="/path/to/delta_table/",
isShallow=True, replace=True)
```

### Get DataFrame representation of a Delta Lake table
```python
df = deltaTable.toDF()
```

### Run SQL queries on Delta Lake tables
```python
spark.sql("SELECT * FROM tableName")
spark.sql("SELECT * FROM delta.`/path/to/delta_table`")
```

# Performance Optimization
### Compact data files with Optimize and Z-Order
```python
*Databricks Delta Lake feature
spark.sql("OPTIMIZE tableName [ZORDER BY (colA, colB)]")
```

### Auto-optimize tables
```python
# Databricks Delta Lake feature. For existing tables:
spark.sql("ALTER TABLE [table_name |
delta.`path/to/delta_table`]
SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)

# To enable auto-optimize for all new Delta Lake tables:
spark.sql("SET spark.databricks.delta.properties.
defaults.autoOptimize.optimizeWrite = true")
```
### Cache frequently queried data in Delta Cache
```python
# Databricks Delta Lake feature
spark.sql("CACHE SELECT * FROM tableName")
-- or:
spark.sql("CACHE SELECT colA, colB FROM tableName
 WHERE colNameA > 0")
```