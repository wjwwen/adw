# Delta Lake with Spark SQL
Delta Lake is an open-source storage framework that enables a Lakehouse architecture with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs. 

## Table of Contents
- [Create and query delta tables](#create-and-query-delta-tables)
    - [Create and use managed db](#create-and-use-managed-db)
    - [Query by table name](#query-by-table-name)
    - [Query by path name](#query-by-path-name)
    - [Convert Parquet table to Delta Lake format](#convert-parquet-table-to-delta-lake-format)
    - [Create table and define schema explicitly with SQL DDL](#create-table-and-define-schema-explicitly-with-sql-ddl)
- [Delta Lake DDL/DML](#delta-lake-ddldml)
    - [Update rows that match a predicate condition](#update-rows-that-match-a-predicate-condition)
    - [Delete rows that match a predicate condition](#delete-rows-that-match-a-predicate-condition)
    - [Insert values directly into table](#insert-values-directly-into-table)
    - [Upsert (update + insert) using MERGE](#upsert-update--insert-using-merge)
    - [Alter table schema — add columns](#alter-table-schema--add-columns)
    - [Alter table — add constraint](#alter-table--add-constraint)
- [Time Travel](#time-travel)
    - [View transaction log (aka Delta Log)](#view-transaction-log-aka-delta-log)
    - [Query historical versions of tables](#query-historical-versions-of-tables)
    - [Find changes between two versions of table](#find-changes-between-two-versions-of-table)
    - [Rollback a table to an earlier version](#rollback-a-table-to-an-earlier-version)
- [Utility Methods](#utility-methods)
    - [View table details](#view-table-details)
    - [Delete old files with Vacuum](#delete-old-files-with-vacuum)
    - [Clone a Delta Lake table](#clone-a-delta-lake-table)
    - [Interoperability with Python / DataFrames](#interoperability-with-python--dataframes)
    - [Run SQL queries from Python](#run-sql-queries-from-python)
    - [Modify data retention settings for Delta Lake table](#modify-data-retention-settings-for-delta-lake-table)
- [Performance Optimizations](#performance-optimizations)
    - [Compact data files with Optimize and Z-Order](#compact-data-files-with-optimize-and-z-order)
    - [Auto-optimize tables](#auto-optimize-tables)
    - [Cache frequently queried data in Delta Cache](#cache-frequently-queried-data-in-delta-cache)


[Delta Lake Documentation](https://docs.delta.io/latest/index.html)

# Create and query delta tables
### Create and use managed db
```sql
-- Managed database is saved in the Hive metastore.
Default database is named "default".
DROP DATABASE IF EXISTS dbName;
CREATE DATABASE dbName;
USE dbName -- This command avoids having to specify
dbName.tableName every time instead of just tableName.
```

### Query by table name 
```sql
SELECT * FROM [dbName.] tableName
```

### Query by path name 
```sql
SELECT * FROM delta.`path/to/delta_table` 
```

### Convert Parquet table to Delta Lake format
```sql
-- by table name
CONVERT TO DELTA [dbName.]tableName
[PARTITIONED BY (col_name1 col_type1, col_name2
col_type2)]
-- path-based tables
CONVERT TO DELTA parquet.`/path/to/table` -- note backticks
[PARTITIONED BY (col_name1 col_type1, col_name2 col_type2)]
```

### Create table and define schema explicitly with SQL DDL
```sql
CREATE TABLE [dbName.] tableName (
    id INT [NOT NULL],
    name STRING,
    date DATE,
    int_rate FLOAT)
USING DELTA
[PARTITIONED BY (time, date)] -- optional
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

### Update rows that match a predicate condition
```sql
UPDATE tableName SET event = 'click' WHERE event = 'clk'
```

### Delete rows that match a predicate condition
```sql
DELETE FROM tableName WHERE "date < '2017-01-01"
```

### Insert values directly into table
```sql
INSERT INTO TABLE tableName VALUES (
 (8003, "Kim Jones", "2020-12-18", 3.875),
 (8004, "Tim Jones", "2020-12-20", 3.750)
);
-- Insert using SELECT statement
INSERT INTO tableName SELECT * FROM sourceTable
-- Atomically replace all data in table with new values
INSERT OVERWRITE loan_by_state_delta VALUES (...)

```

### Upsert (update + insert) using MERGE
```sql
MERGE INTO target
USING updates
ON target.Id = updates.Id
WHEN MATCHED AND target.delete_flag = "true" THEN
 DELETE
WHEN MATCHED THEN
 UPDATE SET * -- star notation means all columns
WHEN NOT MATCHED THEN
 INSERT (date, Id, data) -- or, use INSERT *
 VALUES (date, Id, data)
```

### Alter table schema — add columns
```sql
ALTER TABLE tableName ADD COLUMNS (
 col_name data_type
 [FIRST|AFTER colA_name])
```

### Alter table — add constraint
```sql
-- Add "Not null" constraint:
ALTER TABLE tableName CHANGE COLUMN col_name SET NOT NULL
-- Add "Check" constraint:
ALTER TABLE tableName
ADD CONSTRAINT dateWithinRange CHECK date > "1900-01-01"
-- Drop constraint:
ALTER TABLE tableName DROP CONSTRAINT dateWithinRange
```

# Time Travel
### View transaction log (aka Delta Log)
```sql
DESCRIBE HISTORY tableName
```

### Query historical versions of tables
```sql
SELECT * FROM tableName VERSION AS OF 0
SELECT * FROM tableName@v0 -- equivalent to VERSION AS OF 0
SELECT * FROM tableName TIMESTAMP AS OF "2020-12-18"
```

### Find changes between two versions of table
```sql
SELECT * FROM tableName VERSION AS OF 12
EXCEPT ALL SELECT * FROM tableName VERSION AS OF 11
```

### Rollback a table to an earlier version
```sql
-- RESTORE requires Delta Lake version 0.7.0+ & DBR 7.4+.
RESTORE tableName VERSION AS OF 0
RESTORE tableName TIMESTAMP AS OF "2020-12-18"
```

# Utility Methods
### View table details
```sql
DESCRIBE DETAIL tableName
DESCRIBE FORMATTED tableName
```

### Delete old files with Vacuum
```sql
VACUUM tableName [RETAIN num HOURS] [DRY RUN]
```

### Clone a Delta Lake table
```sql
-- Deep clones copy data from source, shallow clones don't.
CREATE TABLE [dbName.] targetName
[SHALLOW | DEEP] CLONE sourceName [VERSION AS OF 0]
[LOCATION "path/to/table"]
-- specify location only for path-based tables
```

### Interoperability with Python / DataFrames
```sql
-- Read name-based table from Hive metastore into DataFrame
df = spark.table("tableName")
-- Read path-based table into DataFrame
df = spark.read.format("delta").load("/path/to/delta_table")
```

### Run SQL queries from Python
```sql
spark.sql("SELECT * FROM tableName")
spark.sql("SELECT * FROM delta.`/path/to/delta_table`")
```

### Modify data retention settings for Delta Lake table
```sql
-- logRetentionDuration -> how long transaction log history
is kept, deletedFileRetentionDuration -> how long ago a file
must have been deleted before being a candidate for VACCUM.
ALTER TABLE tableName
SET TBLPROPERTIES(
 delta.logRetentionDuration = "interval 30 days",
 delta.deletedFileRetentionDuration = "interval 7 days"
);
SHOW TBLPROPERTIES tableName
```

# Performance Optimizations
### Compact data files with Optimize and Z-Order
```sql
OPTIMIZE tableName
[ZORDER BY (colNameA, colNameB)]
```

### Auto-optimize tables
```sql
ALTER TABLE [table_name | delta.`path/to/delta_table`]
SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)
```

### Cache frequently queried data in Delta Cache
```sql
CACHE SELECT * FROM tableName
-- or:
CACHE SELECT colA, colB FROM tableName WHERE colNameA > 0
```