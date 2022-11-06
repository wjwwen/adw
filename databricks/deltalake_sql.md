# Delta Lake with Spark SQL
Delta Lake is an open-source storage framework that enables a Lakehouse architecture with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs. 

## Table of Contents
- [Create and query delta tables](#Create-and-query-delta-tables)
    - [Create and use managed db](#Create-and-use-managed-db)
    - [Query by table name](#Query-by-table-name)
    - [Query by path name](#Query-by-path-name)
    - [Convert Parquet table to Delta Lake format](#Convert-Parquet-table-to-Delta-lake-format)
    - [Create table, define schema explicitly with SQL DDL](#Create-table,-define-schema-explicitly-with-SQL-DDL)

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

### Create table, define schema explicitly with SQL DDL
```sql
CREATE TABLE [dbName.] tableName (
    id INT [NOT NULL],
    name STRING,
    date DATE,
    int_rate FLOAT)
USING DELTA
[PARTITIONED BY (time, date)] -- optional
```
