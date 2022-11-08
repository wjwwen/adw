from pdp_normalization.rules.rules import Rules
from pdp_normalization.normalization.normalizations import NormalizationRule
import logging, json
from pyspark.sql import Row
from pdp_normalization.common import logger
from delta_export.delta_postgresql_export import DeltaPostgreqlExport
from pdp_normalization.services.sceret_data_manager import get_secret
import boto3
import json
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

# %%
# Declare variables:
bucket_location = 'plt-dswb-raw-dev-use1-s3'
bucket_prefix = "exchange-data/raw/"
seed_file = "mft_Italy_ICE_Endex_Daily"
save_location = "s3://plt-dswb-data-dev-use1-s3/commodity-prices/normalized-ice-italy-psv-futures"
table_name = "normalized_ice_italy_psv_futures"

# %%
def check_table_exist(db_tbl_name):
    table_exist = False
    try:
        spark.read.table(db_tbl_name) # Check if spark can read the table
        table_exist = True        
    except:
        pass
    return table_exist 

# %%
# Get list of files from S3
# Set up s3 resource and bucket
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucket_location)

# Create a list of file names
s3_files = my_bucket.objects.filter(Prefix=bucket_prefix).all()
temp_files = [s3_files.key for s3_files in s3_files]

# Search list of files for key words such as country to line up with appropriate rules files
files_df = pd.DataFrame(columns = ['filename'])
for file in temp_files:
  if seed_file in file:
#     parse_filename = file.split('_')
    files_df = files_df.append({'filename': file}, ignore_index=True)

# display(files_df)

# %%
# Transform
# Define functions
def normalize_psv(df_raw):
  df_raw = df_raw.withColumn("fields",explode("fields")).select("*",col("fields.*"))

  df_raw = df_raw.withColumnRenamed("tableName","price_description")\
                 .withColumnRenamed("valueType","price_type")\
                 .withColumnRenamed("date","trade_date")\
                 .withColumnRenamed("value","price_value")\
                 .withColumn("unit_of_measure",lit("Euro/MWh"))

  # raw_df = raw_df.select(concat(lit("01-"),col("contractName")).as("contract_date")).show(false)
  df_raw = df_raw.withColumn("tmp_contract_date",concat(lit("01-"),col("contractName")))\
                 .withColumn("contract_date",to_date(col("tmp_contract_date"),"dd-MMM-yy"))\
                 .withColumn("trade_date",to_date(col("trade_date"),"MM/dd/yyyy"))\
                 .withColumn("price_value", col("price_value").cast("float"))
  # raw_df = raw_df.to_date(col("contract_date"),"MM/dd/yyyy")

  df_raw = df_raw.select(["price_description","price_type","trade_date","contract_date","price_value","unit_of_measure","collection_date","file_name"])
  
  return df_raw

# %%
# Load first file
final_df.write.format("delta").mode("overwrite").save(save_location)

# %%
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS commodity_prices")
spark.sql("CREATE TABLE IF NOT EXISTS commodity_prices.{0} USING DELTA LOCATION {1} COMMENT 'Normalized data table for ICE Italian PSV Futures with uncoverted units'".format(table_name, repr(save_location)))
spark.sql("ALTER TABLE commodity_prices.{0} SET TBLPROPERTIES ( 'product' = 'lng', 'replication' = 'hourly', 'data_steward' = 'Analytics Data','it_owner' = 'Analytics Data', 'lifecycle' = 'none', 'checkpoint' = 'false' )".format(table_name))