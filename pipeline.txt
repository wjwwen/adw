import logging, json
import pyspark.sql.types import * 
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.functions import input_file_name
from pyspark.sql.window import Window
// logging.basicConfig(level=logging.INFO)
// logging.getLogger("py4j.java_gateway".setLevel(logging.Error))

dbutils.fs.ls('s3://plt-dswb-raw-dev-use1-s3/exchange-data/raw/2022/07')

file = "s3://plt-dswb-raw-dev-use1-s3/exvhange-data/raw/2022/07/mft_Italy_ICE_Endex_Daily_10696031_20220701070223.json"

raw_df = spark.read.option("multiline", "true").json(file).withColumn("file_name", input_file_name())
display(raw_df)

def normalise_psf(df_raw):
    df_raw = df_raw.withColumn("fields", explode("fields")).select("*", col("fields.*"))

    df_raw = df_raw.withColumnRenamed("tableName","price_description")\
                   .withColumnRenamed("valueType", "price_type")\
                   .withColumnRenamed("date","trade_date")\
                   .withColumnRenamed("value","price_value")\
                   .withColumn("unit_of_measure",lit("Euro/MWh"))

    df_raw = df_raw.withColumn("tmp_contract_date", concat(lit("01-"), col("contract_name")))\
                   .withColumn("contract_date", to_date(col("tmp_contract_date"), "dd-MMM-yy"))\
                   .withColumn("trade_date", to_date(col("trade_date"), "MM/dd/yyyy"))\
                   .withColumn("price_value", col("price_value").cast("float"))

    df_raw = df_raw.select(["price_description", "price_type", "trade_date", "contract_date", "price_value", "unit_of_measure", "collection_date", "file_name"])

    return df_raw

final_df = normalize_psv(raw_df)
final_df = final_df.withColumn("rank", dense_rank().over(Window.partitionBy("trade_date", "contract_date").orderBy(desc("collection_date")))).where("rank=1").select("*").drop("rank")
