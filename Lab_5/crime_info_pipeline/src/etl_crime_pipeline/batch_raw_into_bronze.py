from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

catalog_name = spark.conf.get("crime.ingestion.catalog_name")
STORAGE_PATH = f"/Volumes/{catalog_name}/crime_bronze/batch"

crime_cities_schema = StructType([
    StructField("city_id", IntegerType(), True),         
    StructField("city", StringType(), True),         
    StructField("state", StringType(), True),            
    StructField("latitude", DoubleType(), True),                
    StructField("longitude", DoubleType(), True),               
    StructField("population", IntegerType(), True)                
])


# Basic record parsing and adding ETL audit columns
def parse(df):
    return (df
        .withColumn("etl_processed_timestamp", current_timestamp())
        .withColumn("etl_rec_uuid", expr("uuid()"))    # adds unique id for each row
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("file_modification_time", col("_metadata.file_modification_time"))
    )


@dp.materialized_view(
name=f"{catalog_name}.crime_bronze.crime_cities",
comment="batch table of crime cities",    
table_properties = {"quality" : "bronze"},  
cluster_by_auto = True
)
def crime_batch():
    return (
        spark.read \
        .format("csv") \
        .schema(crime_cities_schema) \
        .option("header", "true") \
        .option("sep", ",") \
        .load(STORAGE_PATH) \
        .transform(parse)
    )

    