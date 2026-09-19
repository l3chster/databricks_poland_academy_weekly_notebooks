from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

catalog_name = spark.conf.get("crime.ingestion.catalog_name")
STORAGE_PATH = f"/Volumes/{catalog_name}/crime_bronze/streaming"
SCHEMA_LOCATION = f"/Volumes/{catalog_name}/crime_bronze/schemas"

AUTOLOADER_OPTIONS = {
    "cloudFiles.format": "json",
    "cloudFiles.schemaLocation": SCHEMA_LOCATION    
}

crime_schema = StructType([
    StructField("incident_id", StringType(), True),         
    StructField("crime_type", StringType(), True),         
    StructField("district", StringType(), True),            
    StructField("city", StringType(), True),                
    StructField("state", StringType(), True),               
    StructField("address", StringType(), True),            
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("incident_datetime", StringType(), True),   
    StructField("officer_id", StringType(), True),          
    StructField("officer_first_name", StringType(), True), 
    StructField("officer_last_name", StringType(), True),  
    StructField("badge_number", StringType(), True),        
    StructField("suspect_id", StringType(), True),          
    StructField("suspect_first_name", StringType(), True),  
    StructField("suspect_last_name", StringType(), True),   
    StructField("suspect_age", StringType(), True),
    StructField("suspect_gender", StringType(), True),      
    StructField("suspect_race", StringType(), True),        
    StructField("victim_id", StringType(), True),           
    StructField("victim_first_name", StringType(), True),   
    StructField("victim_last_name", StringType(), True),    
    StructField("victim_age", StringType(), True),
    StructField("victim_gender", StringType(), True),       
    StructField("victim_phone", StringType(), True),        
    StructField("weapon_used", StringType(), True),        
    StructField("severity", StringType(), True),            
    StructField("case_status", StringType(), True),         
    StructField("resolution", StringType(), True),          
    StructField("num_arrests", StringType(), True),
    StructField("property_loss_usd", StringType(), True),
    StructField("reported_online", StringType(), True),        
    StructField("notes", StringType(), True)                       
])


# Basic record parsing and adding ETL audit columns
def parse(df):
    return (df
        .withColumn("etl_processed_timestamp", current_timestamp())
        .withColumn("etl_rec_uuid", expr("uuid()"))    # adds unique id for each row
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("file_modification_time", col("_metadata.file_modification_time"))
    )


@dp.table(
    name="crime_streaming_bronze",
    comment="raw crime events",
    table_properties={
        "quality": "bronze",
        #"pipelines.reset.allowed": "false",  # preserves the data in the delta table if you do full refresh
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def crime_streaming():
    return (
        spark.readStream
             .format("cloudFiles")
             .options(**AUTOLOADER_OPTIONS)
             .schema(crime_schema)
             .load(STORAGE_PATH)
             .transform(parse)
    )