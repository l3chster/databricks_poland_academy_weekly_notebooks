from pyspark import pipelines as dp
from pyspark.sql.functions import coalesce, col, current_timestamp, initcap, lit, lower, when, try_to_timestamp, expr
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
)

catalog_name = spark.conf.get("crime.ingestion.catalog_name")

validations = {
    "officer_id check": "officer_id LIKE 'OFF%'", 
    "suspect age check": "suspect_age IS NULL OR (suspect_age > 0 AND suspect_age < 120)",
    "victim age check": "victim_age IS NULL OR (victim_age > 0 AND victim_age < 120)",
    "badge number": "badge_number > 0",
    "num_arrests check": "num_arrests IS NULL OR num_arrests > 0"
    }

@dp.view
@dp.expect_or_drop("existing incident_id", "incident_id IS NOT NULL")
@dp.expect_all(validations)                   
def crime_silver_clean():

    df = spark.readStream.table(f"{catalog_name}.crime_bronze.crime_streaming_bronze")
    string_cols = [name for name, dtype in df.dtypes if dtype == "string"]    

    return (  

           # BUISNESS LOGIC         
           df.select('*')
           .replace(["N/A", "Unknown", "", " "], None, subset=string_cols)
           .withColumn(
           "suspect_gender", 
           when(lower(col("suspect_gender")).startswith("m"), "male")
           .when(lower(col("suspect_gender")).startswith("f"), "female")
           .otherwise(lit(None)) 
           )
           .withColumn(
           "victim_gender", 
           when(lower(col("victim_gender")).startswith("m"), "male")
           .when(lower(col("victim_gender")).startswith("f"), "female")
           .otherwise(lit(None)) 
           )
           .withColumn(
           "reported_online",
           when(
           lower(col("reported_online")).startswith("y") | 
           lower(col("reported_online")).startswith("t") | 
           (col("reported_online") == "1"), 
           True
           )
           .when(
           lower(col("reported_online")).startswith("n") | 
           lower(col("reported_online")).startswith("f") | 
           (col("reported_online") == "0"), 
           False
           )
           .otherwise(None)
           )
           .withColumn(
           "severity",
           when(
           (col("severity") == "1") | lower(col("severity")).startswith("l"), 
           "Low")            
           .when(
           (col("severity") == "2") | lower(col("severity")).startswith("m"), 
           "Medium"
           )
           .when(
           (col("severity") == "3") | lower(col("severity")).startswith("h"), 
           "High"
           )
           .when(
           (col("severity") == "4") | lower(col("severity")).startswith("c"), 
           "Critical"
           )
           .otherwise(lit(None))
           )
           .withColumn("district", initcap(col("district")))
           .withColumn("officer_first_name", initcap(col("officer_first_name")))
           .withColumn("officer_last_name", initcap(col("officer_last_name")))
           .withColumn("suspect_first_name", initcap(col("suspect_first_name")))
           .withColumn("suspect_last_name", initcap(col("suspect_last_name")))

           # CAST TYPES
           .withColumn("suspect_age", expr("TRY_CAST(suspect_age AS DOUBLE)").cast(IntegerType()))
           .withColumn("victim_age", expr("TRY_CAST(victim_age AS DOUBLE)").cast(IntegerType()))
           .withColumn("num_arrests", expr("TRY_CAST(num_arrests AS DOUBLE)").cast(IntegerType()))
           .withColumn("property_loss_usd", expr("TRY_CAST(property_loss_usd AS DOUBLE)"))           
           .withColumn(
                "incident_datetime", 
                coalesce(
                try_to_timestamp(col("incident_datetime"), lit("yyyy-MM-dd HH:mm:ss")),
                try_to_timestamp(col("incident_datetime"), lit("dd/MM/yyyy HH:mm")),
                try_to_timestamp(col("incident_datetime"), lit("dd/MM/yyyy HH:mm:ss")),
                try_to_timestamp(col("incident_datetime"), lit("MM/dd/yyyy HH:mm:ss")),       
                try_to_timestamp(col("incident_datetime"), lit("dd-MM-yyyy")),
                try_to_timestamp(col("incident_datetime"), lit("yyyy-MM-dd")),
                try_to_timestamp(col("incident_datetime"), lit("dd/MM/yyyy")),
                try_to_timestamp(col("incident_datetime"), lit("MM/dd/yyyy"))
            )
            )
                      
           
           # METADATA
           .withColumnRenamed("etl_processed_timestamp", "bronze_timestamp")
           .withColumn("silver_timestamp", current_timestamp())           
           .drop("etl_rec_uuid", "source_file", "file_modification_time", "latitude", "longitude")   # latitude & longitude are in city_table       
        )


dp.create_streaming_table(
    name=f"{catalog_name}.crime_silver.crime_streaming_silver",
    comment="transformed streaming data",
    table_properties={
        "quality": "silver",
        #"pipelines.reset.allowed": "false",  # preserves the data in the delta table if you do full refresh
        "delta.autoOptimize.optimizeWrite": "true"
    }
)    


dp.create_auto_cdc_flow(
    target=f"{catalog_name}.crime_silver.crime_streaming_silver",
    source="crime_silver_clean",
    keys=["incident_id"],
    sequence_by=col("bronze_timestamp"),
    stored_as_scd_type="1"
)

    