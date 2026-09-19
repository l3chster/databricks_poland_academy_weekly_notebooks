from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

catalog_name = spark.conf.get("crime.ingestion.catalog_name")

validations = {
    "population is positive": "population > 0", 
    "population is not null": "population IS NOT NULL",
    "city is not null": "city IS NOT NULL",
    "longitude is not null": "longitude IS NOT NULL",
    "latitude is not null": "latitude IS NOT NULL",
    }

@dp.materialized_view(
    name=f"{catalog_name}.crime_silver.crime_cities_silver",
    comment="transformed batch data",
    table_properties={
        "quality": "silver"        
    }
)
@dp.expect_all(validations)   
@dp.expect_or_drop("city_id checking", "city_id IS NOT NULL")                 
def crime_silver_clean():      

    return (
           spark.read.table(f"{catalog_name}.crime_bronze.crime_cities")
           .withColumnRenamed("etl_processed_timestamp", "bronze_timestamp")
           .withColumn("silver_timestamp", current_timestamp())           
           .drop("etl_rec_uuid", "source_file", "file_modification_time")       
        )    