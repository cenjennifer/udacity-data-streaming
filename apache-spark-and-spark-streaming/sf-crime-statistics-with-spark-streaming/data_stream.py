import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import time

# DONE Create a schema for incoming resources
# Reference: https://classroom.udacity.com/nanodegrees/nd029/parts/d3d2cbfa-dc05-44c3-8db8-84e1d931170d/modules/d0276127-727a-4339-aacd-abb732149883/lessons/952320e3-fbe2-44fa-ab23-2acc8ce14e68/concepts/4e9c4c4e-078c-4e75-aaa0-c7e81c5151f0
# lesson 3: unit 16 establishing schema
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):
    spark.sparkContext.setLogLevel("WARN")
    # DONE Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    
    # Refer to: https://classroom.udacity.com/nanodegrees/nd029/parts/d3d2cbfa-dc05-44c3-8db8-84e1d931170d/modules/d0276127-727a-4339-aacd-abb732149883/lessons/233d13c1-2510-4e7a-9852-26491ae740b0/concepts/0d1cfe0c-b222-4837-878c-8753a5398103
    # Lesson 5: Unit 3 kafka darta source API
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.spark.sf.policecalls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetPerTrigger",200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # DONE: extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    service_table.printSchema()


    # DONE select original_crime_type_name and disposition
   
    distinct_table = service_table \
            .select('original_crime_type_name', 'disposition', 'call_date_time') \
            .distinct()
            # Question: What does watermark do here?

    distinct_table.printSchema()
    
    # count the number of original crime type
    agg_df = distinct_table \
          .select("original_crime_type_name", "call_date_time") \
          .withWatermark("call_date_time", "30 minutes") \
          .groupBy(psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"), distinct_table.original_crime_type_name) \
          .count()
    

    # DONE Q1. Submit a screen shot of a batch ingestion of the aggregation
    # DONE write output stream
    
    # Reference: Lesson 5: Unit 8 Progress Reports in Sparks Console
    query = agg_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="30 seconds") \
        .start()

    # DONE attach a ProgressReporter
    query.awaitTermination()

    # DONE get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    radio_code_df.printSchema()

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # DONE rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # DONE join on disposition column
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition, "inner")

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # DONE Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
