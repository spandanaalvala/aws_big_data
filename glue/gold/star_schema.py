import sys
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    monotonically_increasing_id
)

# --------------------
# Glue setup (CORRECT)
# --------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------
# Paths
# --------------------
silver_path = "s3://spandweather-dev-datalake/silverstaging/"
gold_base = "s3://spandweather-dev-datalake/goldcurated"

fact_path = f"{gold_base}/fact_weather/"
dim_time_path = f"{gold_base}/dim_time/"
dim_location_path = f"{gold_base}/dim_location/"
quarantine_path = f"{gold_base}/quarantine_fact_weather/"

# --------------------
# DynamoDB watermark
# --------------------
ddb = boto3.resource("dynamodb")
table = ddb.Table("spandweather-dev-watermark")

resp = table.get_item(
    Key={"pipeline_stage": "gold", "dataset": "weather"}
)

last_ts = (
    resp["Item"]["last_processed_ts"]
    if "Item" in resp
    else "1970-01-01T00:00:00Z"
)

# --------------------
# Read Silver incrementally
# --------------------
df = spark.read.parquet(silver_path)
df_inc = df.filter(col("processed_timestamp") > last_ts)

def run_job():
    df = spark.read.parquet(silver_path)
    df_inc = df.filter(col("processed_timestamp") > last_ts)

    if df_inc.count() == 0:
        print("No new Silver data to process")
        return

# --------------------
# DIM LOCATION
# --------------------
dim_location = (
    df_inc
    .select("city")
    .dropDuplicates()
    .withColumn("location_id", monotonically_increasing_id())
)

dim_location.write.mode("overwrite").parquet(dim_location_path)

# --------------------
# DIM TIME
# --------------------
dim_time = (
    df_inc
    .withColumn("event_ts", to_timestamp("timestamp"))
    .select(
        year("event_ts").alias("year"),
        month("event_ts").alias("month"),
        dayofmonth("event_ts").alias("day"),
        hour("event_ts").alias("hour")
    )
    .dropDuplicates()
    .withColumn("time_id", monotonically_increasing_id())
)

dim_time.write.mode("overwrite").parquet(dim_time_path)

# --------------------
# FACT WEATHER (STRICT)
# --------------------
fact_joined = (
    df_inc
    .join(dim_location, "city", "inner")
    .join(
        dim_time,
        (year(to_timestamp("timestamp")) == dim_time.year) &
        (month(to_timestamp("timestamp")) == dim_time.month) &
        (dayofmonth(to_timestamp("timestamp")) == dim_time.day) &
        (hour(to_timestamp("timestamp")) == dim_time.hour),
        "inner"
    )
)

# --------------------
# Quarantine bad records
# --------------------
bad_facts = df_inc.join(dim_location, "city", "left_anti")

if bad_facts.count() > 0:
    bad_facts.write.mode("append").parquet(quarantine_path)

# --------------------
# Final fact
# --------------------
fact_weather = (
    fact_joined
    .select(
        "location_id",
        "time_id",
        "temperature",
        "humidity",
        "wind_speed"
    )
    .dropna(subset=["location_id", "time_id"])
    .withColumn("weather_id", monotonically_increasing_id())
)

fact_weather.write.mode("append").parquet(fact_path)

# --------------------
# Update watermark
# --------------------
max_ts = df_inc.agg({"processed_timestamp": "max"}).collect()[0][0]

table.put_item(
    Item={
        "pipeline_stage": "gold",
        "dataset": "weather",
        "last_processed_ts": max_ts.isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
)

run_job()
job.commit()

