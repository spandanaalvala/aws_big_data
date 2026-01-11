import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import current_timestamp, lit

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# ---- Glue setup ----
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---- Paths ----
bucket = "spandweather-dev-datalake"
raw_base = f"s3://{bucket}/rawbronze/rawtoprocess/"
processed_path = f"s3://{bucket}/rawbronze/processed/"

full_load_path = raw_base + "full_load/"
streaming_path = raw_base + "streaming/"

# ---- Read BOTH sources ----
df_full = spark.read.option("recursiveFileLookup", "true").json(full_load_path)
df_stream = spark.read.option("recursiveFileLookup", "true").json(streaming_path)

df_full = df_full.withColumn("source_type", lit("full"))
df_stream = df_stream.withColumn("source_type", lit("streaming"))

df = df_full.unionByName(df_stream, allowMissingColumns=True)

# ---- Add processed timestamp ----
df_processed = df.withColumn("processed_timestamp", current_timestamp())

# ---- Write processed ----
df_processed.write.mode("append").parquet(processed_path)

# ---- CLEAN rawtoprocess AFTER SUCCESS ----
s3 = boto3.client("s3")

def delete_prefix(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in page["Contents"]]}
            )

# delete only after successful write
delete_prefix("rawbronze/rawtoprocess/full_load/")
delete_prefix("rawbronze/rawtoprocess/streaming/")

job.commit()
