import sys
import boto3
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsgluedq.transforms import EvaluateDataQuality

# ---- Glue setup ----
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---- Paths ----
bronze_processed_path = "s3://spandweather-dev-datalake/rawbronze/processed/"
silver_path = "s3://spandweather-dev-datalake/silverstaging/"

# ---- DynamoDB watermark ----
ddb = boto3.resource("dynamodb")
table = ddb.Table("spandweather-dev-watermark")

resp = table.get_item(
    Key={"pipeline_stage": "silver", "dataset": "weather"}
)

last_ts = (
    resp["Item"]["last_processed_ts"]
    if "Item" in resp
    else "1970-01-01T00:00:00Z"
)
def run_job():
    # ---- Read Bronze Processed ----
    df = spark.read.parquet(bronze_processed_path)

    # ---- Incremental filter ----
    df_inc = df.filter(col("processed_timestamp") > last_ts)

    if df_inc.count() == 0:
        print("No new data to process")
        return

    # ---- Clean ----
    df_clean = (
        df_inc
        .dropDuplicates(["city", "timestamp"])
        .filter(col("temperature").isNotNull())
    )

    # ---- Convert to DynamicFrame ----
    dyf = DynamicFrame.fromDF(df_clean, glueContext, "silver_dyf")

    dq_rules = """
    Rules = [
    IsComplete "city",
    IsComplete "timestamp",
    IsComplete "temperature",
    ColumnValues "temperature" between -60 and 60,
    ColumnValues "humidity" between 0 and 110
    ]
    """

    dq_results = EvaluateDataQuality.apply(
        frame=dyf,
        ruleset=dq_rules,
        publishing_options={
            "dataQualityEvaluationContext": "silver_dq_check",
            "enableDataQualityResultsPublishing": True
        }
    )


    dq_df = dq_results.toDF()
    failed_rules = dq_df.filter(col("Outcome") == "Failed").count()

    if failed_rules > 0:
        raise Exception("Data Quality checks failed")

    # ---- Write Silver ----
    df_clean.write.mode("append").parquet(silver_path)

    # ---- Update watermark ----
    max_ts = (
        df_clean
        .agg({"processed_timestamp": "max"})
        .collect()[0][0]
    )

    table.put_item(
        Item={
            "pipeline_stage": "silver",
            "dataset": "weather",
            "last_processed_ts": max_ts.isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
    )

# ---- Run & commit ----
run_job()
job.commit()
