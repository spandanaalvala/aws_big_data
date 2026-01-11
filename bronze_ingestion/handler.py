import json
import urllib.request
import boto3
import os
import random
from datetime import date, timedelta, datetime

s3 = boto3.client("s3")
kinesis = boto3.client("kinesis")
sqs = boto3.client("sqs")
sns = boto3.client("sns")

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_PREFIX = os.environ.get("S3_PREFIX")
KINESIS_STREAM = os.environ.get("KINESIS_STREAM_NAME")
DLQ_URL = os.environ.get("DLQ_URL")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
SOURCE_TYPE = os.environ.get("SOURCE_TYPE")  # full | streaming


def fetch_weather(city, lat, lon, start_date, end_date):
    url = (
        f"https://archive-api.open-meteo.com/v1/era5"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    )

    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode("utf-8"))

    records = []
    for i, ts in enumerate(data["hourly"]["time"]):
        record = {
            "city": city,
            "timestamp": ts,
            "temperature": data["hourly"]["temperature_2m"][i],
            "humidity": data["hourly"]["relative_humidity_2m"][i],
            "wind_speed": data["hourly"]["wind_speed_10m"][i]
        }
        records.append(record)

    return records


def corrupt_record(record):
    record.pop("temperature", None)
    return record


def lambda_handler(event, context):

    locations = {
        "London": (51.5074, -0.1278),
        "NewYork": (40.7128, -74.0060),
        "Mumbai": (19.0760, 72.8777)
    }

    start_date = date.today() - timedelta(weeks=8)
    end_date = date.today()

    all_records = []

    for city, (lat, lon) in locations.items():
        all_records.extend(
            fetch_weather(city, lat, lon, start_date, end_date)
        )

    for record in all_records:

        # 10% bad data (streaming only)
        if SOURCE_TYPE == "streaming" and random.random() < 0.1:
            sqs.send_message(
                QueueUrl=DLQ_URL,
                MessageBody=json.dumps(corrupt_record(record))
            )
            continue

        if SOURCE_TYPE == "full":
            key = (
                f"{S3_PREFIX}"
                f"year={record['timestamp'][:4]}/"
                f"month={record['timestamp'][5:7]}/"
                f"day={record['timestamp'][8:10]}/"
                f"{record['city']}_{record['timestamp']}.json"
            )

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(record)
            )

        else:
            kinesis.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(record),
                PartitionKey=record["city"]
            )

    return {
        "status": "ok",
        "records": len(all_records),
        "source_type": SOURCE_TYPE
    }
