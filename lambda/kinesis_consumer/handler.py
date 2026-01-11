import json
import base64
import boto3
import os

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
sns = boto3.client("sns")

BUCKET = os.environ["S3_BUCKET"]
PREFIX = os.environ["S3_PREFIX"]
DLQ_URL = os.environ["DLQ_URL"]

ALERT_CITY = os.environ["ALERT_CITY"]
TEMP_THRESHOLD = float(os.environ["TEMP_THRESHOLD"])
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            payload = json.loads(
                base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            )

            city = payload.get("city")
            temperature = payload.get("temperature")
            ts = payload.get("timestamp")

            if not city or temperature is None or not ts:
                raise ValueError("Invalid schema")

            # ---- SNS alert condition ----
            if city == ALERT_CITY and temperature > TEMP_THRESHOLD:
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject="Weather Alert",
                    Message=f"{city} temperature {temperature}°C exceeded {TEMP_THRESHOLD}°C at {ts}"
                )

            s3_key = (
                f"{PREFIX}"
                f"year={ts[:4]}/month={ts[5:7]}/day={ts[8:10]}/"
                f"{city}_{ts}.json"
            )

            s3.put_object(
                Bucket=BUCKET,
                Key=s3_key,
                Body=json.dumps(payload)
            )

        except Exception as e:
            sqs.send_message(
                QueueUrl=DLQ_URL,
                MessageBody=json.dumps({"error": str(e), "record": record})
            )
