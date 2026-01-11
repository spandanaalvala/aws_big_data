import json
import urllib.request
import boto3
import os
from datetime import datetime

# Clients
kinesis = boto3.client("kinesis")
KINESIS_STREAM = os.environ["KINESIS_STREAM_NAME"]

LOCATIONS = {
    "London": (51.5074, -0.1278),
    "NewYork": (40.7128, -74.0060),
    "Mumbai": (19.0760, 72.8777)
}

def lambda_handler(event, context):
    processed_count = 0

    for city, (lat, lon) in LOCATIONS.items():
        # Changed to use the 'current' parameter instead of hourly ranges
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
        )

        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode("utf-8"))

        # Extract only the current data block
        current = data["current"]

        record = {
            "city": city,
            "timestamp": current["time"],
            "temperature": current["temperature_2m"],
            "humidity": current["relative_humidity_2m"],
            "wind_speed": current["wind_speed_10m"]
        }

        # Produce to Kinesis
        kinesis.put_record(
            StreamName=KINESIS_STREAM,
            Data=json.dumps(record),
            PartitionKey=city
        )
        processed_count += 1

    return {
        "status": "success",
        "records_sent": processed_count,
        "execution_time": datetime.utcnow().isoformat()
    }