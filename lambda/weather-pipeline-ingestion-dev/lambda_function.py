import json
import os
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

API_KEY = os.environ["OPEN_WEATHER_API_KEY"]
BUCKET = os.environ["S3_BRONZE_BUCKET"]
CITIES = [c.strip() for c in os.environ.get("WEATHER_CITIES", "London,Tashkent,Moscow,Tokyo").split(",") if c.strip()]
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
API_BASE = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather(city: str) -> dict:
    params = urlencode({
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    })
    url = f"{API_BASE}?{params}"
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))

def write_to_s3(data: dict, key: str) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2)
    s3_client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "openweather_api",
        },
    )

def send_alert(subject: str, message: str) -> None:
    if SNS_TOPIC:
        sns_client.publish(TopicArn=SNS_TOPIC, Subject=subject[:100], Message=message)


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")
    ingestion_id = now.strftime("%Y%m%d_%H%M%S")

    results = {"success": [], "failed": []}

    for city in CITIES:
        try:
            data = fetch_weather(city)

            data["_pipeline_metadata"] = {
                "ingestion_id": ingestion_id,
                "city_requested": city,
                "ingestion_timestamp": now.isoformat(),
                "source": "openweather_api",
            }

            city_slug = city.lower().replace(" ", "_")

            key = (
                f"weather/raw/"
                f"city={city_slug}/"
                f"date={date_partition}/"
                f"hour={hour_partition}/"
                f"{ingestion_id}.json"
            )

            write_to_s3(data, key)
            logger.info(f"OK {city} -> s3://{BUCKET}/{key}")
            results["success"].append(city)

        except HTTPError as e:
            body = e.read().decode("utf-8") if hasattr(e, "read") else ""
            logger.error(f"HTTP {e.code} for {city}: {body}")
            results["failed"].append({"city": city, "error": f"HTTP {e.code}"})
        except (URLError, Exception) as e:
            logger.error(f"Error for {city}: {e}")
            results["failed"].append({"city": city, "error": str(e)})

    summary = (f"Ingestion {ingestion_id}: "
            f"success {len(results["success"])}/{len(CITIES)}, "
            f"failed {len(results["failed"])}.")
    logger.info(summary)
    

    if results["failed"]:
        send_alert(
            subject=f"[Weather Pipeline] Ingestion issues - {ingestion_id}",
            message=json.dumps(results, indent=2, ensure_ascii=False)
        )

    return {
        "statusCode": 200,
        "ingestion_id": ingestion_id,
        "success_count": len(results["success"]),
        "failed_count": len(results["failed"]),
        "results": results,
    }