import requests
import logging
import boto3
import json
import os
from typing import List, Dict

TABLE_NAME = os.getenv("TABLE_NAME")
LAMETRO_API_BASE_URL = os.getenv("LAMETRO_API_BASE_URL")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

table = boto3.resource("dynamodb").Table(name=TABLE_NAME)


def lambda_handler(event: dict, context) -> dict:
    agency_id = event["pathParameters"]["agency_id"]
    routes = update_routes(agency_id)
    return {
        "statusCode": "200",
        "headers": {"Content-type": "application/json"},
        "body": json.dumps(routes, default=float),
    }


def update_routes(agency_id: str) -> Dict[str, dict]:
    items = get_lametro_api_items(agency_id)
    routes = {
        item["id"]: {"route_id": item["id"], "name": item["display_name"]}
        for item in items
    }
    table.put_item(Item={"pk": agency_id, "sk": "routes", "routes": routes})
    return routes


def get_lametro_api_items(agency_id: str) -> List[dict]:
    url = f"{LAMETRO_API_BASE_URL}/agencies/{agency_id}/routes/"
    try:
        r = requests.get(url).json()
    except requests.RequestException as e:
        logger.error(e)
        raise e
    except json.decoder.JSONDecodeError as e:
        logger.error(e)
        raise e
    items = r.get("items", list())
    logger.debug(f"{len(items)} items read from {url}.")
    return items
