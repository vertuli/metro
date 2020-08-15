from datetime import datetime, timezone
import logging
import boto3
import json
import os
from decimal import Decimal
from typing import List, Dict, Tuple
from itertools import groupby

TABLE_NAME = os.getenv("TABLE_NAME")
LAMETRO_API_BASE_URL = os.getenv("LAMETRO_API_BASE_URL")
ACTIVE_PATH_TIMEOUT_SEC = os.getenv("ACTIVE_PATH_TIMEOUT_SEC")
PING_REFRESH_RATE_SEC = os.getenv("PING_REFRESH_RATE_SEC")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

table = boto3.resource("dynamodb").Table(name=TABLE_NAME)


def lambda_handler(event: dict, context) -> dict:
    pings = parse_event(event)
    paths = update_paths(pings)
    # TODO: Rewrite without tuple key so json.dumps does not throw error
    return {
        "statusCode": "200",
        "headers": {"Content-type": "application/json"},
        "body": json.dumps(paths, default=float),
    }


def parse_event(event: dict) -> List[dict]:
    pings = [
        {**r["messageAttributes"], **json.loads(r["body"], parse_float=Decimal)}
        for r in event["Records"]
    ]
    return pings


def update_paths(pings: List[dict]) -> list:
    responses = []
    pings = sorted(pings, key=lambda p: p["agency_id"])
    for agency_id, group in groupby(pings, key=lambda p: p["agency_id"]):
        r = update_agency_paths(group, agency_id)
        responses.append(r)
    return responses


def update_agency_paths(pings: List[dict], agency_id: str) -> list:
    paths = get_active_paths(agency_id)
    active_paths, completed_paths = append_pings(pings, paths)
    r = write(active_paths, completed_paths, agency_id)
    return r


def write(
    active_paths: Dict[str, dict],
    inactive_paths: Dict[Tuple[str, str], List[dict]],
    agency_id: str,
) -> List[dict]:
    responses = []
    r = table.put_item(
        Item={"pk": agency_id, "sk": "paths$active", "paths": active_paths}
    )
    responses.append(r)
    for (pk, sk), paths in inactive_paths.items():
        r = table.update_item(
            Key={"pk": pk, "sk": sk},
            UpdateExpression="SET paths = list_append(if_not_exists(paths, :empty_list), :paths)",
            ExpressionAttributeValues={":empty_list": list(), ":paths": paths},
        )
        responses.append(r)
    return responses

"""
table.update_item(
    Key={"pk": f"{agency_id}${route_id}", "sk": f"paths${local_iso_date}"},
    UpdateExpression="SET #vehicle_id_paths = list_append(:coord, if_not_exists(#vehicle_id_paths, :empty_list))",
    ExpressionAttributeNames={"#vehicle_id_paths": f"paths${vehicle_id}"}, 
    ExpressionAttributeValues={":empty_list": list(), ":coord": [[lng, lat, 0, ts]]},
)
"""

def append_pings(
    pings: Dict[str, dict], paths: Dict[str, dict]
) -> (Dict[str, dict], Dict[Tuple[str, str], List[dict]]):

    active_paths, inactive_paths = dict(), dict()
    now = int(datetime.now(tz=timezone.utc).timestamp())

    for vehicle_id, ping in pings.items():
        # ignore lng=0, lat=0 pings sometimes erroneously reported by lametro:
        if ping["coordinates"][0] == 0 and ping["coordinates"][1] == 0:
            continue

        if vehicle_id not in paths:
            path = ping
        else:
            path = paths.pop(vehicle_id)
            if (
                path["run_id"] == ping["run_id"]
                and path["route_id"] == ping["route_id"]
                and path_is_active(path, now)
            ):
                path["coordinates"] += ping["coordinates"]
            else:
                key = get_path_key(path)
                inactive_paths[key] = inactive_paths.get(key, []) + [path]
                path = ping
        active_paths[vehicle_id] = path

    for vehicle_id, path in paths.items():
        if path_is_active(path, now):
            active_paths[vehicle_id] = path
        else:
            key = get_path_key(path)
            inactive_paths[key] = inactive_paths.get(key, []) + [path]
    return active_paths, inactive_paths


def get_path_key(path: dict) -> (str, str):
    return f"{path['agency_id']}${path['route_id']}", f"paths${path['local_iso_date']}"


def path_is_active(path: dict, now: int) -> bool:
    last_coordinate = path["coordinates"][-1]
    last_ts = last_coordinate[-1]
    return last_ts + int(ACTIVE_PATH_TIMEOUT_SEC) > now


def get_active_paths(agency_id: str) -> Dict[str, dict]:
    r = table.get_item(Key={"pk": agency_id, "sk": "paths$active"})
    paths = r.get("Item", {}).get("paths", {})
    return paths
