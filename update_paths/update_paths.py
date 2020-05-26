import datetime
import requests
import logging
import boto3
import json
import os
from decimal import Decimal
from typing import List, Dict, Tuple

TABLE_NAME = os.getenv("TABLE_NAME")
LAMETRO_API_BASE_URL = os.getenv("LAMETRO_API_BASE_URL")
ACTIVE_PATH_TIMEOUT_SEC = os.getenv("ACTIVE_PATH_TIMEOUT_SEC")
PING_REFRESH_RATE_SEC = os.getenv("PING_REFRESH_RATE_SEC")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

table = boto3.resource("dynamodb").Table(name=TABLE_NAME)


def lambda_handler(event: dict, context) -> dict:
    agency_id = event["pathParameters"]["agency_id"]
    paths = update_paths(
        agency_id
    )  # TODO: Rewrite without tuple key so json.dumps does not throw error
    return {
        "statusCode": "200",
        "headers": {"Content-type": "application/json"},
        "body": json.dumps(paths, default=float),
    }


def update_paths(agency_id: str) -> dict:
    pings = get_pings(agency_id)
    paths = get_paths(agency_id)
    active_paths, inactive_paths = append_pings(pings, paths)
    write(active_paths, inactive_paths, agency_id)
    return {"active_paths": active_paths, "inactive_paths": inactive_paths}


def write(
    active_paths: Dict[str, dict],
    inactive_paths: Dict[Tuple[str, str], List[dict]],
    agency_id: str,
) -> None:
    table.put_item(Item={"pk": agency_id, "sk": "paths$active", "paths": active_paths})
    for (pk, sk), paths in inactive_paths.items():
        table.update_item(
            Key={"pk": pk, "sk": sk},
            UpdateExpression="SET paths = list_append(if_not_exists(paths, :empty_list), :paths)",
            ExpressionAttributeValues={":empty_list": list(), ":paths": paths},
        )


def append_pings(
    pings: Dict[str, dict], paths: Dict[str, dict]
) -> (Dict[str, dict], Dict[Tuple[str, str], List[dict]]):
    active_paths, inactive_paths = dict(), dict()
    now = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())

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


def get_paths(agency_id: str) -> Dict[str, dict]:
    r = table.get_item(Key={"pk": agency_id, "sk": "paths$active"})
    paths = r.get("Item", {}).get("paths", {})
    return paths


def get_pings(agency_id: str) -> Dict[str, dict]:
    items = get_lametro_api_items(agency_id)
    pings = {
        i["id"]: make_ping(agency_id, i)
        for i in items
        if i["seconds_since_report"] < int(PING_REFRESH_RATE_SEC)
    }
    return pings


def get_lametro_api_items(agency_id: str) -> List[dict]:
    url = f"{LAMETRO_API_BASE_URL}/agencies/{agency_id}/vehicles/"
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


def make_ping(agency_id: str, item: dict) -> dict:
    route_id = item["route_id"]
    vehicle_id = item["id"]
    run_id = item.get("run_id", "NONE")  # some lametro pings have no run_id
    utc_ts, local_iso_date = get_utc_ts_and_local_iso_date(item["seconds_since_report"])
    coordinate = [
        Decimal(str(item["longitude"])),
        Decimal(str(item["latitude"])),
        Decimal(str(utc_ts)),
    ]
    ping = {
        "pk": f"{agency_id}${route_id}",
        "sk": f"paths${local_iso_date}",
        "agency_id": agency_id,
        "route_id": route_id,
        "run_id": run_id,
        "vehicle_id": vehicle_id,
        "local_iso_date": local_iso_date,
        "coordinates": [coordinate],
    }
    return ping


def get_utc_ts_and_local_iso_date(seconds_since_report: int) -> (int, str):
    local_dt = get_item_local_dt(seconds_since_report)
    local_iso_date = local_dt.date().isoformat()
    utc_ts = int(local_dt.timestamp())
    return utc_ts, local_iso_date


def get_item_local_dt(seconds_since_report: int) -> datetime.datetime:
    tz = datetime.timezone(datetime.timedelta(hours=-8), name="PST")
    dt = datetime.datetime.now(tz=tz) - datetime.timedelta(seconds=seconds_since_report)
    dst_start, dst_end = get_local_dst_start_stop_dts(dt.year, tz)
    if dst_start <= dt < dst_end:
        dt = dt.astimezone(datetime.timezone(datetime.timedelta(hours=-7), name="PDT"))
    return dt


def get_local_dst_start_stop_dts(
    year: int, tz: datetime.timezone
) -> (datetime.datetime, datetime.datetime):
    local_dst_start_dt = datetime.datetime.combine(
        date=first_isoweekday_of_month(year, month=3, isoweekday=7)
        + datetime.timedelta(weeks=1),
        time=datetime.time(2, 0, tzinfo=tz),
    )  # second Sunday of March at 2:00am standard time
    local_dst_stop_dt = datetime.datetime.combine(
        date=first_isoweekday_of_month(year, month=11, isoweekday=7),
        time=datetime.time(1, 0, tzinfo=tz),
    )  # first Sunday of November at 1:00am standard time (2:00am daylight time)
    return local_dst_start_dt, local_dst_stop_dt


def first_isoweekday_of_month(year: int, month: int, isoweekday: int) -> datetime.date:
    dt = datetime.date(year, month, 1)
    while dt.isoweekday() != isoweekday:
        dt += datetime.timedelta(days=1)
    return dt
