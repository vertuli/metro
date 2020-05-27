from datetime import date, time, datetime, timezone, timedelta
import uuid
import requests
import logging
import boto3
import json
import os
from typing import List

QUEUE_URL = os.getenv("QUEUE_URL")
LAMETRO_API_BASE_URL = os.getenv("LAMETRO_API_BASE_URL")
ACTIVE_PATH_TIMEOUT_SEC = os.getenv("ACTIVE_PATH_TIMEOUT_SEC")
PING_REFRESH_RATE_SEC = os.getenv("PING_REFRESH_RATE_SEC")
MAX_BATCH_SIZE = 10

PST_TZ = timezone(timedelta(hours=-8), name="PST")
PDT_TZ = timezone(timedelta(hours=-7), name="PDT")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

queue = boto3.resource("sqs").Queue(url=QUEUE_URL)


def lambda_handler(event: dict, context) -> dict:
    agency_id = event["pathParameters"]["agency_id"]
    responses = get_pings(agency_id)
    return {
        "statusCode": "200",
        "headers": {"Content-type": "application/json"},
        "body": json.dumps(responses),
    }


def get_pings(agency_id: str) -> List[dict]:
    url = f"{LAMETRO_API_BASE_URL}/agencies/{agency_id}/vehicles/"
    items = request_items(url)
    msgs = [make_msg(item, agency_id) for item in items]
    responses = write_sqs(msgs)
    return responses


def write_sqs(msgs: List[dict]) -> List[dict]:
    responses = []
    batches = [
        msgs[x : x + MAX_BATCH_SIZE] for x in range(0, len(msgs), MAX_BATCH_SIZE)
    ]
    for batch in batches:
        r = queue.send_messages(Entries=batch)
        logger.debug(r)
        responses.append(r)
    return responses


# TODO: Use Lambda Layers to share this code between functions.
def request_items(url: str) -> List[dict]:
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
    logger.debug(items)
    return items


def make_msg(item: dict, agency_id: str) -> dict:
    if agency_id in ["lametro", "lametro-rail"]:
        ping = parse_lametro_item(item, agency_id)
    else:
        logger.error(f"Cannot parse pings from agency {agency_id}!")
        raise NameError
    msg_dedup_id = f"{ping['agency_id']}:{ping['vehicle_id']}:{str(ping['ts'] % PING_REFRESH_RATE_SEC)}"
    coordinate = [ping.pop("lng"), ping.pop("lat"), ping.pop("ts")]
    msg_attrs = {k: {"DataType": "string", "StringValue": v} for k, v in ping.items()}
    msg = {
        "Id": uuid.uuid4(),
        "MessageBody": json.dumps(coordinate),
        "MessageAttributes": msg_attrs,
        "MessageGroupId": agency_id,
        "MessageDeduplicationId": msg_dedup_id,
    }
    return msg


def parse_lametro_item(item: dict, agency_id: str) -> dict:
    utc_dt = datetime.now(tz=timezone.utc) - timedelta(
        seconds=item["seconds_since_report"]
    )
    local_dt = get_local_dt(utc_dt=utc_dt, std_tz=PST_TZ, dst_tz=PDT_TZ)
    ping = {
        "agency_id": agency_id,
        "route_id": item["route_id"],
        "vehicle_id": item["id"],
        "run_id": item.get("run_id", "NONE"),  # some lametro pings have no run_id
        "local_iso_date": local_dt.date().isoformat(),
        "lng": item["longitude"],
        "lat": item["latitude"],
        "ts": utc_dt.timestamp(),
    }
    return ping


def get_local_dt(utc_dt: datetime, std_tz: timezone, dst_tz: timezone) -> datetime:
    local_std_dt = utc_dt.astimezone(std_tz)
    local_dst_start, local_dst_end = get_local_dst_start_stop_dts(local_std_dt)
    if local_dst_start <= local_std_dt < local_dst_end:
        local_dt = local_std_dt.astimezone(dst_tz)
    else:
        local_dt = local_std_dt
    return local_dt


def get_local_dst_start_stop_dts(local_std_dt: datetime) -> (datetime, datetime):
    # DST starts second Sunday of March at 02:00 standard time -> 03:00 daylight time
    dst_start_dt = datetime.combine(
        date=first_weekday_of_month(local_std_dt.year, month=3, iso_weekday=7)
        + timedelta(weeks=1),
        time=time(2, 0, tzinfo=local_std_dt.tzinfo),
    )
    # DST ends first Sunday of November at 02:00 daylight time -> 01:00 standard time
    dst_stop_dt = datetime.combine(
        date=first_weekday_of_month(local_std_dt.year, month=11, iso_weekday=7),
        time=time(1, 0, tzinfo=local_std_dt.tzinfo),
    )
    return dst_start_dt, dst_stop_dt


def first_weekday_of_month(year: int, month: int, iso_weekday: int) -> date:
    dt = date(year, month, 1)
    while dt.isoweekday() != iso_weekday:
        dt += timedelta(days=1)
    return dt
