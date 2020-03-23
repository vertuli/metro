import time
import requests
import boto3
import os
from decimal import Decimal
from typing import List
from collections import namedtuple

LAMETRO_API_URL = os.environ["LAMETRO_API_URL"]
PATHS_TABLE_NAME = os.environ["PATHS_TABLE_NAME"]
ACTIVE_VEHICLES_INDEX = os.environ["ACTIVE_VEHICLES_INDEX"]
STALE_EVENT_THRESHOLD_SEC = int(os.environ["STALE_EVENT_THRESHOLD_SEC"])
STALE_PATH_THRESHOLD_SEC = int(os.environ["STALE_PATH_THRESHOLD_SEC"])

dynamodb = boto3.resource("dynamodb")
paths_table = dynamodb.Table(PATHS_TABLE_NAME)


GroupKey = namedtuple("GroupKey", ["vehicle_id", "route_id", "run_id"])
Event = namedtuple(
    "Event", ["vehicle_id", "route_id", "run_id", "coordinate", "timestamp", "heading"]
)
Path = namedtuple(
    "Path",
    [
        "route_id",
        "start_ts",
        "active_vehicle_id",
        "vehicle_id",
        "run_id",
        "last_ts",
        "coordinates",
        "timestamps",
        "headings",
    ],
)
PathUpdate = namedtuple("PathUpdate", ["path", "event"])


def lambda_handler(event: dict, context: object) -> dict:
    access_ts = int(time.time())
    events, paths = read(access_ts)

    new_events = list(filter(lambda e: is_event_new(e, access_ts), events))
    active_paths, completed_paths = categorize_paths(paths, access_ts)
    active_path_updates, new_paths = assign_events_to_paths(active_paths, new_events)

    write(new_paths, active_path_updates, completed_paths)

    return {
        "statusCode": "200",
        "headers": {"Content-type": "application/json"},
        "body": {
            "access_ts": access_ts,
            "new_events_count": len(new_events),
            "new_paths_count": len(new_paths),
            "active_path_updates": len(active_path_updates),
            "completed_paths_count": len(completed_paths),
        },
    }


def read(access_ts: int) -> (List[Event], List[Path]):
    events = read_events(access_ts)
    paths = read_currently_active_paths()
    return events, paths


def read_events(access_ts: int) -> List[Event]:
    items = read_api()
    events = list(map(lambda item: event_from_item(item, access_ts), items))
    print(f"Created {len(events)} total events from retrieved items.")
    return events


def read_currently_active_paths() -> List[Path]:
    response = paths_table.scan(IndexName=ACTIVE_VEHICLES_INDEX)
    items = response.get("Items")
    paths = [Path(**item) for item in items]
    print(f"Read {len(paths)} active paths from DynamoDB.")
    return paths


def read_api() -> List[dict]:
    response = requests.get(LAMETRO_API_URL)
    if response.status_code != 200:
        raise ConnectionError(f"{LAMETRO_API_URL} returned code {response.status_code}")
    try:
        items = response.json()["items"]
    except (ValueError, KeyError) as e:
        raise e(f"{LAMETRO_API_URL} response not JSON w/items key.\n{response.content}")
    if not items:
        raise ValueError(f"{LAMETRO_API_URL} returned no items.\n{response.content}")
    print(f"Retrieved {len(items)} items from {LAMETRO_API_URL}.")
    return items


def categorize_paths(paths: List[Path], access_ts: int) -> (List[Path], List[Path]):
    active_paths = []
    completed_paths = []
    for path in paths:
        if is_path_still_active(path, access_ts):
            active_paths.append(path)
        else:
            completed_paths.append(path)
    return active_paths, completed_paths


def assign_events_to_paths(
    paths: List[Path], events: List[Event]
) -> (List[PathUpdate], List[Path]):
    path_updates = []
    new_paths = []
    vehicle_paths = {path.vehicle_id: path for path in paths}
    for event in events:
        path = vehicle_paths.get(event.vehicle_id)
        if path:
            path_update = PathUpdate(path=path, event=event)
            path_updates.append(path_update)
        else:
            new_path = new_path_from_event(event)
            new_paths.append(new_path)
    return path_updates, new_paths


def new_path_from_event(event: Event) -> Path:
    new_path = Path(
        vehicle_id=event.vehicle_id,
        start_ts=event.timestamp,
        active_vehicle_id=event.vehicle_id,
        route_id=event.route_id,
        run_id=event.run_id,
        last_ts=event.timestamp,
        coordinates=[event.coordinate],
        timestamps=[event.timestamp],
        headings=[event.heading],
    )
    return new_path


def is_path_still_active(path: Path, access_ts: int) -> bool:
    return (access_ts - path.timestamps[-1]) < STALE_PATH_THRESHOLD_SEC


def is_event_new(event: Event, access_ts: int) -> bool:
    return (access_ts - event.timestamp) < STALE_EVENT_THRESHOLD_SEC


def event_from_item(item: dict, access_ts: int) -> Event:
    # hack for boto float-type errors: https://github.com/boto/boto3/issues/665
    event = Event(
        vehicle_id=item["id"],
        route_id=item["route_id"],
        run_id=item.get("run_id", "NONE"),  # Some events have no run_id.
        coordinate=[Decimal(str(item["longitude"])), Decimal(str(item["latitude"]))],
        timestamp=access_ts - int(item["seconds_since_report"]),
        heading=Decimal(str(item["heading"])),
    )
    return event


def write(
    new_paths: List[Path],
    active_path_updates: List[PathUpdate],
    completed_paths: List[Path],
) -> None:
    update_completed_paths(completed_paths)
    update_active_paths(active_path_updates)
    put_new_active_paths(new_paths)
    return None


def update_completed_paths(paths: List[Path]) -> None:
    for path in paths:
        update_completed_path(path)
    return None


def update_completed_path(path: Path) -> None:
    key = {"route_id": path.route_id, "start_ts": path.start_ts}
    paths_table.update_item(Key=key, UpdateExpression="REMOVE active_vehicle_id")
    print(f"Path with key={key} marked inactive.")
    return None


def update_active_paths(updates: List[PathUpdate]) -> None:
    for update in updates:
        update_active_path(update)
    return None


def update_active_path(update: PathUpdate) -> None:
    key = {"route_id": update.path.route_id, "start_ts": update.path.start_ts}
    update_expr = "SET " + ", ".join(
        [
            "coordinates = list_append(coordinates, :new_coord)",
            "timestamps = list_append(timestamps, :new_timestamp)",
            "headings = list_append(headings, :new_heading)",
        ]
    )
    expr_attr_vals = {
        ":new_coord": [update.event.coordinate],
        ":new_timestamp": [update.event.timestamp],
        ":new_heading": [update.event.heading],
    }
    paths_table.update_item(
        Key=key, UpdateExpression=update_expr, ExpressionAttributeValues=expr_attr_vals
    )
    print(f"Active path with key={key} updated with event={update.event}.")
    return None


def put_new_active_paths(paths: List[Path]) -> None:
    for path in paths:
        put_new_active_path(path)
    return None


def put_new_active_path(path: Path) -> None:
    item = path._asdict()
    paths_table.put_item(Item=item)
    return None
