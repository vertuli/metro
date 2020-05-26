from __future__ import annotations
import os
import json
import logging
from typing import List, Optional
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.getenv("TABLE_NAME")

table = boto3.resource("dynamodb").Table(name=TABLE_NAME)


# TODO: Split into three separate functions using code shared with Lambda Layers.
def lambda_handler(event: dict, context) -> dict:
    agency_id, iso_date, route_id = parse_event(event)
    if not iso_date:
        paths = get_active_paths(agency_id)
    elif not route_id:
        paths = get_all_paths_by_iso_date(agency_id, iso_date)
    else:
        paths = get_paths(agency_id, iso_date, route_id)
    geojson = make_geojson(paths)
    return {
        "statusCode": "200",
        "headers": {"Content-type": "application/json"},
        "body": json.dumps(geojson, default=float),
    }


def parse_event(event: dict) -> (str, Optional[str], Optional[str]):
    logger.debug(f"Parsing event: {event}")
    params = event["pathParameters"]
    return params.get("agency_id"), params.get("iso_date"), params.get("route_id")


def get_active_paths(agency_id: str) -> List[dict]:
    r = table.get_item(Key={"pk": agency_id, "sk": "paths$active"})
    paths = r.get("Item", {}).get("paths", {})
    return list(paths.values())


def get_all_paths_by_iso_date(agency_id: str, iso_date: str) -> List[dict]:
    r = table.get_item(Key={"pk": agency_id, "sk": "routes"})
    routes = r.get("Item", {}).get("routes", {})
    paths = [p for route_id in routes for p in get_paths(agency_id, iso_date, route_id)]
    logger.debug(f"{len(paths)} paths retrieved from {table.name}")
    return paths


def get_paths(agency_id: str, iso_date: str, route_id: str) -> List[dict]:
    r = table.get_item(Key={"pk": f"{agency_id}${route_id}", "sk": f"paths${iso_date}"})
    paths = r.get("Item", {}).get("paths", [])
    logger.debug(f"{len(paths)} paths retrieved from {table.name}")
    return paths


def make_geojson(paths: List[dict]) -> dict:
    features = []
    for path in paths:
        # convert from decimal to float/int and skip erroneous lng=0, lat=0 coordinates sometimes reported by lametro:
        raw_coords = path.pop("coordinates")
        coords = [
            [float(c[0]), float(c[1]), float(0), int(c[2])]
            for c in raw_coords
            if c[0] != 0 and c[1] != 0
        ]

        feature = {
            "type": "Feature",
            "properties": path,
            "geometry": {"type": "LineString", "coordinates": coords},
        }

        # Fix for kepler.gl bug - https://github.com/keplergl/kepler.gl/issues/1124
        if len(feature["geometry"]["coordinates"]) > 2:
            features.append(feature)
    feature_collection = {"type": "FeatureCollection", "features": features}
    return feature_collection
