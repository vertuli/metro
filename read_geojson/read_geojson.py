from decimal import Decimal
import boto3
import os
import json
from typing import List, Optional

PATHS_TABLE_NAME = os.environ["PATHS_TABLE_NAME"]

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(PATHS_TABLE_NAME)


def lambda_handler(event: dict, context: object) -> dict:
    params = parse_query_string_params(event)
    paths = get_all_routes_paths(
        params["route_ids"], params["starts_after_ts"], params["starts_before_ts"]
    )
    geojson = make_geojson(paths)
    response = {
        "statusCode": "200",
        "headers": {"Content-type": "application/json"},
        "body": geojson,
    }
    return response


def parse_query_string_params(event: dict) -> dict:
    params = {
        "route_ids": event["multiValueQueryStringParameters"]["route_id"],
        "starts_after_ts": event["queryStringParameters"].get("starts_after_ts"),
        "starts_before_ts": event["queryStringParameters"].get("starts_before_ts"),
    }
    return params


def get_all_routes_paths(
    route_ids: List[str],
    starts_after_ts: Optional[str],
    starts_before_ts: Optional[str],
) -> List[dict]:
    paths = []
    for route_id in route_ids:
        paths += get_route_paths(route_id, starts_after_ts, starts_before_ts)
    return paths


def get_route_paths(
    route_id: str, starts_after_ts: Optional[str], starts_before_ts: Optional[str]
) -> List[dict]:
    expr = "route_id = :route_id"
    vals = {":route_id": route_id}
    if starts_after_ts and not starts_before_ts:
        expr += " AND start_ts >= :starts_after_ts"
        vals[":starts_after_ts"] = int(starts_after_ts)
    if starts_before_ts and not starts_after_ts:
        expr += " AND start_ts <= :starts_before_ts"
        vals[":starts_before_ts"] = int(starts_before_ts)
    if starts_after_ts and starts_before_ts:
        expr += " AND start_ts BETWEEN :starts_after_ts AND :starts_before_ts"
        vals[":starts_after_ts"] = int(starts_after_ts)
        vals[":starts_before_ts"] = int(starts_before_ts)
    response = table.query(KeyConditionExpression=expr, ExpressionAttributeValues=vals)
    paths = list(map(convert_decimals_to_floats, response["Items"]))
    return paths


def make_geojson(paths: List[dict]) -> str:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            make_feature(path) for path in paths if len(path["coordinates"]) > 1
        ],
    }
    return json.dumps(geojson)


def make_feature(path: dict) -> dict:
    coordinates = path.pop("coordinates")
    longitudes = [c[0] for c in coordinates]
    latitudes = [c[1] for c in coordinates]
    altitudes = [0] * len(coordinates)  # no altitude information available.
    timestamps = [int(ts) for ts in path.pop("timestamps")]
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": list(zip(longitudes, latitudes, altitudes, timestamps)),
        },
        "properties": {
            "run_id": path["run_id"],
            "vehicle_id": path["vehicle_id"],
            "route_id": path["route_id"],
        },
    }
    return feature


def convert_decimals_to_floats(d: dict) -> dict:
    for k, v in d.items():
        if k == "coordinates":
            d[k] = [[float(c[0]), float(c[1])] for c in v]
        if k in ["headings", "timestamps"]:
            d[k] = [float(i) for i in v]
        if isinstance(v, Decimal):
            d[k] = float(v)
        if isinstance(v, dict):
            d[k] = convert_decimals_to_floats(v)
    return d
