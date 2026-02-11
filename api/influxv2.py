from datetime import datetime
from typing import Dict, List

import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import ASYNCHRONOUS

import constants

host: str = constants.INFLUX.HOST
token: str = constants.INFLUX.TOKEN
db: str = constants.INFLUX.DB
org: str = "personal"

def write(
        points: List[Point]
) -> None:
    with influxdb_client.InfluxDBClient(
        url=host,
        token=token,
        org=org,
    ) as client:
        client.write_api(write_options=ASYNCHRONOUS).write(bucket=db, org=org, record=points)

def wrap_point(
    measurement: str,
    *,
        tags: Dict[str, str],
    fields: Dict[str, str],
    time: datetime | str,
) -> Point:
    point: Point = Point(measurement)
    for key, value in fields.items():
        point.field(key, value)

    for key, value in tags.items():
        point.tag(key, value)

    return point.time(time)

