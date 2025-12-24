from datetime import datetime
from typing import Any, Dict, List

from influxdb_client_3 import (
    InfluxDBClient3,
    InfluxDBError,
    Point,
    WriteOptions,
    WritePrecision,
    write_client_options,
)

import constants

host = constants.INFLUX.HOST
token = constants.INFLUX.TOKEN
database = constants.INFLUX.DB


def success(self, data: str):
    print("Successfully wrote batch")
    # print(f"Successfully wrote batch: data: {data}")


def error(self, data: str, exception: InfluxDBError):
    print(f"Failed writing batch: config: {self}, data: {data} due: {exception}")


def retry(self, data: str, exception: InfluxDBError):
    print(
        f"Failed retry writing batch: config: {self}, data: {data} retry: {exception}"
    )


write_options = WriteOptions(
    batch_size=500,
    flush_interval=10_000,
    jitter_interval=2_000,
    retry_interval=5_000,
    max_retries=5,
    max_retry_delay=30_000,
    exponential_base=2,
)

wco = write_client_options(
    success_callback=success,
    error_callback=error,
    retry_callback=retry,
    write_options=write_options,
)


def write(
    points: List[Point], write_precision: WritePrecision = WritePrecision.S
) -> None:
    with InfluxDBClient3(
        host=host, token=token, database=database, write_client_options=wco
    ) as client:
        client.write(points, write_precision=write_precision)


def wrap_point(
    measurement: str,
    *,
    tags: Dict[str, str],
    fields: Dict[str, Any],
    time: datetime | str,
) -> Point:
    point: Point = Point(measurement)
    for key, value in fields.items():
        point.field(key, value)

    for (
        key,
        value,
    ) in tags.items():
        point.tag(key, value)

    return point.time(time)


async def query(
    query: str,
    database: str,
    language: str = "influxql",
) -> object | Any:
    with InfluxDBClient3(
        host=host, token=token, database=database, write_client_options=wco
    ) as client:
        return await client.query_async(query, language, database=database)
