from typing import Dict, List

import asyncclick as click
import pendulum

import api.influx as influx_api
import api.influxv2 as influx_apiv2
import constants
from api.octopus import OctopusAPI


@click.group()
def octopus():
    pass


@click.command()
async def run():
    """main command that gets data from 5 days now"""
    now: pendulum.DateTime = pendulum.today(constants.TZ)
    five_d: pendulum.DateTime = now.subtract(days=5)
    four_d: pendulum.DateTime = now.subtract(days=4)
    # print(five_d.to_iso8601_string(), four_d.to_iso8601_string())

    print(
        f"adding data since {five_d.to_iso8601_string()} until {four_d.to_iso8601_string()}"
    )
    octopus_api: OctopusAPI = OctopusAPI()
    nodes: List[Dict] = await octopus_api.get_consumption_per_hour(
        five_d.to_iso8601_string(), four_d.to_iso8601_string()
    )
    points: List = await convert_nodes(nodes)
    influx_api.write(points)


@click.command()
@click.option("--since", help="date since to fetch data")
@click.option("--until", help="date until to fetch data")
async def add_data(since: str, until: str):
    """adds data getting it from octopus API and adding it into influx from a date until another one"""
    print(f"adding data since {since} until {until}")

    octopus_api: OctopusAPI = OctopusAPI()

    nodes: List[Dict] = await octopus_api.get_consumption_per_hour(since, until)
    points: List = await convert_nodes(nodes)
    influx_api.write(points)


@click.command()
async def get_account_info():
    """gets account info"""
    octopus_api: OctopusAPI = OctopusAPI()
    await octopus_api.get_account_info(constants.OCTOPUS.ACCOUNT_ID)


octopus.add_command(run)
octopus.add_command(add_data)
octopus.add_command(get_account_info)


async def convert_nodes(nodes: List[Dict]) -> List:
    points: List = list()

    for node in nodes:
        real_node: Dict = node["node"]
        points.append(
            influx_api.wrap_point(
                "consumption",
                tags={"unit": "kwh"},
                fields={
                    "consumption": float(real_node["value"]),
                    "power": constants.BILLED_POWER,
                },
                time=real_node["startAt"],
            ) if constants.INFLUX.VERSION == "3" else
            influx_apiv2.wrap_point(
                "consumption",
                tags={"unit": "kwh"},
                fields={
                    "consumption": float(real_node["value"]),
                    "power": constants.BILLED_POWER,
                },
                time=real_node["startAt"],
            )
        )

    return points


if __name__ == "__main__":
    octopus()
