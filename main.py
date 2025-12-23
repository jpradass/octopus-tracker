import asyncio
from typing import Dict, List
import asyncclick as click

from api.octopus import OctopusAPI
from api.influx import InfluxAPI


async def run_app():
    # api: OctopusAPI = OctopusAPI()
    # accounts = await api.get_accounts()

    pass


@click.group()
def octopus():
    pass


@click.command()
def run():
    asyncio.run(run_app())


@click.command()
@click.option("--since", help="date since to fetch data")
@click.option("--until", help="date until to fetch data")
async def add_data(since: str, until: str):
    print(f"adding data since {since} until {until}")

    # "2025-12-18T00:00:00+01:00", "2025-12-19T00:00:00+01:00"
    octopus_api: OctopusAPI = OctopusAPI()
    influx_api: InfluxAPI = InfluxAPI()
    nodes: List[Dict] = await octopus_api.get_consumption_per_day(since, until)
    points: List = list()
    for node in nodes:
        real_node: Dict = node["node"]
        points.append(
            influx_api.wrap_point(
                "consumption",
                tags={"unit": "kwh"},
                fields={"kwh": float(real_node["value"])},
                time=real_node["startAt"],
            )
        )
        # print(real_node["startAt"], real_node["value"])

    influx_api.write(points)


octopus.add_command(run)
octopus.add_command(add_data)

if __name__ == "__main__":
    octopus()
