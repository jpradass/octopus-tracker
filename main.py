import asyncclick as click
import pendulum

import constants
from api.octopus import OctopusAPI


@click.group()
def octopus():
    pass


@click.command()
async def run():
    now: pendulum.DateTime = pendulum.today(constants.TZ)
    five_d: pendulum.DateTime = now.subtract(days=5)
    four_d: pendulum.DateTime = now.subtract(days=4)
    # print(five_d.to_iso8601_string(), four_d.to_iso8601_string())
    
    print(f"adding data since {five_d.to_iso8601_string()} until {four_d.to_iso8601_string()}")
    octopus_api: OctopusAPI = OctopusAPI()
    await octopus_api.add_data(five_d.to_iso8601_string(), four_d.to_iso8601_string())


@click.command()
@click.option("--since", help="date since to fetch data")
@click.option("--until", help="date until to fetch data")
async def add_data(since: str, until: str):
    print(f"adding data since {since} until {until}")
    octopus_api: OctopusAPI = OctopusAPI()
    await octopus_api.add_data(since, until)
    # "2025-12-18T00:00:00+01:00", "2025-12-19T00:00:00+01:00"


octopus.add_command(run)
octopus.add_command(add_data)

if __name__ == "__main__":
    octopus()
