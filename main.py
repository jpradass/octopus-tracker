from typing import Dict, List

import asyncclick as click
import pendulum

import api.influx as influx_api
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
    await fetch_and_write(since, until)


@click.command()
async def serve():
    """runs the service in an infinite loop"""
    import asyncio
    import os

    load_frequency_hours = int(os.getenv("LOAD_FREQUENCY_HOURS", 6))
    days_offset_load = int(os.getenv("DAYS_OFFSET_LOAD", 0))

    print(f"Starting service. Frequency: {load_frequency_hours}h. Startup backfill: {days_offset_load} days.")

    # Startup backfill logic
    now = pendulum.now(constants.TZ)
    if days_offset_load > 0:
        start_time = now.subtract(days=days_offset_load)
        print(f"Performing startup backfill from {start_time}")
        await fetch_and_write(start_time.to_iso8601_string(), now.to_iso8601_string())
    else:
        # If no explicit backfill, we might still want to ensure we have recent data
        # Let's just fetch the last period to be safe (e.g. 2 * frequency)
        start_time = now.subtract(hours=load_frequency_hours * 2)
        print(f"Performing startup check from {start_time}")
        await fetch_and_write(start_time.to_iso8601_string(), now.to_iso8601_string())

    while True:
        print(f"Sleeping for {load_frequency_hours} hours...")
        await asyncio.sleep(load_frequency_hours * 3600)
        
        now = pendulum.now(constants.TZ)
        # Fetch slightly more than frequency to ensure no gaps
        start_time = now.subtract(hours=load_frequency_hours + 1)
        print(f"Waking up. Fetching data from {start_time}")
        try:
            await fetch_and_write(start_time.to_iso8601_string(), now.to_iso8601_string())
        except Exception as e:
            print(f"Error in service loop: {e}")


@click.command()
async def get_account_info():
    """gets account info"""
    octopus_api: OctopusAPI = OctopusAPI()
    await octopus_api.get_account_info(constants.OCTOPUS.ACCOUNT_ID)


octopus.add_command(run)
octopus.add_command(add_data)
octopus.add_command(get_account_info)
octopus.add_command(serve)


async def fetch_and_write(since: str, until: str):
    octopus_api: OctopusAPI = OctopusAPI()
    
    print(f"Fetching consumption from {since} to {until}")
    nodes: List[Dict] = await octopus_api.get_consumption_per_hour(since, until)
    if not nodes:
        print("No data found.")
        return

    # Check for errors in response (octopus.py returns errors dict if found)
    if isinstance(nodes, dict) and "errors" in nodes: # though type hint says List[Dict], implementation returns errors dict if error
        print(f"Error fetching data: {nodes}")
        return

    # Smart backfill: Filter out existing points
    # Need to parse since/until to datetime for query
    try:
        since_dt = pendulum.parse(since)
        until_dt = pendulum.parse(until)
        
        existing_timestamps = await influx_api.get_existing_timestamps(
            "consumption", since_dt, until_dt
        )
        # existing_timestamps are python datetime objects (likely naive or utc depending on influx client)
        # We need to be careful with timezone matching.
        # Let's normalize everything to string ISO check or timestamp comparison?
        # InfluxDB returned timestamps are usually UTC. 
        # The Octopus data 'startAt' is an ISO string.
        
        # Helper set for fast lookup - convert to ISO format string
        # We'll normalize to UTC ISO strings for comparison just to be safe, 
        # or stick to the string format from Octopus if it matches Influx.
        # Let's inspect what Influx returns in a real run... but we can't.
        # Best bet: convert Influx timestamps to ISO strings.
        
        existing_set = set()
        for ts in existing_timestamps:
            # ts is datetime. 
            # If it has timezone info, convert to ISO.
            # Octopus returns "2023-10-27T00:00:00+02:00"
            existing_set.add(pendulum.instance(ts).to_iso8601_string())

        new_nodes = []
        for node in nodes:
            real_node = node["node"]
            start_at = real_node["startAt"]
            # We might need to handle slight format diffs (e.g. Z vs +00:00)
            # A robust way is to parse `startAt` to datetime and compare
            node_dt = pendulum.parse(start_at)
            
            # Check if this timestamp exists in existing_set (fuzzy match might be needed if precision differs)
            # For now let's try exact match on seconds precision via pendulum comparison
            is_present = False
            for existing_ts in existing_timestamps:
                 # Influx stored time vs Octopus time
                 # Using pendulum for comparison handles timezones well
                 if pendulum.instance(existing_ts).diff(node_dt).in_seconds() == 0:
                     is_present = True
                     break
            
            if not is_present:
                new_nodes.append(node)
        
        print(f"Found {len(nodes)} points. {len(nodes) - len(new_nodes)} already exist. Writing {len(new_nodes)} new points.")
        nodes = new_nodes

    except Exception as e:
        print(f"Error during smart filtering, attempting to write all (Influx might dedupe if point logic is same): {e}")

    if not nodes:
        return

    points: List = await convert_nodes(nodes)
    influx_api.write(points)


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
            )
        )

    return points


if __name__ == "__main__":
    octopus()
