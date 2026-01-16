import logging
import os
from typing import Dict, List

import asyncclick as click
import pendulum

import api.influx as influx_api
import constants
from api.octopus import OctopusAPI

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@click.group()
def octopus():
    pass


@click.command()
async def run():
    """main command that gets data from 5 days now"""
    influx_api.create_db_if_not_exists()
    now: pendulum.DateTime = pendulum.today(constants.TZ)
    five_d: pendulum.DateTime = now.subtract(days=5)
    four_d: pendulum.DateTime = now.subtract(days=4)
    # print(five_d.to_iso8601_string(), four_d.to_iso8601_string())

    logger.info(
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
    influx_api.create_db_if_not_exists()
    logger.info(f"adding data since {since} until {until}")
    await fetch_and_write(since, until)


@click.command()
async def serve():
    """runs the service in an infinite loop"""
    import asyncio

    load_frequency_hours = int(os.getenv("LOAD_FREQUENCY_HOURS", 6))
    days_offset_load = int(os.getenv("DAYS_OFFSET_LOAD", 0))
    reset_db = os.getenv("RESET_DB_ON_STARTUP", "false").lower() == "true"

    if reset_db:
        logger.warning("RESET_DB_ON_STARTUP is true. Deleting database...")
        influx_api.delete_db()
    
    influx_api.create_db_if_not_exists()

    logger.info(f"Starting service. Frequency: {load_frequency_hours}h. Startup backfill: {days_offset_load} days. Reset DB: {reset_db}")

    # Startup backfill logic
    now = pendulum.now(constants.TZ)
    
    # If we reset the DB, we MUST backfill from the offset if provided, or at least load something.
    # If reset_db is true and days_offset_load is 0, we might want to default to something or just rely on what is set.
    # Assuming user provides DAYS_OFFSET_LOAD if they want to backfill after reset.
    
    if days_offset_load > 0 or reset_db:
        # If resetting, we definitely want to load from the offset (even if it means 0 days if user set 0, effectively 0 backfill?)
        # But usually reset implies we want to reload history.
        # If days_offset_load is 0 but reset is True, let's assume we load some default or just nothing if that's what is configured.
        # However, code says `if days_offset_load > 0`. 
        # Requirement: "borrando todos los datos y cargando los datos desde la fecha que marca DAYS_OFFSET_LOAD".
        
        offset = days_offset_load if days_offset_load > 0 else 0
        if reset_db and offset == 0:
             logger.warning("Database reset but DAYS_OFFSET_LOAD is 0. No historical data will be loaded.")
        
        if offset > 0:
            start_time = now.subtract(days=offset)
            logger.info(f"Performing startup backfill from {start_time}")
            await fetch_and_write(start_time.to_iso8601_string(), now.to_iso8601_string())
    else:
        # If no explicit backfill and NO reset, check recent data
        start_time = now.subtract(hours=load_frequency_hours * 2)
        logger.info(f"Performing startup check from {start_time}")
        await fetch_and_write(start_time.to_iso8601_string(), now.to_iso8601_string())

    while True:
        logger.info(f"Sleeping for {load_frequency_hours} hours...")
        await asyncio.sleep(load_frequency_hours * 3600)
        
        now = pendulum.now(constants.TZ)
        start_time = now.subtract(hours=load_frequency_hours + 1)
        logger.info(f"Waking up. Fetching data from {start_time}")
        try:
            await fetch_and_write(start_time.to_iso8601_string(), now.to_iso8601_string())
        except Exception as e:
            logger.error(f"Error in service loop: {e}")


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
    
    logger.info(f"Fetching consumption from {since} to {until}")
    nodes: List[Dict] = await octopus_api.get_consumption_per_hour(since, until)
    if not nodes:
        logger.info("No data found.")
        return

    # Check for errors in response (octopus.py returns errors dict if found)
    if isinstance(nodes, dict) and "errors" in nodes: # though type hint says List[Dict], implementation returns errors dict if error
        logger.error(f"Error fetching data: {nodes}")
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
        
        if len(new_nodes) > 0:
            logger.info(f"Found {len(nodes)} points. {len(nodes) - len(new_nodes)} already exist. Writing {len(new_nodes)} new points.")
        else:
            logger.info(f"Found {len(nodes)} points. All already exist. Skipping write.")
        nodes = new_nodes

    except Exception as e:
        logger.error(f"Error during smart filtering, attempting to write all (Influx might dedupe if point logic is same): {e}")

    if not nodes:
        return

    points: List = await convert_nodes(nodes)
    influx_api.write(points)


def parse_time_config(time_str: str, default: pendulum.Time) -> pendulum.Time:
    try:
        h, m = map(int, time_str.split(":"))
        return pendulum.time(h, m)
    except Exception as e:
        logger.error(f"Error parsing time configuration '{time_str}': {e}. Using default {default}")
        return default


def is_time_in_range(check_time: pendulum.Time, start: pendulum.Time, end: pendulum.Time) -> bool:
    if start <= end:
        return start <= check_time < end
    else:
        # Crosses midnight
        return check_time >= start or check_time < end


async def convert_nodes(nodes: List[Dict]) -> List:
    points: List = list()

    # Parse sun hours config
    sun_start = parse_time_config(constants.SUN_HOURS_START, pendulum.time(12, 0))
    sun_end = parse_time_config(constants.SUN_HOURS_END, pendulum.time(18, 0))
    
    # Parse power peak config
    peak_start = parse_time_config(constants.POWER_PEAK_START, pendulum.time(8, 0))
    peak_end = parse_time_config(constants.POWER_PEAK_END, pendulum.time(0, 0))

    for node in nodes:
        real_node: Dict = node["node"]

        # Determine tariff based on time (Sun Club: defined by SUN_HOURS)
        dt = pendulum.parse(real_node["startAt"])
        # Ensure we check the hour in the user's configured timezone
        dt_local = dt.in_timezone(constants.TZ)
        point_time = dt_local.time()

        is_sun_hours = is_time_in_range(point_time, sun_start, sun_end)
        is_power_peak = is_time_in_range(point_time, peak_start, peak_end)
        
        tariff = "sun_club" if is_sun_hours else "standard"
        
        points.append(
            influx_api.wrap_point(
                "consumption",
                tags={"unit": "kwh", "tariff": tariff},
                fields={
                    "consumption": float(real_node["value"]),
                    "power": constants.BILLED_POWER,
                    "sun_hours": is_sun_hours,
                    "power_peak": is_power_peak,
                },
                time=real_node["startAt"],
            )
        )

    return points


if __name__ == "__main__":
    octopus()
