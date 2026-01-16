import os
import logging
from typing import Dict, List

from python_graphql_client import GraphqlClient
import constants

logger = logging.getLogger(__name__)



class OctopusAPI:
    def __init__(self) -> None:
        self._token: str = None

    async def login(self) -> None:
        client = GraphqlClient(endpoint=constants.OCTOPUS.GRAPHQL_URL)
        response = await client.execute_async(
            constants.OCTOPUS.TOKEN_MUTATION,
            {
                "input": {
                    "email": os.getenv("OCTOPUS_USER"),
                    "password": os.getenv("OCTOPUS_PASS"),
                }
            },
        )

        if "errors" in response:
            logger.error(f"Login errors: {response['errors']}")
            print("there was some errors", response["errors"])

        self._token = response["data"]["obtainKrakenToken"]["token"]
        logger.info("Successfully obtained Kraken token")

    async def get_accounts(self):
        if not self._token:
            await self.login()

        client = GraphqlClient(
            endpoint=constants.OCTOPUS.GRAPHQL_URL, headers={"authorization": self._token}
        )
        response = await client.execute_async(constants.OCTOPUS.ACCOUNTS_QUERY)

        if "errors" in response:
            return response["errors"]

        return response["data"]["viewer"]["accounts"]

    async def get_consumption_per_hour(self, start: str, end: str) -> List[Dict]:
        if not self._token:
            await self.login()

        client = GraphqlClient(
            endpoint=constants.OCTOPUS.GRAPHQL_URL, headers={"authorization": self._token}
        )
        logger.info(f"Requesting consumption from {start} to {end}")
        response = await client.execute_async(
            constants.OCTOPUS.CONSUMPTION_QUERY,
            {
                "propertyId": constants.OCTOPUS.PROPERTY_ID,
                "first": 500,
                "utilityFilters": [
                    {
                        "electricityFilters": {
                            "readingDirection": "CONSUMPTION",
                            "readingFrequencyType": "HOUR_INTERVAL",
                        }
                    }
                ],
                "timezone": constants.TZ,
                "startAt": start,
                "endAt": end,
            },
            "getAccountMeasurements"
        )

        if "errors" in response:
            return response["errors"]
        
        return response["data"]["property"]["measurements"]["edges"]
    
    async def get_account_info(self, account: str):
        if not self._token:
            await self.login()

        client = GraphqlClient(
            endpoint=constants.OCTOPUS.GRAPHQL_URL, headers={"authorization": self._token}
        )

        response = await client.execute_async(
            constants.OCTOPUS.ACCOUNT_INFO_QUERY,
            {
                "account": account,
            },
            "accountBillingInfo"
        )

        if "errors" in response:
            logger.error(f"Error getting account info: {response['errors']}")
            return response["errors"]
        
        logger.info(f"Got account info: {response}")
        # print(response)
