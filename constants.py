__all__ = ["OCTOPUS", "INFLUX", "TZ", "BILLED_POWER"]

import os

BILLED_POWER: float = float(os.getenv("BILLED_POWER", "3.5"))
TZ: str = os.getenv("TZ", "Europe/Madrid")
SUN_HOURS_START: str = os.getenv("SUN_HOURS_START", "12:00")
SUN_HOURS_END: str = os.getenv("SUN_HOURS_END", "18:00")
POWER_PEAK_START: str = os.getenv("POWER_PEAK_START", "08:00")
POWER_PEAK_END: str = os.getenv("POWER_PEAK_END", "00:00")


class OCTOPUS:
    GRAPHQL_URL: str = os.getenv(
        "OCTOPUS_GRAPHQL_URL", "https://octopusenergy.es/api/graphql/kraken"
    )
    PROPERTY_ID: str = os.getenv("OCTOPUS_PROPERTY_ID", "629606")
    ACCOUNT_ID: str = os.getenv("ACCOUNT_ID", "A-0B377DCE")
    TOKEN_MUTATION = """
    mutation obtainKrakenToken($input: ObtainJSONWebTokenInput!) {
        obtainKrakenToken(input: $input) {
        token
        }
    }
    """

    ACCOUNTS_QUERY = """
    query getAccountNames {
        viewer {
            accounts {
                ... on Account {
                    number
                }
            }
        }
    }
    """

    ACCOUNT_INFO_QUERY = """
    query ($account: String!) {
        accountBillingInfo(accountNumber: $account) {
        ledgers {
            ledgerType
            statementsWithDetails(first: 1) {
            edges {
                node {
                amount
                consumptionStartDate
                consumptionEndDate
                issuedDate
                }
            }
            }
            balance
        }
        }
    }
    """

    CONSUMPTION_QUERY = """
    query getAccountMeasurements(
        $propertyId: ID! 
        $first: Int!        
        $utilityFilters: [UtilityFiltersInput!]        
        $startOn: Date        
        $endOn: Date        
        $startAt: DateTime        
        $endAt: DateTime        
        $timezone: String    
        ) {        
            property(id: $propertyId) {            
                measurements(
                    first: $first                
                    utilityFilters: $utilityFilters                
                    startOn: $startOn                
                    endOn: $endOn                
                    startAt: $startAt                
                    endAt: $endAt                
                    timezone: $timezone           
                ) {
                    edges {                    
                        node {
                            value                        
                            unit ... on IntervalMeasurementType {                            
                                startAt                            
                                endAt                            
                                durationInSeconds                       
                            } metaData {                            
                                statistics {                                
                                    costExclTax {                                    
                                        pricePerUnit {                                        
                                            amount                                   
                                        }                                    
                                        costCurrency                                    
                                        estimatedAmount                                
                                    }                                
                                    costInclTax {                                    
                                        costCurrency                                    
                                        estimatedAmount                                
                                    }                                
                                    value                                
                                    description                                
                                    label                                
                                    type                            
                                }                        
                            }                    
                        }                
                    }            
                }        
            }    
        }

    """


class INFLUX:
    HOST = os.getenv("INFLUX_HOST", "http://localhost:8181")
    TOKEN = os.getenv("INFLUX_TOKEN")
    DB = os.getenv("INFLUX_DB", "octopus")
