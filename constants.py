__all__ = ["OCTOPUS", "INFLUX", "TZ", "BILLED_POWER"]

import os

BILLED_POWER: float = float(os.getenv("BILLED_POWER", "3.5"))
TZ: str = os.getenv("TZ", "Europe/Madrid")


class OCTOPUS:
    GRAPHQL_URL = os.getenv(
        "OCTOPUS_GRAPHQL_URL", "https://octopusenergy.es/api/graphql/kraken"
    )
    PROPERTY_ID = os.getenv("OCTOPUS_PROPERTY_ID", "629606")
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
