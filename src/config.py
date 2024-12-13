# src/config.py
EPA_CONFIG = {
    "graphql_url": "https://data.epa.gov/dmapservice/query/graphql",
    "batch_size": 10,  # Match the API limit
    "timeout": 30,
    "max_retries": 3,
    "tables": {
        "ghg": "ghg__c_fuel_level_information",
        "facilities": "frs.frs_facility",
        "emissions": "emissions.facility_emissions",
    },
}
