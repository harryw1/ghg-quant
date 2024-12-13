# src/config.py
EPA_CONFIG = {
    "graphql_url": "https://data.epa.gov/dmapservice/query/graphql",
    "batch_size": 10,
    "timeout": 30,
    "max_retries": 3,
    "tables": {
        "ghg": "ghg__rlps_ghg_emitter_sector",
        "facilities": "frs_facility",
        "emissions": "facility_emissions",
    },
}

EPA_TABLES_QUERY = """
query {
  __type(name: "Query") {
    fields {
      name
      description
    }
  }
}
"""
