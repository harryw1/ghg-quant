# src/config.py
EPA_CONFIG = {
    "base_url": "https://data.epa.gov/efservice",
    "tables": {
        "facilities": "ghg.pub_dim_facility",
        "emissions": "ghg.pub_facts_sector_ghg_emission",
    },
    "batch_size": 1000,
    "timeout": 30,
    "max_retries": 3,
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
