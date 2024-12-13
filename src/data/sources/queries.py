# src/data/sources/queries.py
SCHEMA_QUERY = """
query {
    __type(name: "ghg__rlps_ghg_emitter_sector") {
        fields {
            name
            type {
                name
                kind
            }
        }
    }
}
"""

EPA_GHG_QUERY = """
query($state: String!, $year: Int!, $offset: Int, $limit: Int) {
    ghg__rlps_ghg_emitter_sector(
        offset: $offset,
        limit: $limit,
        where: {
            state: { equals: $state }
            year: { equals: $year }
        }
    ) {
        co2e_emission
        county
        facility_name
        latitude
        longitude
        year
        sector_name
        sector_type
        state
        subsector_desc
        subsector_name
    }
}
"""
