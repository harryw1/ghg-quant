# src/data/sources/queries.py
EPA_GHG_QUERY = """
query batchQuery($offset: Integer, $limit: Integer, $orderBy: OrderBy!) {
    ghg__c_fuel_level_information(offset: $offset, limit: $limit, orderBy: $orderBy) {
        facility_id
        facility_name
        reporting_year
        fuel_type
        unit_name
        unit_type
        tier1_co2_combustion_emissions
        tier1_ch4_combustion_emissions
        tier1_n2o_combustion_emissions
        annual_heat_input
        annual_heat_input_uom
    }
}
"""
