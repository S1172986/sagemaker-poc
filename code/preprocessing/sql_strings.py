sql_fields= """
    select distinct 
        trial_id, longitude, latitude, year, planting_date, variety_name2, yield, relative_maturity_2,soil_type_2, previous_crop_2, irrigation
    from 
        dsdi.usecase_9states_all_yield_training_data_part1_final
    
    union 
    
    select distinct 
        trial_id, longitude, latitude, year, planting_date, variety_name2, yield, relative_maturity_2,soil_type_2, previous_crop_2, irrigation
    from 
        dsdi.usecase_9states_all_yield_training_data_part2_final
    
    union
    
    select distinct 
        trial_id, longitude, latitude, year, planting_date, variety_name2, yield, relative_maturity_2,soil_type_2, previous_crop_2, irrigation
    from 
        dsdi.usecase_9states_all_yield_training_data_part3_final
    """
sql_unique_location_weather = """
    select distinct
        lat,lon, place_id
    from 
        spectrum_schema.mio171_weather_5x5_pivoted_archive
"""
sql_remote_sensing1 = """
    select * from dsdi.usecase_9states_all_yield_training_data_part1_final
"""
sql_remote_sensing2 = """
    select * from dsdi.usecase_9states_all_yield_training_data_part2_final
"""
sql_remote_sensing3 = """
    select * from dsdi.usecase_9states_all_yield_training_data_part3_final
"""

sql_unique_location_ssurgo = """
    select distinct
        lat,lon, place_id
    from 
        spectrum_schema.stg171_ssurgo_dataset
"""

sql_unique_location_nasa = """
    select distinct
        latitude as lat,
        longitude as lon
    from 
        spectrum_schema.stg171_nasa_usda
"""
sql_weather = """
select
    * 
from
    spectrum_schema.mio171_weather_5x5_pivoted_archive
where
    place_id='{place_id}' and 
    date >= '{start_date}' and 
    date <= '{end_date}'
"""
sql_weather1 = """
select
    * 
from
    spectrum_schema.mio171_weather_5x5_pivoted_archive
where
    place_id='{place_id}'
"""
sql_ssurgo = """
select
    * 
from
    spectrum_schema.stg171_ssurgo_dataset
where
    place_id in {place_ids}
"""
sql_nasa = """
select
    * 
from
    spectrum_schema.stg171_nasa_usda 
where
    latitude={lat} and 
    longitude={lon} and 
    start_date >= '{start_date}' and 
    end_date <= '{end_date}'
"""