import argparse
import os
import time
import boto3
import logging
import threading
import pandas as pd
import datetime as dt

from typing import List

from sql_strings import sql_weather
from utils_airlock import Airlock
from helper import run_processing


def threaded_task(
    airlock: Airlock,
    results: List,
    index:int,
    sql: str,
    logger
):
    start_time = time.time()
    temp_df = airlock.get_data(sql)
    results[index] = temp_df
    end_time = time.time()


def main(
    bucket: str,
    folder: str,
    fields_file: str,
    field_weather_locations_file: str,
    field_weather_file: str,
    start_index: int,
    end_index: int,
    upload_s3:bool = False
):
    no_threads = 10
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     logger.addHandler(stream_handler)
    file_handler = logging.FileHandler('etl4_1.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
#     to_process = run_processing(
#         bucket,
#         folder,
#         [field_weather_file, fields_file, field_weather_locations_file]
#     )
#     logger.info(to_process['msg'])
    
#     if not to_process['status']:
#         return False
    
    logger.info('start: {s}, end: {e}'.format(s=start_index, e=end_index))
    logger.info("Downloading file {f} from S3".format(f=fields_file))
    field_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=fields_file)
    field_df = pd.read_csv(field_file)
    logger.info('Row count: {r}'.format(r=str(field_df.shape[0])))
    
    logger.info("Downloading file {f} from S3".format(f=field_weather_locations_file))
    field_weather_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_weather_locations_file)
    field_weather_locations_df = pd.read_csv(field_weather_locations_file)
    field_weather_locations_df = field_weather_locations_df.drop_duplicates()
    logger.info('Row count: {r}'.format(r=field_weather_locations_df.shape[0]))
    
    combined_df = pd.merge(field_df, field_weather_locations_df, how='left', on=['longitude', 'latitude'])
    combined_df = combined_df[['weather_place', 'planting_date']].drop_duplicates()
    combined_df = combined_df.sort_values(by=['weather_place', 'planting_date'])
    combined_df = combined_df.reset_index()
    logger.info('Unique rows for weather station: {r}'.format(r=combined_df[['weather_place']].drop_duplicates().shape[0]))
    logger.info('Unique rows for weather station and date: {r}'.format(r=combined_df.shape[0]))
   
    sql_list = []
        
    for r_ind, row in combined_df.iterrows():
        temp_place = row['weather_place']
        temp_planting_date = row['planting_date']
        temp_planitng_date = dt.datetime.strptime(temp_planting_date, "%Y-%m-%d")

        temp_start_date = dt.datetime.strftime(temp_planitng_date, "%Y%m%d")
        temp_end_date = temp_planitng_date + dt.timedelta(days=364)
        temp_end_date = dt.datetime.strftime(temp_end_date,"%Y%m%d")

        sql_list.append( sql_weather.format(
            place_id=temp_place,
            start_date=temp_start_date,
            end_date=temp_end_date
        ))
    sql_list = sql_list[start_index: end_index]
    
    airlock = Airlock()
    airlock.create_connection()
    
    if os.path.exists(field_weather_file):
        weather_df = pd.read_csv(field_weather_file)
    else:
        weather_df = pd.DataFrame()
    
    for i in range(0, len(sql_list), no_threads):
        results = [None] * no_threads
        temp_sql_list = sql_list[i: i + no_threads]
        
        start_time = time.time()
        threads = []
        for j, sqls in enumerate(temp_sql_list):
            threads.append(threading.Thread(target=threaded_task, args=(airlock, results, j, sqls, logger )))
        
        [t.start() for t in threads]
        [t.join() for t in threads]
        end_time = time.time()
        temp_df = pd.concat(results, axis=0)
        weather_df = pd.concat([weather_df, temp_df], axis=0)
        
        logger.info('{n} Threds completed in {t} secs with from i:{i1} to i:{i2} with rows: {r} '.format(t=end_time-start_time, i1=i, i2=i+no_threads, r=weather_df.shape[0], n=no_threads))

    airlock.close_connection()
    
    logger.info('Final row count: {r}'.format(r=weather_df.shape[0]))
#     547488
    logger.info('Final column names: {c}'.format(c=weather_df.columns.tolist()))
#     ['place_id', 'lat', 'lon', 'timeresolution', 'date', 'cloud_cover_total_pct_surface_max', 'cloud_cover_total_pct_surface_avg', 'cloud_cover_total_pct_surface_min', 'evapotranspiration_mm_surface_sum', 'leaf_wetness_probability_pct_2_m_mean', 'precipitation_total_mm_surface_sum', 'relative_humidity_pct_2_m_above_gnd_max', 'relative_humidity_pct_2_m_above_gnd_avg', 'relative_humidity_pct_2_m_above_gnd_min', 'shortwave_radiation_w_per_m2_surface_sum', 'soil_moisture_meter3_per_meter3_0_7_cm_down_max', 'soil_moisture_meter3_per_meter3_0_7_cm_down_avg', 'soil_moisture_meter3_per_meter3_0_7_cm_down_min', 'soil_moisture_meter3_per_meter3_28_100_cm_down_max', 'soil_moisture_meter3_per_meter3_28_100_cm_down_avg', 'soil_moisture_meter3_per_meter3_28_100_cm_down_min', 'soil_moisture_meter3_per_meter3_7_28_cm_down_max', 'soil_moisture_meter3_per_meter3_7_28_cm_down_avg', 'soil_moisture_meter3_per_meter3_7_28_cm_down_min', 'soil_temperature_c_0_7_cm_down_max', 'soil_temperature_c_0_7_cm_down_avg', 'soil_temperature_c_0_7_cm_down_min', 'soil_temperature_c_28_100_cm_down_max', 'soil_temperature_c_28_100_cm_down_avg', 'soil_temperature_c_28_100_cm_down_min', 'soil_temperature_c_7_28_cm_down_max', 'soil_temperature_c_7_28_cm_down_avg', 'soil_temperature_c_7_28_cm_down_min', 'sunshine_duration_min_surface_sum', 'temperature_c_surface_max', 'temperature_c_surface_avg', 'temperature_c_surface_min', 'temperature_c_2_m_above_gnd_max', 'temperature_c_2_m_above_gnd_avg', 'temperature_c_2_m_above_gnd_min', 'vapor_pressure_hpa_2_m_above_gnd_avg', 'wind_speed_km_per_h_2_m_above_gnd_max', 'wind_speed_km_per_h_2_m_above_gnd_avg', 'wind_speed_km_per_h_2_m_above_gnd_min', 'temperature_c_2_m_above_gnd_avg_halfday_0000', 'temperature_c_2_m_above_gnd_avg_halfday_1200', 'photosynthetic_active_radiation_w_per_m2_surface_avg', 'nems_wind_speed_km_per_h_2_m_above_gnd_max', 'nems_wind_speed_km_per_h_2_m_above_gnd_avg', 'nems_wind_speed_km_per_h_2_m_above_gnd_min', 'nems_shortwave_radiation_w_per_m2_surface_sum', 'nems_evapotranspiration_mm_surface_sum', 'nems_vapor_pressure_deficit_hpa_2_m_above_gnd_max', 'nems_vapor_pressure_deficit_hpa_2_m_above_gnd_avg', 'nems_relative_humidity_pct_2_m_above_gnd_max', 'nems_relative_humidity_pct_2_m_above_gnd_avg', 'nems_relative_humidity_pct_2_m_above_gnd_min', 'apparent_temperature_c_2_m_above_gnd_max', 'apparent_temperature_c_2_m_above_gnd_avg', 'apparent_temperature_c_2_m_above_gnd_min', 'growing_degree_days_gddc_surface_sum', 'nems_daylight_duration_min_2_m_above_gnd_sum', 'reference_evapotranspiration_2_m_above_ground_sum', 'nems_wind_gust_km_per_h_surface_max', 'precipitation_total_min_value', 'precipitation_total_avg_value', 'precipitation_total_max_value', 'precipitation_total_perc_min_sum', 'precipitation_total_perc_medium_sum', 'precipitation_total_perc_max_sum', 'relative_humidity_min_value', 'relative_humidity_avg_value', 'relative_humidity_max_value', 'relative_humidity_perc_min_sum', 'relative_humidity_perc_medium_sum', 'relative_humidity_perc_max_sum', 'temperature_2_m_min_value', 'temperature_2_m_avg_value', 'temperature_2_m_max_value', 'temperature_2_m_perc_min_sum', 'temperature_2_m_perc_medium_sum', 'temperature_2_m_perc_max_sum', 'wind_speed_2_m_min_value', 'wind_speed_2_m_avg_value', 'wind_speed_2_m_max_value', 'wind_speed_2_m_perc_min_sum', 'wind_speed_2_m_perc_medium_sum', 'wind_speed_2_m_perc_max_sum', 'smap_soil_moisture_soil_column', 'smap_soil_moisture_water_equevalent', 'evaporation_surface_min_value', 'evaporation_surface_avg_value', 'evaporation_surface_max_value', 'evaporation_surface_perc_min_sum', 'evaporation_surface_perc_medium_sum', 'evaporation_surface_perc_max_sum', 'evapotranspiration_surface_min_value', 'evapotranspiration_surface_avg_value', 'evapotranspiration_surface_max_value', 'evapotranspiration_surface_perc_min_sum', 'evapotranspiration_surface_perc_medium_sum', 'evapotranspiration_surface_perc_max_sum']
    
    logger.info('Saving data as csv file')
    weather_df.to_csv(field_weather_file, index=False)
    
    if upload_s3:
        logger.info('Uploading data in s3 bucket')
        boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, field_weather_file)).upload_file(field_weather_file)

        logger.info('Deleting local files')
        os.remove(field_weather_file)
        
        

if __name__ == '__main__':
    
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--fields-file", type=str, default='1_field_raw_data.csv')
    parser.add_argument("--field-weather-locations-file", type=str, default='3_field_weather_locations.csv')
    parser.add_argument("--field-weather-file", type=str, default='4_field_weather.csv')
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=100)
    
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket,
        args.folder,
        args.fields_file,
        args.field_weather_locations_file,
        args.field_weather_file,
        args.start,
        args.end,
        False
    )
    
    