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


WEATHER_COLUMNS = {
    'frost_days': 'sum',
    'heat_stress': 'sum',
    'dry_days': 'sum',
    'low_humidity': 'sum',
    'high_humidity': 'sum',
    
    'evapotranspiration_mm_surface_sum': 'sum',
    'leaf_wetness_probability_pct_2_m_mean': 'avg',
    'precipitation_total_mm_surface_sum': 'sum',
    'shortwave_radiation_w_per_m2_surface_sum': 'sum',
    'sunshine_duration_min_surface_sum': 'sum',
    'vapor_pressure_hpa_2_m_above_gnd_avg': 'avg',
    'photosynthetic_active_radiation_w_per_m2_surface_avg': 'avg',

    'cloud_cover_total_pct_surface_avg': 'avg',
    'cloud_cover_total_pct_surface_max': 'max',
    'cloud_cover_total_pct_surface_min': 'min',

    'relative_humidity_pct_2_m_above_gnd_avg': 'avg',
    'relative_humidity_pct_2_m_above_gnd_max': 'max',
    'relative_humidity_pct_2_m_above_gnd_min': 'min',
    
    'soil_moisture_meter3_per_meter3_0_7_cm_down_avg': 'avg',
    'soil_moisture_meter3_per_meter3_0_7_cm_down_max': 'max',
    'soil_moisture_meter3_per_meter3_0_7_cm_down_min': 'min',
    'soil_moisture_meter3_per_meter3_7_28_cm_down_avg': 'avg',
    'soil_moisture_meter3_per_meter3_7_28_cm_down_max': 'max',
    'soil_moisture_meter3_per_meter3_7_28_cm_down_min': 'min',
    'soil_moisture_meter3_per_meter3_28_100_cm_down_avg': 'avg',
    'soil_moisture_meter3_per_meter3_28_100_cm_down_max': 'max',
    'soil_moisture_meter3_per_meter3_28_100_cm_down_min': 'min',
    
    'soil_temperature_c_0_7_cm_down_avg': 'avg',
    'soil_temperature_c_0_7_cm_down_max': 'max',
    'soil_temperature_c_0_7_cm_down_min': 'min',
    'soil_temperature_c_7_28_cm_down_avg': 'avg',
    'soil_temperature_c_7_28_cm_down_max': 'max',
    'soil_temperature_c_7_28_cm_down_min': 'min',
    'soil_temperature_c_28_100_cm_down_avg': 'avg',
    'soil_temperature_c_28_100_cm_down_max': 'max',
    'soil_temperature_c_28_100_cm_down_min': 'min',

    'temperature_c_surface_avg': 'avg',
    'temperature_c_surface_max': 'max',
    'temperature_c_surface_min': 'min',
    'temperature_c_2_m_above_gnd_avg': 'avg',
    'temperature_c_2_m_above_gnd_max': 'max',
    'temperature_c_2_m_above_gnd_min': 'min',
    'temperature_c_2_m_above_gnd_avg_halfday_0000': 'avg',
    'temperature_c_2_m_above_gnd_avg_halfday_1200': 'avg',
    
    'wind_speed_km_per_h_2_m_above_gnd_avg': 'avg',
    'wind_speed_km_per_h_2_m_above_gnd_max': 'max',
    'wind_speed_km_per_h_2_m_above_gnd_min': 'min'
}

          
def main(
    bucket: str,
    folder:str,
    field_weather_location_file:str,
    field_weather_file: str,
    field_gdd_stage_file: str,
    weather_agg_file:str
):
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     logger.addHandler(stream_handler)
    file_handler = logging.FileHandler('etl6_1.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    to_process = run_processing(
        bucket,
        folder,
        [weather_agg_file, field_weather_location_file, field_weather_file, field_gdd_stage_file]
    )
    logger.info(to_process['msg'])
    if not to_process['status']:
        return True
    
    logger.info("Downloading file {f} from S3".format(f=field_weather_location_file))
    field_weather_location_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_weather_location_file)
    field_weather_location_df = pd.read_csv(field_weather_location_file)
    logger.info('Row count: {r}'.format(r=str(field_weather_location_file.shape[0])))
    field_weather_location_df = field_weather_location_df.drop(['weather_lat', 'weather_lon', 'weather_distance'], axis=1)
    field_weather_location_df = field_weather_location_df.rename({'weather_place': 'place_id'})
    
    
    logger.info("Downloading file {f} from S3".format(f=field_weather_file))
    field_weather_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_weather_file)
    field_weather_df = pd.read_csv(field_weather_file)
    logger.info('Row count: {r}'.format(r=str(field_weather_df.shape[0])))
    
    field_weather_df = pd.merge(field_weather_df, field_weather_location_df, how='left', on=['place_id'])
    field_weather_df = field_weather_df.drop(['lat', 'lon', 'place_id'], axis=1)
    
    
    
    field_weather_df['frost_days'] = field_weather_df['temperature_c_2_m_above_gnd_min'].apply(lambda x:1 if x<0 else 0)
    field_weather_df['heat_stress'] = field_weather_df['temperature_c_2_m_above_gnd_avg'].apply(lambda x: 1 if x>28 else 0)
    field_weather_df['dry_days'] = field_weather_df['precipitation_total_mm_surface_sum'].apply(lambda x: 1 if x>28 else 0)
    field_weather_df['low_humidity'] = field_weather_df['relative_humidity_pct_2_m_above_gnd_avg'].apply(lambda x: 1 if x<70 else 0)
    field_weather_df['high_humidity'] = field_weather_df['relative_humidity_pct_2_m_above_gnd_avg'].apply(lambda x: 1 if x>90 else 0)
    
    
    logger.info("Downloading file {f} from S3".format(f=field_gdd_stage_file))
    field_gdd_stage_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_gdd_stage_file)
    field_gdd_stage_df = pd.read_csv(field_gdd_stage_file)
    logger.info('Row count: {r}'.format(r=str(field_gdd_stage_df.shape[0])))
    
    
    for c in WEATHER_COLUMNS:
        field_gdd_stage_df[c] = None
    time1 = time.time()
    for r_ind, row in field_gdd_stage_df.iterrows():
        if r_ind %1000 ==0:
            time2 = time.time()
            logger.info('Processing row {r} out of {t} rows in {s} secs'.format(r=r_ind, t=field_gdd_stage_df.shape[0],s=time2-time1))
            time1 = time.time()
        row_weather_df = field_weather_df[field_weather_df['longitude']==row['longitude']]
        row_weather_df = row_weather_df[row_weather_df['latitude'] == row['latitude']]
        
        stage_start_date = row['start_date']
        stage_end_date = row['end_date']
        
        row_weather_df = row_weather_df[row_weather_df['date']>=int(stage_start_date)]
        row_weather_df = row_weather_df[row_weather_df['date']<=int(stage_end_date)]
        if row_weather_df.shape[0]==0:
            continue
        
        for c in WEATHER_COLUMNS:
            agg = WEATHER_COLUMNS[c]
            
            if agg =='sum':
                field_gdd_stage_df.loc[r_ind, c] = row_weather_df[c].sum()
            if agg =='max':
                field_gdd_stage_df.loc[r_ind, c] = row_weather_df[c].max()
            if agg =='min':
                field_gdd_stage_df.loc[r_ind, c] = row_weather_df[c].min()
            if agg =='avg':
                field_gdd_stage_df.loc[r_ind, c] = row_weather_df[c].mean()
    
    field_gdd_stage_df = field_gdd_stage_df.dropna(axis=0)
    
    logger.info('Final row count: {r}'.format(r=field_gdd_stage_df.shape[0]))
#     151756
    logger.info('Final column names: {c}'.format(c=field_gdd_stage_df.columns.tolist()))
#     ['growth_bin', 'start_date', 'end_date', 'trial_id', 'longitude', 'latitude', 'year', 'planting_date', 'variety_name2', 'yield', 'relative_maturity_2', 'soil_type_2', 'previous_crop_2', 'irrigation',  'frost_days', 'heat_stress', 'dry_days', 'low_humidity', 'high_humidity', 'evapotranspiration_mm_surface_sum', 'leaf_wetness_probability_pct_2_m_mean', 'precipitation_total_mm_surface_sum', 'shortwave_radiation_w_per_m2_surface_sum', 'sunshine_duration_min_surface_sum', 'vapor_pressure_hpa_2_m_above_gnd_avg', 'photosynthetic_active_radiation_w_per_m2_surface_avg', 'cloud_cover_total_pct_surface_avg', 'cloud_cover_total_pct_surface_max', 'cloud_cover_total_pct_surface_min', 'relative_humidity_pct_2_m_above_gnd_avg', 'relative_humidity_pct_2_m_above_gnd_max', 'relative_humidity_pct_2_m_above_gnd_min', 'soil_moisture_meter3_per_meter3_0_7_cm_down_avg', 'soil_moisture_meter3_per_meter3_0_7_cm_down_max', 'soil_moisture_meter3_per_meter3_0_7_cm_down_min', 'soil_moisture_meter3_per_meter3_7_28_cm_down_avg', 'soil_moisture_meter3_per_meter3_7_28_cm_down_max', 'soil_moisture_meter3_per_meter3_7_28_cm_down_min', 'soil_moisture_meter3_per_meter3_28_100_cm_down_avg', 'soil_moisture_meter3_per_meter3_28_100_cm_down_max', 'soil_moisture_meter3_per_meter3_28_100_cm_down_min', 'soil_temperature_c_0_7_cm_down_avg', 'soil_temperature_c_0_7_cm_down_max', 'soil_temperature_c_0_7_cm_down_min', 'soil_temperature_c_7_28_cm_down_avg', 'soil_temperature_c_7_28_cm_down_max', 'soil_temperature_c_7_28_cm_down_min', 'soil_temperature_c_28_100_cm_down_avg', 'soil_temperature_c_28_100_cm_down_max', 'soil_temperature_c_28_100_cm_down_min', 'temperature_c_surface_avg', 'temperature_c_surface_max', 'temperature_c_surface_min', 'temperature_c_2_m_above_gnd_avg', 'temperature_c_2_m_above_gnd_max', 'temperature_c_2_m_above_gnd_min', 'temperature_c_2_m_above_gnd_avg_halfday_0000', 'temperature_c_2_m_above_gnd_avg_halfday_1200', 'wind_speed_km_per_h_2_m_above_gnd_avg', 'wind_speed_km_per_h_2_m_above_gnd_max', 'wind_speed_km_per_h_2_m_above_gnd_min']
    logger.info('Saving data as csv file')
    field_gdd_stage_df.to_csv(weather_agg_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, weather_agg_file)).upload_file(weather_agg_file)

    logger.info('Deleting local files')
    os.remove(weather_agg_file)
        

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--field-weather-location-file", type=str, default='3_field_weather_location.csv')
    parser.add_argument("--field-weather-file", type=str, default='4_field_weather.csv')
    parser.add_argument("--field-gdd-stage-file", type=str, default='5_field_gdd_stage.csv')
    parser.add_argument("--weather_agg_file", type=str, default='6_weather_agg.csv')
    
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket,
        args.folder,
        args.field_weather_location_file,
        args.field_weather_file,
        args.field_gdd_stage_file,
        args.weather_agg_file
    )