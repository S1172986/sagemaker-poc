import os
import boto3
import argparse
import boto3
import logging
import pandas as pd

from typing import List

from utils_airlock import Airlock
from helper import get_bucket_files
from sql_strings import sql_remote_sensing1, sql_remote_sensing2, sql_remote_sensing3
from helper import run_processing


COLUMNS_TO_DELETE = ['precipitation_total_mm_surface_sum', 'relative_humidity_pct_2_m_above_gnd_max', 'relative_humidity_pct_2_m_above_gnd_avg', 'relative_humidity_pct_2_m_above_gnd_min', 'shortwave_radiation_w_per_m2_surface_sum', 'soil_moisture_meter3_per_meter3_0_7_cm_down_max', 'soil_moisture_meter3_per_meter3_0_7_cm_down_avg', 'soil_moisture_meter3_per_meter3_0_7_cm_down_min', 'soil_moisture_meter3_per_meter3_28_100_cm_down_max', 'soil_moisture_meter3_per_meter3_28_100_cm_down_avg', 'soil_moisture_meter3_per_meter3_28_100_cm_down_min', 'soil_moisture_meter3_per_meter3_7_28_cm_down_max', 'soil_moisture_meter3_per_meter3_7_28_cm_down_avg', 'soil_moisture_meter3_per_meter3_7_28_cm_down_min', 'soil_temperature_c_0_7_cm_down_max', 'soil_temperature_c_0_7_cm_down_avg', 'soil_temperature_c_0_7_cm_down_min', 'soil_temperature_c_28_100_cm_down_max', 'soil_temperature_c_28_100_cm_down_avg', 'soil_temperature_c_28_100_cm_down_min', 'soil_temperature_c_7_28_cm_down_max', 'soil_temperature_c_7_28_cm_down_avg', 'soil_temperature_c_7_28_cm_down_min', 'sunshine_duration_min_surface_sum', 'temperature_c_2_m_above_gnd_max', 'temperature_c_2_m_above_gnd_avg', 'temperature_c_2_m_above_gnd_min', 'wind_speed_km_per_h_2_m_above_gnd_max', 'wind_speed_km_per_h_2_m_above_gnd_avg', 'wind_speed_km_per_h_2_m_above_gnd_min', 'temperature_c_2_m_above_gnd_avg_halfday_0000', 'temperature_c_2_m_above_gnd_avg_halfday_1200','as1', 'as2', 'theta_s_0_5', 'theta_s_5_15', 'theta_s_15_30', 'theta_s_30_60', 'theta_s_60_100', 'theta_s_100_200', 'theta_r_0_5', 'theta_r_5_15', 'theta_r_15_30', 'theta_r_30_60', 'theta_r_60_100', 'theta_r_100_200', 'sand_0_5', 'sand_5_15', 'sand_15_30', 'sand_30_60', 'sand_60_100', 'sand_100_200', 'ksat_0_5', 'ksat_5_15', 'ksat_15_30', 'ksat_30_60', 'ksat_60_100', 'ksat_100_200', 'clay_0_5', 'clay_5_15', 'clay_15_30', 'clay_30_60', 'clay_60_100', 'clay_100_200', 'om_0_5', 'om_5_15', 'om_15_30', 'om_30_60', 'om_60_100', 'om_100_200', 'silt_0_5', 'silt_5_15', 'silt_15_30', 'silt_30_60', 'silt_60_100', 'silt_100_200', 'ph_0_5', 'ph_5_15', 'ph_15_30', 'ph_30_60', 'ph_60_100', 'ph_100_200', 'bd_0_5', 'bd_5_15', 'bd_15_30', 'bd_30_60', 'bd_60_100', 'bd_100_200', 'global_trend', 'frost_days', 'heat_stress', 'dry_days', 'low_humidity', 'high_humidity'] 
def main(
    bucket: str, 
    location: str, 
    remote_sensing_file: str
)-> None:
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl1_3.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    to_process = run_processing(
        bucket,
        location, 
        [remote_sensing_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False

    airlock = Airlock()
    airlock.create_connection()
    
    df = []
    rows = [0, 150000,300000, 450000, 600000, 750000, 900000]
    
    logger.info('Reading first table from database')
    df.append(airlock.get_data(sql_remote_sensing1))
    
    logger.info('Reading second table from database')
    for r in rows:
        logger.info('{gettting from {r1} to {r2}}'.format(r1=r, r2=r+150000))
        temp_sql = sql_remote_sensing2 + 'OFFSET {o} LIMIT 150000'.format(o=r) 
        temp_df = airlock.get_data(temp_sql)
        temp_df = temp_df.drop(COLUMNS_TO_DELETE, axis=1)
        df.append(temp_df)
        logger.info('Rows retrieved: {r}'.format(r=df[-1].shape[0]))
    
    logger.info('Reading third table from database')
    df.append(airlock.get_data(sql_remote_sensing3))
    
    remote_sensing_df = pd.concat(df, axis=0)
    logger.info('Final row count: {r}'.format(r=remote_sensing_df.shape[0]))
    logger.info('Final column name: {c}'.format(c=remote_sensing_df.columns.tolist()))
    
    logger.info('Saving data as csv file')
    remote_sensing_df.to_csv(remote_sensing_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(location, remote_sensing_file)).upload_file(remote_sensing_file)
    
    logger.info('Deleting local files')
    os.remove(remote_sensing_file)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--remote-sensing-file", type=str, default='6_remote_sensing_data.csv')
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket, 
        args.folder, 
        args.remote_sensing_file
    )
    