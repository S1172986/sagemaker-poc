import time
import boto3
import os
import argparse
import logging
import pandas as pd

from utils_airlock import Airlock
from helper import run_processing
from sql_strings import sql_unique_location_weather

def main(
    bucket: str, 
    folder: str, 
    weather_locations_file: str
)-> None:
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl2_1.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    to_process = run_processing(
        bucket,
        folder,
        [weather_locations_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False
    
    airlock = Airlock()
    airlock.create_connection()
    
    logger.info('Getting all weather locations from database')
    start = time.time()
    weather_location_df = airlock.get_data(sql_unique_location_weather)
    stop = time.time()
    airlock.close_connection()
    logger.info('Execution took {s} seconds'.format(s=stop-start))
    logger.info('Final row count: {r}'.format(r=weather_location_df.shape[0]))
    logger.info('Final column name: {c}'.format(c=weather_location_df.columns.tolist()))
    
    logger.info('Saving data as csv file')
#     436703
    weather_location_df.to_csv(weather_locations_file, index=False)
#     ['lat', 'lon', 'place_id']
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, weather_locations_file)).upload_file(weather_locations_file)
    
    logger.info('Deleting local files')
    os.remove(weather_locations_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--weather-locations-file", type=str, default='2_all_weather_locations.csv')
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket,
        args.folder,
        args.weather_locations_file
    )
    
    
   