import time
import boto3
import os
import argparse
import logging
from tqdm import tqdm
import pandas as pd
import datetime as dt

from helper import get_distance_km, get_closest_point, run_processing

def main(
    bucket: str, 
    folder: str, 
    unique_field_locations_file: str, 
    weather_locations_file: str, 
    field_weather_locations_file: str
) ->  None:
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl3_1.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    to_process = run_processing(
        bucket,
        folder,
        [field_weather_locations_file, unique_field_locations_file, weather_locations_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False
    
    logger.info("Downloading file {f} from S3".format(f=unique_field_locations_file))
    unique_field_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=unique_field_locations_file)
    unique_field_locations_df = pd.read_csv(unique_field_locations_file)
    logger.info('Row count: {r}'.format(r=unique_field_locations_df.shape[0]))
    
    logger.info("Downloading file {f} from S3".format(f=weather_locations_file))
    weather_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=weather_locations_file)
    weather_location_df = pd.read_csv(weather_locations_file)
    logger.info('Row count: {r}'.format(r=weather_location_df.shape[0]))
    
    logger.info("Matching weather location to field locations")
    weather_lat, weather_lon, weather_place, weather_distance = [], [], [], []
    for i, row in unique_field_locations_df.iterrows():
        if i %1000 ==0:
            logger.info('Processing row no: {r}'.format(r=i))
        closest_loc = get_closest_point(row['latitude'], row['longitude'], weather_location_df)
        weather_lat.append(closest_loc['lat'])
        weather_lon.append(closest_loc['lon'])
        weather_place.append(closest_loc['place_id'])
        weather_distance.append(closest_loc['distance'])
    
    unique_field_locations_df['weather_lat'] = weather_lat
    unique_field_locations_df['weather_lon'] = weather_lon
    unique_field_locations_df['weather_place'] = weather_place
    unique_field_locations_df['weather_distance'] = weather_distance
    
    logger.info('Final row count: {r}'.format(r=unique_field_locations_df.shape[0]))
#     9954
    logger.info('Final column names: {c}'.format(c=unique_field_locations_df.columns.tolist()))
#     ['latitude', 'longitude', 'weather_lat', 'weather_lon', 'weather_place', 'weather_distance']
    logger.info('Saving data as csv file')
    unique_field_locations_df.to_csv(field_weather_locations_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, 
                                                                      field_weather_locations_file
                                                                     )).upload_file(field_weather_locations_file)
    
    logger.info('Deleting local files')
    os.remove(field_weather_locations_file)
    
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--unique-field-locations-file", type=str, default='1_unique_fieild_locations.csv')
    parser.add_argument("--weather-locations-file", type=str, default='2_all_weather_locations.csv')
    parser.add_argument("--field-weather-locations-file", type=str, default='3_field_weather_locations.csv')
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket, 
        args.folder, 
        args.unique_field_locations_file, 
        args.weather_locations_file,
        args.field_weather_locations_file,
    )