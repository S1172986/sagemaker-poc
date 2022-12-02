import time
import boto3
import os
import argparse
import logging
import pandas as pd
import datetime as dt

from tqdm import tqdm

from helper import get_closest_point, run_processing
    
def main(
    bucket: str, 
    folder: str, 
    unique_field_locations_file: str, 
    nasa_locations_file: str, 
    field_nasa_locations_file: str
):
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl3_3.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    to_process = run_processing(
        bucket,
        folder,
        [field_nasa_locations_file, unique_field_locations_file, nasa_locations_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False
    
    logger.info("Downloading file {f} from S3".format(f=unique_field_locations_file))
    unique_field_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=unique_field_locations_file)
    unique_field_locations_df = pd.read_csv(unique_field_locations_file)
    logger.info('Row count: {r}'.format(r=unique_field_locations_df.shape[0]))
    
    logger.info("Downloading file {f} from S3".format(f=nasa_locations_file))
    nasa_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=nasa_locations_file)
    nasa_location_df = pd.read_csv(nasa_file)
    logger.info('Row count: {r}'.format(r=nasa_location_df.shape[0]))
    
    logger.info("Matching nasa location to field locations")
    nasa_lat, nasa_lon, nasa_place, nasa_distance = [], [], [], []
    for i, row in unique_field_locations_df.iterrows():
        if i %1000 ==0:
            logger.inf('Processing row number: {r}'.format(r=i))
        closest_loc = get_closest_point(row['latitude'], row['longitude'], nasa_location_df)
        nasa_lat.append(closest_loc['lat'])
        nasa_lon.append(closest_loc['lon'])
        nasa_place.append(closest_loc['place_id'])
        nasa_distance.append(closest_loc['distance'])
    
    unique_field_locations_df['nasa_lat'] = nasa_lat
    unique_field_locations_df['nasa_lon'] = nasa_lon
    unique_field_locations_df['nasa_place'] = nasa_place
    unique_field_locations_df['nasa_distance'] = nasa_distance

    logger.info('Final row count: {r}'.format(r=unique_field_locations_df.shape[0]))
#     9954
    logger.info('Final column names: {c}'.format(c=unique_field_locations_df.columns.tolist()))
#     ['latitude', 'longitude', 'nasa_lat', 'nasa_lon', 'nasa_place', 'nasa_distance']
    logger.info('Saving data as csv file')
    unique_field_locations_df.to_csv(field_nasa_locations_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, field_nasa_locations_file)).upload_file(field_nasa_locations_file)
    
    logger.info('Deleting local files')
    os.remove(field_nasa_locations_file)
    
    
    
if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--unique-field-locations-file", type=str, default='1_unique_fieild_locations.csv')
    parser.add_argument("--nasa-locations-file", type=str, default='2_all_nasa_locations.csv')
    parser.add_argument("--field-nasa-locations-file", type=str, default='3_field_nasa_locations.csv' )
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket, 
        args.folder, 
        args.unique_field_locations_file, 
        args.nasa_locations_file,
        args.field_nasa_locations_file,
    )
            