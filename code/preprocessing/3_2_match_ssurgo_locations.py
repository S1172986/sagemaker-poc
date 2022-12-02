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
    ssurgo_locations_file: str, 
    field_ssurgo_locations_file: str
)-> None:
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl3_2.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    to_process = run_processing(
        bucket,
        folder,
        [field_ssurgo_locations_file, unique_field_locations_file, ssurgo_locations_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False
    
    logger.info("Downloading file {f} from S3".format(f=unique_field_locations_file))
    unique_field_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=unique_field_locations_file)
    unique_field_locations_df = pd.read_csv(unique_field_locations_file)
    logger.info('Row count: {r}'.format(r=unique_field_locations_df.shape[0]))
    
    logger.info("Downloading file {f} from S3".format(f=ssurgo_locations_file))
    ssurgo_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=ssurgo_locations_file)
    ssurgo_location_df = pd.read_csv(ssurgo_locations_file)
    logger.info('Row count: {r}'.format(r=ssurgo_location_df.shape[0]))
    
    logger.info("Matching ssurgo location to field locations")
    ssurgo_lat, ssurgo_lon, ssurgo_place, ssurgo_distance = [], [], [], []
    for i, row in unique_field_locations_df.iterrows():
        if i %1000 ==0:
            logger.info('Processing row no: {r}'.format(r=i))
        closest_loc = get_closest_point(row['latitude'], row['longitude'], ssurgo_location_df)
        ssurgo_lat.append(closest_loc['lat'])
        ssurgo_lon.append(closest_loc['lon'])
        ssurgo_place.append(closest_loc['place_id'])
        ssurgo_distance.append(closest_loc['distance'])
    
    unique_field_locations_df['ssurgo_lat'] = ssurgo_lat
    unique_field_locations_df['ssurgo_lon'] = ssurgo_lon
    unique_field_locations_df['ssurgo_place'] = ssurgo_place
    unique_field_locations_df['ssurgo_distance'] = ssurgo_distance
    
    logger.info('Final row count: {r}'.format(r=unique_field_locations_df.shape[0]))
#     9954
    logger.info('Final column names: {c}'.format(c=unique_field_locations_df.columns.tolist()))
#     ['latitude', 'longitude', 'ssurgo_lat', 'ssurgo_lon', 'ssurgo_place', 'ssurgo_distance']

    logger.info('Saving data as csv file')
    unique_field_locations_df.to_csv(field_ssurgo_locations_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, 
                                                                      field_ssurgo_locations_file
                                                                     )).upload_file(field_ssurgo_locations_file)
    
    logger.info('Deleting local files')
    os.remove(field_ssurgo_locations_file)

    
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--unique-field-locations-file", type=str, default='1_unique_fieild_locations.csv')
    parser.add_argument("--ssurgo-locations-file", type=str, default='2_all_ssurgo_locations.csv')
    parser.add_argument("--field-ssurgo-locations-file", type=str, default='3_field_ssurgo_locations.csv')
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket, 
        args.folder, 
        args.unique_field_locations_file, 
        args.ssurgo_locations_file,
        args.field_ssurgo_locations_file,
    )
    
   