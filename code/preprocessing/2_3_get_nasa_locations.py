import time
import boto3
import os
import argparse
import logging
import pandas as pd

from utils_airlock import Airlock
from helper import run_processing
from sql_strings import sql_unique_location_nasa

def main(
    bucket: str, 
    folder: str, 
    nasa_locations_file: str
) ->  None:
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl2_3.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    to_process = run_processing(
        bucket,
        folder,
        [nasa_locations_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False
    
    airlock = Airlock()
    airlock.create_connection()
    logger.info('Getting all nasa locations from database')
    start = time.time()
    
    nasa_location_df = airlock.get_data(sql_unique_location_nasa)
    stop = time.time()
    airlock.close_connection()
    logger.info('Execution took {s} seconds'.format(s=stop-start))
    
    nasa_location_df = nasa_location_df.drop_duplicates()
    nasa_location_df['place_id'] = nasa_location_df.apply(lambda x:'_'.join(['gridpoint', '{:09.5f}'.format(x['lat']), '{:09.5f}'.format(x['lon'])]), axis=1)
    
    logger.info('Final row count: {r}'.format(r=nasa_location_df.shape[0]))
#     9830400
    logger.info('Final column names: {c}'.format(c=nasa_location_df.columns.tolist()))
#     ['lat', 'lon', 'place_id']
    logger.info('Saving data as csv file')
    nasa_location_df.to_csv(nasa_locations_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, nasa_locations_file)).upload_file(nasa_locations_file)
    
    logger.info('Deleting local files')
    os.remove(nasa_locations_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--nasa-locations-file", type=str, default='2_all_nasa_locations.csv')
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket,
        args.folder,
        args.nasa_locations_file 
    )