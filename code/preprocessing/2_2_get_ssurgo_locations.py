import time
import boto3
import os
import argparse
import logging
import pandas as pd

from utils_airlock import Airlock
from helper import run_processing
from sql_strings import sql_unique_location_ssurgo
    
def main(
    bucket: str, 
    folder: str, 
    ssurgo_locations_file: str
) ->  None:
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl2_2.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    to_process = run_processing(
        bucket,
        folder,
        [ssurgo_locations_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False
    
    airlock = Airlock()
    airlock.create_connection()
    
    logger.info('Getting all ssurgo locations from database')
    start = time.time()
    ssurgo_location_df = airlock.get_data(sql_unique_location_ssurgo)
    stop = time.time()
    airlock.close_connection()
    logger.info('Execution took {s} seconds'.format(s=stop-start))
    
    
    ssurgo_location_df = ssurgo_location_df.drop_duplicates()
    logger.info('Final row count: {r}'.format(r=ssurgo_location_df.shape[0]))
#     79626
    logger.info('Final column names: {c}'.format(c=ssurgo_location_df.columns.tolist()))
#     ['lat', 'lon', 'place_id']
    
    logger.info('Saving data as csv file')
    ssurgo_location_df.to_csv(ssurgo_locations_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, ssurgo_locations_file)).upload_file(ssurgo_locations_file)
    
    logger.info('Deleting local files')
    os.remove(ssurgo_locations_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--ssurgo-locations-file", type=str, default='2_all_ssurgo_locations.csv')
    args, _ = parser.parse_known_args()

    main(
        args.bucket,
        args.folder,
        args.ssurgo_locations_file
    )
    