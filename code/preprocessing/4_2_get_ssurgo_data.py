import argparse
import os
import time
import boto3
import logging
import pandas as pd
import datetime as dt

from sql_strings import sql_ssurgo
from utils_airlock import Airlock
from helper import run_processing


def main(
    bucket: str,
    folder: str,
    field_ssurgo_locations_file: str,
    field_ssurgo_file: str
)-> None:
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     logger.addHandler(stream_handler)
    file_handler = logging.FileHandler('etl4_2.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
#     to_process = run_processing(
#         bucket,
#         folder,
#         [field_ssurgo_file, field_ssurgo_locations_file,]
#     )
#     if not to_process:
#         return False

    logger.info("Downloading file {f} from S3".format(f=field_ssurgo_locations_file))
    field_ssurgo_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_ssurgo_locations_file)
    field_ssurgo_locations_df = pd.read_csv(field_ssurgo_locations_file)
    logger.info('Row count: {r}'.format(r=field_ssurgo_locations_df.shape[0]))
    
    
    ssurgo_places = field_ssurgo_locations_df['ssurgo_place'].unique().tolist()
    ssurgo_places = "('" + "', '".join(ssurgo_places) + "')" 
    
    start_time = time.time()
    airlock = Airlock()
    airlock.create_connection()
    temp_sql = sql_ssurgo.format(
        place_ids=ssurgo_places
    )
    ssurgo_df = airlock.get_data(temp_sql)
    airlock.close_connection()
    end_time = time.time()
    logger.info(">> Time taken (sec) in execuitng query: {t}".format(t=end_time - start_time))
    
#     ssurgo_df = ssurgo_df.drop(columns=['lat', 'lon'])
#     ssurgo_df = ssurgo_df.rename(columns={'place_id':'ssurgo_place'})
    
#     ssurgo_df = pd.merge(ssurgo_df, field_ssurgo_locations_df, how='left', on=['ssurgo_place'])
    
    logger.info('Final row count: {r}'.format(r=ssurgo_df.shape[0]))
#     537516
    logger.info('Final column names: {c}'.format(c=ssurgo_df.columns.tolist()))
#     ['statistics', 'layers_depts', 'ssurgo_place', 'value', 'variable', 'latitude', 'longitude', 'ssurgo_lat', 'ssurgo_lon', 'ssurgo_distance']
    logger.info('Saving data as csv file')
    ssurgo_df.to_csv(field_ssurgo_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, field_ssurgo_file)).upload_file(field_ssurgo_file)
    
    logger.info('Deleting local files')
    os.remove(field_ssurgo_file)
        
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--field-ssurgo-locations-file", type=str, default='3_field_ssurgo_locations.csv')
    parser.add_argument("--field-ssurgo-file", type=str, default='4_field_ssurgo.csv')
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket,
        args.folder,
        args.field_ssurgo_locations_file,
        args.field_ssurgo_file
    )
        
    
    
    