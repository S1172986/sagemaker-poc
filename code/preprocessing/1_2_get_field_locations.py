import os
import boto3
import argparse
import boto3
import logging
import pandas as pd

from typing import List

from sql_strings import sql_fields
from helper import run_processing

def main(
    bucket:str,
    folder:str,
    fields_file: str,
    unique_field_locations_file:str
)-> None:
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl1_2.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)
    
    to_process = run_processing(
        bucket,
        folder,
        [unique_field_locations_file, fields_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False      
    
    logger.info("Downloading file {f} from S3".format(f=fields_file))
    field_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=fields_file)
    field_df = pd.read_csv(field_file)
    logger.info('Row count: {n}'.format(n=field_df.shape[0]))
    
    unique_field_df = field_df[['latitude', 'longitude']]
    unique_field_df = unique_field_df.drop_duplicates()
    
    logger.info('Final row count: {r}'.format(r=unique_field_df.shape[0]))
#     9954
    logger.info('Final column name: {c}'.format(c=unique_field_df.columns.tolist()))
#     ['latitude', 'longitude']
    logger.info('Saving data as csv file')
    unique_field_df.to_csv(unique_field_locations_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, unique_field_locations_file)).upload_file(unique_field_locations_file)
    
    logger.info('Deleting local files')
    os.remove(unique_field_locations_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--fields-file", type=str, default='1_field_raw_data.csv')
    parser.add_argument("--unique-field-locations-file", type=str, default='1_unique_fieild_locations.csv')
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket, 
        args.folder, 
        args.fields_file, 
        args.unique_field_locations_file
    )