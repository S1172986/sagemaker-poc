import os
import boto3
import argparse
import boto3
import logging
import pandas as pd

from typing import List

from utils_airlock import Airlock
from helper import run_processing
from sql_strings import sql_fields

# s3://sagemaker-<region>-<account_id>/<processing_job_name>/output/<output_name/
def main(
    bucket: str, 
    folder: str, 
    field_file: str
)-> None:
    region = 'eu-central-1'
    account_id='226275233641'
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl1_1.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)
    
    to_process = run_processing(
        bucket,
        folder,
        [field_file]
    )
    
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False
    
    airlock = Airlock()
    airlock.create_connection()
    
    logger.info('Reading data from database')
    field_df = airlock.get_data(sql_fields)
    
    logger.info('Final row count: {r}'.format(r=field_df.shape[0]))
#     103459
    logger.info('Final column name: {c}'.format(c=field_df.columns.tolist()))
#     ['trial_id', 'longitude', 'latitude', 'year', 'planting_date', 'variety_name2', 'yield', 'relative_maturity_2', 'soil_type_2', 'previous_crop_2', 'irrigation']
    
    logger.info('Saving data as csv file')
#     fields_loc = os.path.join('/opt/ml/processing', fields_file)
    fields_loc = fields_file
    field_df.to_csv(fields_loc, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(location, field_file)).upload_file(field_file)
    
    logger.info('Deleting local files')
    os.remove(field_file)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--field-file", type=str, default='1_field_raw_data.csv')
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket, 
        args.folder, 
        args.field_file
    )