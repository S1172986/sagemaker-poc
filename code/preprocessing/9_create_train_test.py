import os
import argparse
import boto3
import logging
import datetime as dt
import reverse_geocoder as rg
import pandas as pd

from helper import run_processing

def main(
    bucket: str,
    folder:str,
    stage_folder: str
):
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl9.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)
    
    
    logger.info("Downloading all_data file  in folder{f} from S3".format(f=stage_folder))
    stage_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=os.path.join(stage_folder, 'all_data.csv'))
    stage_df = pd.read_csv(stage_file)
    logger.info('Row count: {r}'.format(r=str(stage_df.shape[0])))
    
    years = stage_df['year'].unique().tolist()
    for year in years:
        logger.info('Processing year: {y}'.format(y=year))
        test_df = stage_df[stage_df['year'] == year]
        train_df = stage_df[stage_df['year'] != year]
        
        logger.info('Train data: {r}'.format(r=train_df.shape))
        logger.info('Test data: {r}'.format(r=test_df.shape))
        
        logger.info('Saving data as csv file')
        test_df.to_csv('test.csv', index=False)
        train_df.to_csv('train.csv', index=False)
        
        logger.info('Uploading data in s3 bucket')
        boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, stage_folder+'/train_test_{y}/train'.format(y=year),'train.csv')).upload_file('train.csv')
        boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, stage_folder+'/train_test_{y}/test'.format(y=year),'test.csv')).upload_file('test.csv')
        
        logger.info('Deleting local files')
        os.remove('train.csv')
        os.remove('test.csv')
        

if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    
    parser.add_argument("--stage-folder", type=str, default='stages/V0')
    
    args, _ = parser.parse_known_args()
    main(
        args.bucket,
        args.folder,
        args.stage_folder
    )