import argparse
import os
import time
import boto3
import logging
import threading
import pandas as pd
import datetime as dt

from typing import List

from helper import run_processing
   
def main(
    bucket: str,
    folder:str,
    field_nasa_location_file:str,
    field_nasa_file: str,
    field_gdd_stage_file: str,
    nasa_agg_file:str
):
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    file_handler = logging.FileHandler('etl6_2.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    to_process = run_processing(
        bucket,
        folder,
        [nasa_agg_file, field_nasa_location_file, field_nasa_file, field_gdd_stage_file]
    )
    logger.info(to_process['msg'])
    if not to_process['status']:
        return True
    
    logger.info("Downloading file {f} from S3".format(f=field_nasa_location_file))
    field_nasa_location_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_nasa_location_file)
    field_nasa_location_df = pd.read_csv(field_nasa_location_file)
    logger.info('Row count: {r}'.format(r=str(field_nasa_location_df.shape[0])))
    
    logger.info("Downloading file {f} from S3".format(f=field_nasa_file))
    field_nasa_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_nasa_file)
    field_nasa_df = pd.read_csv(field_nasa_file)
    logger.info('Row count: {r}'.format(r=str(field_nasa_df.shape[0])))
    
    
    logger.info("Downloading file {f} from S3".format(f=field_gdd_stage_file))
    field_gdd_stage_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_gdd_stage_file)
    field_gdd_stage_df = pd.read_csv(field_gdd_stage_file)
    logger.info('Row count: {r}'.format(r=str(field_gdd_stage_df.shape[0])))
    
    field_gdd_stage_df = pd.merge(field_gdd_stage_df, field_nasa_location_df, how='left', on=['longitude', 'latitude'])
    
    types = field_nasa_df['type'].unique().tolist()
    for t in types:
        field_gdd_stage_df[t] = None
    
    for r_ind, row in field_gdd_stage_df.iterrows():
        if r_ind %100 ==0:
            logger.info('Processing row {r} out of {t} rows'.format(r=r_ind, t=field_gdd_stage_df.shape[0]))
        
        row_nasa_df = field_nasa_df[field_nasa_df['latitude']==row['nasa_lat']]
        row_nasa_df = row_nasa_df[row_nasa_df['longitude']==row['nasa_lon']]
        
        if row_nasa_df.shape[0]==0:
            continue
        
        row_planting_date = row['planting_date']
        row_planting_date = dt.datetime.strptime(row_planting_date, '%Y-%m-%d')
        row_start_date = dt.datetime.strftime(row_planting_date, "%Y%m%d")
        row_end_date = row_planting_date + dt.timedelta(days=364)
        row_end_date = dt.datetime.strftime(row_end_date,"%Y%m%d")
        
        row_nasa_df = row_nasa_df[row_nasa_df['start_date']>=int(row_start_date)]
        row_nasa_df = row_nasa_df[row_nasa_df['end_date']<=int(row_end_date)]
        
        if row_nasa_df.shape[0]==0:
            continue
        
        stage_start_date = row['start_date']
        stage_end_date = row['end_date']
        
        row_nasa_df = row_nasa_df[row_nasa_df['start_date']>=int(stage_start_date)]
        row_nasa_df = row_nasa_df[row_nasa_df['end_date']<=int(stage_end_date)]
        
        if row_nasa_df.shape[0]==0:
            continue
        row_nasa_df = row_nasa_df[['type', 'value']]
        row_nasa_df = row_nasa_df.groupby(by=['type']).mean()
        row_nasa_df = row_nasa_df.reset_index()
        
        for _, r in row_nasa_df.iterrows():
            field_gdd_stage_df[r['type']]=r['value']
    
    field_gdd_stage_df = field_gdd_stage_df.drop(['nasa_lat', 'nasa_lon', 'nasa_place', 'nasa_distance'], axis=1)
    
    
    logger.info('Final row count: {r}'.format(r=field_gdd_stage_df.shape[0]))
#     151756
    logger.info('Final column names: {c}'.format(c=field_gdd_stage_df.columns.tolist()))
#     ['growth_bin', 'start_date', 'end_date', 'trial_id', 'longitude', 'latitude', 'year', 'planting_date', 'variety_name2', 'yield', 'relative_maturity_2', 'soil_type_2', 'previous_crop_2', 'irrigation', 'weather_lat', 'weather_lon', 'weather_place', 'weather_distance',  'as2', 'as1']
    logger.info('Saving data as csv file')
    field_gdd_stage_df.to_csv(nasa_agg_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, nasa_agg_file)).upload_file(nasa_agg_file)

    logger.info('Deleting local files')
    os.remove(nasa_agg_file)
        

if __name__ == '__main__':
    
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--field-nasa-location-file", type=str, default='3_field_nasa_locations.csv')
    parser.add_argument("--field-nasa-file", type=str, default='4_field_nasa.csv')
    parser.add_argument("--field-gdd-stage-file", type=str, default='5_field_gdd_stage.csv')
    parser.add_argument("--nasa_agg_file", type=str, default='6_nasa_agg.csv')
    
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket,
        args.folder,
        args.field_nasa_location_file,
        args.field_nasa_file,
        args.field_gdd_stage_file,
        args.nasa_agg_file
    )