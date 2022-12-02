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
    field_ssurgo_location_file:str,
    field_ssurgo_file: str,
    field_gdd_stage_file: str,
    ssurgo_agg_file:str
):
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    file_handler = logging.FileHandler('etl6_3.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    to_process = run_processing(
        bucket,
        folder,
        [ssurgo_agg_file, field_ssurgo_location_file, field_ssurgo_file, field_gdd_stage_file]
    )
    logger.info(to_process['msg'])
    if not to_process['status']:
        return True
    
    logger.info("Downloading file {f} from S3".format(f=field_ssurgo_location_file))
    field_ssurgo_location_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_ssurgo_location_file)
    field_ssurgo_location_df = pd.read_csv(field_ssurgo_location_file)
    logger.info('Row count: {r}'.format(r=str(field_ssurgo_location_df.shape[0])))
    
    logger.info("Downloading file {f} from S3".format(f=field_ssurgo_file))
    field_ssurgo_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_ssurgo_file)
    field_ssurgo_df = pd.read_csv(field_ssurgo_file)
    logger.info('Row count: {r}'.format(r=str(field_ssurgo_df.shape[0])))
    
    field_ssurgo_df ['column'] = field_ssurgo_df.apply(lambda x:x['variable'] + '_' + x['layers_depts'], axis=1)
    field_ssurgo_df = field_ssurgo_df.drop(['variable', 'layers_depts', 'statistics','lat', 'lon'], axis=1)
    field_ssurgo_df = field_ssurgo_df.drop_duplicates()
    field_ssurgo_df = field_ssurgo_df.pivot(index=['place_id'], columns='column', values='value')
    field_ssurgo_df = field_ssurgo_df.reset_index()
    field_ssurgo_df = field_ssurgo_df.rename({'place_id': 'ssurgo_place'}, axis=1)
    
    field_ssurgo_df = pd.merge(field_ssurgo_df, field_ssurgo_location_df, how='left', on=['ssurgo_place'])
    field_ssurgo_df = field_ssurgo_df.drop(['ssurgo_lat', 'ssurgo_lon', 'ssurgo_place', 'ssurgo_distance'], axis=1)
    
    logger.info("Downloading file {f} from S3".format(f=field_gdd_stage_file))
    field_gdd_stage_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_gdd_stage_file)
    field_gdd_stage_df = pd.read_csv(field_gdd_stage_file)
    logger.info('Row count: {r}'.format(r=str(field_gdd_stage_df.shape[0])))
    
    field_gdd_stage_df = pd.merge(field_gdd_stage_df, field_ssurgo_df, how='left', on=['longitude', 'latitude'])
    
    logger.info('Final row count: {r}'.format(r=field_gdd_stage_df.shape[0]))
#     151756
    logger.info('Final column names: {c}'.format(c=field_gdd_stage_df.columns.tolist()))
#     ['growth_bin', 'start_date', 'end_date', 'trial_id', 'longitude', 'latitude', 'year', 'planting_date', 'variety_name2', 'yield', 'relative_maturity_2', 'soil_type_2', 'previous_crop_2', 'irrigation', 'bd_0_5', 'bd_100_200', 'bd_15_30', 'bd_30_60', 'bd_5_15', 'bd_60_100', 'clay_0_5', 'clay_100_200', 'clay_15_30', 'clay_30_60', 'clay_5_15', 'clay_60_100', 'ksat_0_5', 'ksat_100_200', 'ksat_15_30', 'ksat_30_60', 'ksat_5_15', 'ksat_60_100', 'om_0_5', 'om_30_60', 'om_5_15', 'ph_0_5', 'ph_15_30', 'ph_30_60', 'ph_5_15', 'sand_0_5', 'sand_100_200', 'sand_15_30', 'sand_5_15', 'sand_60_100', 'silt_0_5', 'silt_100_200', 'silt_15_30', 'silt_30_60', 'silt_5_15', 'silt_60_100', 'theta_r_0_5', 'theta_r_100_200', 'theta_r_15_30', 'theta_r_30_60', 'theta_r_5_15', 'theta_r_60_100', 'theta_s_0_5', 'theta_s_100_200', 'theta_s_15_30', 'theta_s_30_60', 'theta_s_5_15', 'theta_s_60_100']
    
    logger.info('Saving data as csv file')
    field_gdd_stage_df.to_csv(ssurgo_agg_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, ssurgo_agg_file)).upload_file(ssurgo_agg_file)

    logger.info('Deleting local files')
    os.remove(ssurgo_agg_file)
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--field-ssurgo-location-file", type=str, default='3_field_ssurgo_locations.csv')
    parser.add_argument("--field-ssurgo-file", type=str, default='4_field_ssurgo.csv')
    parser.add_argument("--field-gdd-stage-file", type=str, default='5_field_gdd_stage.csv')
    parser.add_argument("--ssurgo_agg_file", type=str, default='6_ssurgo_agg.csv')
    
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket,
        args.folder,
        args.field_ssurgo_location_file,
        args.field_ssurgo_file,
        args.field_gdd_stage_file,
        args.ssurgo_agg_file
    )