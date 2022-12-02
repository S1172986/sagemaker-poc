import argparse
import os
import time
import boto3
import logging
import threading
import pandas as pd
import datetime as dt

from typing import List

from sql_strings import sql_nasa
from utils_airlock import Airlock
from helper import run_processing

def threaded_task(
    airlock: Airlock,
    results: List,
    index:int,
    sql: str,
    logger
):
    start_time = time.time()
    temp_df = airlock.get_data(sql)
    results[index] = temp_df
    end_time = time.time()
    
def main(
    bucket: str,
    folder: str,
    fields_file: str,
    field_nasa_locations_file: str,
    field_nasa_file: str,
    start_index: int,
    end_index: int,
    upload_s3:bool = False
):
    no_threads = 10
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     logger.addHandler(stream_handler)
    file_handler = logging.FileHandler('etl4_3.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    to_process = run_processing(
        bucket,
        folder,
        [field_nasa_file, fields_file, field_nasa_locations_file]
    )
    logger.info(to_process['msg'])
    if not to_process['status']:
        return False
    
    logger.info("Downloading file {f} from S3".format(f=fields_file))
    field_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=fields_file)
    field_df = pd.read_csv(field_file)
    logger.info('Row count: {r}'.format(r=str(field_df.shape[0])))
    
    logger.info("Downloading file {f} from S3".format(f=field_nasa_locations_file))
    field_nasa_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_nasa_locations_file)
    field_nasa_locations_df = pd.read_csv(field_nasa_locations_file)
    field_nasa_locations_df = field_nasa_locations_df.drop_duplicates()
    logger.info('Row count: {r}'.format(r=field_nasa_locations_df.shape[0]))
    
    combined_df = pd.merge(field_df, field_nasa_locations_df, how='left', on=['longitude', 'latitude'])
    combined_df = combined_df[['nasa_lat', 'nasa_lon', 'planting_date']].drop_duplicates()
    combined_df = combined_df.reset_index()
    logger.info('Unique rows for nasa station: {r}'.format(r=combined_df[['nasa_lat', 'nasa_lon']].drop_duplicates().shape[0]))
    logger.info('Unique rows for nasa station and date: {r}'.format(r=combined_df.shape[0]))
    
    sql_list = []
    for r_ind, row in combined_df.iterrows():
        temp_lat = row['nasa_lat']
        temp_lon = row['nasa_lon']
        temp_planting_date = row['planting_date']
        temp_planitng_date = dt.datetime.strptime(temp_planting_date, "%Y-%m-%d")
                
        temp_start_date = dt.datetime.strftime(temp_planitng_date, "%Y%m%d")
        temp_end_date = temp_planitng_date + dt.timedelta(days=364)
        temp_end_date = dt.datetime.strftime(temp_end_date,"%Y%m%d")
        
        sql_list.append(
            sql_nasa.format(
                lat=temp_lat,
                lon=temp_lon,
                start_date=temp_start_date,
                end_date=temp_end_date
            )
        )
    
    sql_list = sql_list[start_index: end_index]
    
    airlock = Airlock()
    airlock.create_connection()
    
    if os.path.exists(field_nasa_file):
        nasa_df = pd.read_csv(field_nasa_file)
    else:
        nasa_df = pd.DataFrame()
        
    for i in range(0, len(sql_list), no_threads):
        results = [None] * no_threads
        temp_sql_list = sql_list[i: i + no_threads]
        
        start_time = time.time()
        threads = []
        for j, sqls in enumerate(temp_sql_list):
            threads.append(threading.Thread(target=threaded_task, args=(airlock, results, j, sqls, logger )))

        [t.start() for t in threads]
        [t.join() for t in threads]
        end_time = time.time()
        temp_df = pd.concat(results, axis=0)
        nasa_df = pd.concat([nasa_df, temp_df], axis=0)
        logger.info('{n} Threds completed in {t} secs with from i:{i1} to i:{i2} with rows: {r} '.format(t=end_time-start_time, i1=i, i2=i+no_threads, r=nasa_df.shape[0], n=no_threads))
        
    airlock.close_connection()
    
    logger.info('Final row count: {r}'.format(r=nasa_df.shape[0]))
#     1197900
    logger.info('Final column names: {c}'.format(c=nasa_df.columns.tolist()))
#     ['value', 'longitude', 'latitude', 'type', 'start_date', 'end_date']
    logger.info('Saving data as csv file')
    nasa_df.to_csv(field_nasa_file, index=False)
    if upload_s3:
        logger.info('Uploading data in s3 bucket')
        boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, field_nasa_file)).upload_file(field_nasa_file)

        logger.info('Deleting local files')
        os.remove(field_nasa_file)
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--fields-file", type=str, default='1_field_raw_data.csv')
    parser.add_argument("--field-nasa-locations-file", type=str, default='3_field_nasa_locations.csv')
    parser.add_argument("--field-nasa-file", type=str, default='4_field_nasa.csv')
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=100)
    args, _ = parser.parse_known_args()
    
    main(
        args.bucket,
        args.folder,
        args.fields_file,
        args.field_nasa_locations_file,
        args.field_nasa_file,
        args.start,
        args.end,
        False
    )