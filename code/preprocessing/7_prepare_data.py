import os
import argparse
import boto3
import logging
import datetime as dt
import reverse_geocoder as rg
import pandas as pd

from helper import run_processing

STATE_MAP = {
    'Nebraska': 'ne',
    'Illinois': 'il',
    'Colorado': 'co',
    'Iowa': 'ia',
    'Kansas': 'ks',
    'Minnesota': 'mn',
    'Missouri': 'mo',
    'South Dakota': 'sd',
}


def get_state_county(row: pd.Series):
    lon = row['longitude']
    lat = row['latitude']
    
    location = rg.search((lat, lon), mode=1)
    state = location[0]['admin1']
    if state not in STATE_MAP:
        return 'None'
    state_code = STATE_MAP[state]
    
    county = location[0]['admin2']
    county = county.lower()
    if 'county' in county:
        county = county.replace('county', ' ')
        county = " ".join(county.split())
    return state + '_' + county
    
def main(
    bucket:str,
    folder:str,
    global_trend_file:str,
    ssurgo_agg_file: str,
    weather_agg_file: str,
    nasa_agg_file: str,
    remote_sensing_file: str,
    final_file: str
):
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     logger.addHandler(stream_handler)
    file_handler = logging.FileHandler('etl7.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

#     to_process = run_processing(
#         bucket,
#         folder,
#         [final_file, remote_sensing_file, weather_agg_file, nasa_agg_file]
#     )
#     logger.info(to_process['msg'])
#     if not to_process['status']:
#         return True

    # READING GLOBAL TREND FILE
    logger.info("Downloading file {f} from S3".format(f=global_trend_file))
    global_trend_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=global_trend_file)
    global_trend_df = pd.read_csv(global_trend_file)
    logger.info('Row count: {r}'.format(r=str(global_trend_df.shape[0])))
    global_trend_df = global_trend_df.drop(['Unnamed: 0'], axis = 1)
    # CREATING COLUMN STATE_COUNTY
    global_trend_df['state_county'] = global_trend_df.apply(lambda x:x['state'] + '_' + x['county'], axis =1)
    global_trend_df = global_trend_df.drop(['state','county'], axis=1)
    
    # READING NASA AGG FILE
    logger.info("Downloading file {f} from S3".format(f=nasa_agg_file))
    nasa_agg_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=nasa_agg_file)
    nasa_agg_df = pd.read_csv(nasa_agg_file)
    logger.info('Row count: {r}'.format(r=str(nasa_agg_df.shape[0])))
    
    # CREATING COLUMN STATE_COUNTY
    nasa_agg_df['state_county'] = nasa_agg_df.apply(get_state_county, axis=1)
    nasa_agg_df = nasa_agg_df[ nasa_agg_df['state_county'] != 'None' ]
    
    # MERGING GLOBAL TREND AND NASA
    output_df = pd.merge(nasa_agg_df, global_trend_df, how='left', on=['state_county'])
    
    
    output_df['global_trend'] = output_df.apply(lambda x:x['year']*x['Slope'] + x['intercept'], axis=1)
    output_df = output_df.drop(['state_county', 'Slope', 'intercept'], axis=1)
    
    
    # READING SSURGO AGG FILE
    logger.info("Downloading file {f} from S3".format(f=ssurgo_agg_file))
    ssurgo_agg_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=ssurgo_agg_file)
    ssurgo_agg_df = pd.read_csv(ssurgo_agg_file)
    logger.info('Row count: {r}'.format(r=str(ssurgo_agg_df.shape[0])))
    ssurgo_agg_df = ssurgo_agg_df.drop( ['start_date', 'end_date',  'longitude', 'latitude', 'year', 'planting_date', 'yield', 'relative_maturity_2', 'soil_type_2', 'previous_crop_2', 'irrigation' ], axis=1)
    
    # MERGING SSURGO AGG DF TO OUTPUT DF
    output_df = pd.merge(output_df, ssurgo_agg_df, how='inner', on=['trial_id', 'variety_name2', 'growth_bin',])
    
    
    #READING WEATHER AGG FILEE
    logger.info("Downloading file {f} from S3".format(f=weather_agg_file))
    weather_agg_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=weather_agg_file)
    weather_agg_df = pd.read_csv(weather_agg_file)
    logger.info('All rows: {r}'.format(r=str(weather_agg_df.shape[0])))
    weather_agg_df = weather_agg_df.drop( ['start_date', 'end_date',  'longitude', 'latitude', 'year', 'planting_date', 'yield', 'relative_maturity_2', 'soil_type_2', 'previous_crop_2', 'irrigation' ], axis=1)
    # MERGIN WEATHER AGG DF TO OUTPUT DF
    output_df = pd.merge(output_df, weather_agg_df, how='inner', on =['trial_id', 'variety_name2','growth_bin'])
    
    
    # READING REMOTE SENSING FILE
    logger.info("Downloading file {f} from S3".format(f=remote_sensing_file))
    remote_sensing_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=remote_sensing_file)
    remote_sensing_df = pd.read_csv(remote_sensing_file)
    logger.info('All rows: {r}'.format(r=str(remote_sensing_df.shape[0])))
    remote_sensing_df =remote_sensing_df.rename({'growth_bins':'growth_bin'}, axis=1)
    remote_sensing_df = remote_sensing_df.drop( ['longitude', 'latitude', 'year', 'planting_date', 'yield', 'relative_maturity_2', 'soil_type_2', 'previous_crop_2', 'irrigation' ], axis=1)
    
    # MERGING REMOTE SENSING DF TO OUTPUT DF
    output_df = pd.merge(output_df, remote_sensing_df, how='inner', on =['trial_id', 'variety_name2','growth_bin'])
    logger.info('Final row count: {r}'.format(r=output_df.shape[0]))
    logger.info('Final column name: {c}'.format(c=output_df.columns.tolist()))
    
    logger.info('Saving data as csv file')
    output_df.to_csv(final_file, index=False)
    
    logger.info('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, final_file)).upload_file(final_file)
    
    logger.info('Deleting local files')
    os.remove(final_file)

    
    
if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    
    parser.add_argument("--global-trend-file", type=str, default='0_global_trend_parameters.csv')
    parser.add_argument("--ssurgo-agg-file", type=str, default='6_ssurgo_agg.csv')
    parser.add_argument("--weather-agg-file", type=str, default='6_weather_agg.csv')
    parser.add_argument("--nasa-agg-file", type=str, default='6_nasa_agg.csv')
    parser.add_argument("--remote-sensing-file", type=str, default='6_remote_sensing_data.csv')
    parser.add_argument("--final-file", type=str, default='7_final_data.csv')
    
    args, _ = parser.parse_known_args()
    main(
        args.bucket,
        args.folder,
        args.global_trend_file,
        args.ssurgo_agg_file,
        args.weather_agg_file,
        args.nasa_agg_file,
        args.remote_sensing_file,
        args.final_file
    )
    
    