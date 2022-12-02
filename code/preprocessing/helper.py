import boto3
import pandas as pd
from math import sin, cos, sqrt, atan2, radians
from typing import List

def get_distance_km(lat1, lon1, lat2, lon2):
    R = 6373.0

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance



def get_closest_point(lat: str, 
                      lon: str, 
                      df: pd.DataFrame
                     ):

    temp_df = df[df['lat'] > lat - .1]
    temp_df = temp_df[temp_df['lat'] < lat + .1]
    temp_df = temp_df[temp_df['lon'] > lon - .1]
    temp_df = temp_df[temp_df['lon'] < lon + .1]
    if temp_df.shape[0]==0:
        temp_df = df[df['lat'] > lat - .5]
        temp_df = temp_df[temp_df['lat'] < lat + .5]
        temp_df = temp_df[temp_df['lon'] > lon - .5]
        temp_df = temp_df[temp_df['lon'] < lon + .5]
    temp_df['distance'] = temp_df.apply(lambda x: get_distance_km(lat, lon, x['lat'], x['lon']), axis=1)
    
    temp_df = temp_df.sort_values(by=['distance'], axis=0)
    return {
       'lat': temp_df['lat'].tolist()[0], 
        'lon':temp_df['lon'].tolist()[0],
        'place_id': temp_df['place_id'].tolist()[0],
        'distance':temp_df['distance'].tolist()[0]
    } 

def get_bucket_files(
    bucket:str,
    folder:str
) -> List[str]:
    all_files = boto3.Session().resource('s3').Bucket(bucket).objects.all()
    all_files = [obj.key for obj in all_files]
    all_files = [a for a in all_files if folder in a]
    all_files = [a.split('/')[-1] for a in all_files]
    all_files = all_files[1:]
    return all_files


def run_processing(
    bucket: str,
    folder: str,
    files: List[str],
):
    all_files = get_bucket_files(
        bucket,
        folder
    )
    
    for f_i, f in enumerate(files):
        if f_i == 0:
            if f in all_files:
                return {'status': False, 'msg': "File {f}  is already available".format(f=f)}
            else:
                return {'status': True, 'msg': "Processing with next steps in ETL"}
        else:
            if f not in all_files:
                return {'status':False, 'msg':"File {f} should be present, please execute earlier steps in ETL".format(f=f)}
    
        
   