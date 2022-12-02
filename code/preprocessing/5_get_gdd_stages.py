import os
import argparse
import logging
import boto3
import datetime as dt
import pandas as pd

from helper import run_processing


GDD_BLACK_LAYER_SLOPE = 14.011
GDD_BLACK_LAYER_INTERCET = 1051.4
GDD_F_SLOPE = 0.6212
GDD_F_INTERCEPT = -207.06
VEG_STAGES = ['pre_season','V0', 'VE', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 
                'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 'V19', 'V20', 'V21', 
                'R1 Silk - Pollenation', 'R2 Blister', 'R3 Milk', 'R4 Dough', 'R5 Dent', 'R6 Black Layer']
STAGE_GROUP = {
    'pre_season': 'pre_season',
    'V0': 'V0', 
    'VE': 'VE', 
    'V1': 'bin1',  
    'V2': 'bin1', 
    'V3': 'bin1', 
    'V4': 'bin1', 
    'V5': 'bin2', 
    'V6': 'bin2', 
    'V7': 'bin2', 
    'V8': 'bin2', 
    'V9': 'bin3', 
    'V10': 'bin3', 
    'V11': 'bin3', 
    'V12': 'bin3', 
    'V13': 'bin3', 
    'V14': 'bin3', 
    'V15': 'bin4', 
    'V16': 'bin4', 
    'V17': 'bin4', 
    'V18': 'bin4', 
    'V19': 'bin4', 
    'V20': 'bin4', 
    'V21': 'bin4', 
    'R1 Silk - Pollenation': 'bin4', 
    'R2 Blister':'R2', 
    'R3 Milk':'R3', 
    'R4 Dough':'R4', 
    'R5 Dent': 'R5', 
    'R6 Black Layer': '--',
    '--':'--'
}


def get_stage_boundaries(relative_maturity: int):
    black_layer = GDD_BLACK_LAYER_SLOPE * relative_maturity + GDD_BLACK_LAYER_INTERCET
    f =  GDD_F_SLOPE * black_layer + GDD_F_INTERCEPT
    veg_growth_stages = [{'name':VEG_STAGES[0], 'start': 0, 'end':120}]
    temp_start = 120
    for st in VEG_STAGES[1:17]:
        temp_end = temp_start + 66
        if temp_start < f:
            veg_growth_stages.append({'name': st, 'start': temp_start, 'end':temp_end})
            temp_start = temp_end
    
    for st in VEG_STAGES[17:24]:
        temp_end = temp_start + 48
        if temp_start < f:
            veg_growth_stages.append({'name': st, 'start': temp_start, 'end':temp_end})
            temp_start = temp_end
        
    veg_growth_stages[-1]['end'] = f
    
    veg_growth_stages.append({'name': VEG_STAGES[24], 'start': f, 'end': f + 0.25 * (black_layer - f)})
    veg_growth_stages.append({'name': VEG_STAGES[25],'start': f + 0.25 * (black_layer - f), 'end': f + 0.4 * (black_layer - f) })
    veg_growth_stages.append({'name': VEG_STAGES[26], 'start': f + 0.4 * (black_layer - f), 'end': f + 0.55 * (black_layer - f)})
    veg_growth_stages.append({'name': VEG_STAGES[27], 'start': f + 0.55 * (black_layer - f), 'end': f + 0.72 * (black_layer - f)})
    veg_growth_stages.append({'name': VEG_STAGES[28], 'start': f + 0.72 * (black_layer - f), 'end': black_layer })
    veg_growth_stages.append({'name': VEG_STAGES[29],'start':black_layer ,'end': black_layer + 200 })
    
    return veg_growth_stages


def find_loc(list_dict, dict_key, dict_val):

    for d_loc, dict in enumerate(list_dict):
        if dict[dict_key] == dict_val:
            return d_loc
    return -1


def group_stages(veg_growth_stages):
    growth_bins = []
    
    for stage in veg_growth_stages:
        bin_name = STAGE_GROUP[stage['name']]
        b_loc = find_loc(growth_bins, 'name', bin_name)
        if b_loc == -1:
            growth_bins.append({
                'name': bin_name,
                'stages':[stage['name']],
                'start': [stage['start']],
                'end':[stage['end']]
            })
        else:
            growth_bins[b_loc]['stages'].append(stage['name'])
            growth_bins[b_loc]['start'].append(stage['start'])
            growth_bins[b_loc]['end'].append(stage['end'])
    for g_loc, g in enumerate(growth_bins):
        growth_bins[g_loc]['start'] =  min(g['start'])
        growth_bins[g_loc]['end'] = max(g['end'])
    return growth_bins
            

    
def main(
    bucket: str,
    folder:str,
    fields_file: str,
    field_weather_locations_file:str,
    field_weather_file: str,
    field_gdd_stage_file: str
):
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     logger.addHandler(stream_handler)
    file_handler = logging.FileHandler('etl5.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    to_process = run_processing(
        bucket,
        folder,
        [field_gdd_stage_file, fields_file, field_weather_file]
    )
    logger.info(to_process['msg'])
    if not to_process['status']:
        return True
    
    logger.info("Downloading file {f} from S3".format(f=fields_file))
    field_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=fields_file)
    field_df = pd.read_csv(field_file)
    logger.info('Row count: {r}'.format(r=str(field_df.shape[0])))
    
    logger.info("Downloading file {f} from S3".format(f=field_weather_locations_file))
    field_weather_locations_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_weather_locations_file)
    field_weather_locations_df = pd.read_csv(field_weather_locations_file)
    field_weather_locations_df = field_weather_locations_df.drop_duplicates()
    logger.info('Row count: {r}'.format(r=field_weather_locations_df.shape[0]))
    
    combined_df = pd.merge(field_df, field_weather_locations_df, how='left', on=['longitude', 'latitude'])
    logger.info('Row count after join: {r}'.format(r=combined_df.shape[0]))
    
    logger.info("Downloading file {f} from S3".format(f=field_weather_file))
    field_weather_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=field_weather_file)
    field_weather_df = pd.read_csv(field_weather_file)
    field_weather_df = field_weather_df.drop_duplicates()
    field_weather_df =   field_weather_df[['place_id', 'date', 
                                           'temperature_c_2_m_above_gnd_min', 'temperature_c_2_m_above_gnd_max']]
    logger.info('Row count: {r}'.format(r=field_weather_df.shape[0]))
    
    gdd_df = pd.DataFrame()
    for r_index, row in combined_df.iterrows():
        if r_index %100 ==0:
            logger.info('Processing row {r} out of {t}'.format(r=r_index, t= combined_df.shape[0]))
        row_weather_place = row['weather_place']
        row_weather_df = field_weather_df[field_weather_df['place_id']==row_weather_place]
    
        if row_weather_df.shape[0]==0:
            continue
            
        row_planting_date = row['planting_date']
        row_planting_date = dt.datetime.strptime(row_planting_date, "%Y-%m-%d")
        row_start_date = dt.datetime.strftime(row_planting_date, "%Y%m%d")
        row_end_date = row_planting_date + dt.timedelta(days=364)
        row_end_date = dt.datetime.strftime(row_end_date,"%Y%m%d")
        
        row_weather_df = row_weather_df[row_weather_df['date'] >= int(row_start_date)]
        row_weather_df = row_weather_df[row_weather_df['date'] <= int(row_end_date)]
        
        gdd_stage_boundaries = get_stage_boundaries(row['relative_maturity_2'])
        growth_bins = group_stages(gdd_stage_boundaries)
            
        temp_df = row_weather_df
        temp_df['temperature_c_2_m_above_gnd_min'] = temp_df['temperature_c_2_m_above_gnd_min'].apply(lambda x: 10 if x < 10 else x)
        temp_df['temperature_c_2_m_above_gnd_max'] = temp_df['temperature_c_2_m_above_gnd_max'].apply(lambda x: 30 if x > 30 else x)
        temp_df['gdd'] = ((temp_df['temperature_c_2_m_above_gnd_min'] + temp_df['temperature_c_2_m_above_gnd_max'])/2) - 10.5
        temp_df ['gdd']= temp_df['gdd'].apply(lambda x: (x * 9)/5)
        temp_df = temp_df.sort_values(by=['date'])
        temp_df['gdd_cumsum'] = temp_df['gdd'].cumsum()
        actual_stages = []
        for g in growth_bins:
            stage_df = temp_df[temp_df['gdd_cumsum'] >= g['start']]
            stage_df = stage_df[stage_df['gdd_cumsum']< g['end']]
            if g['name'] != '--':
                actual_stages.append({
                    'growth_bin':g['name'],
                    'start_date':min(stage_df['date']),
                    'end_date':max(stage_df['date'])
                })
        actual_stages_df = pd.DataFrame(actual_stages)
        for col in combined_df.columns.tolist():
            actual_stages_df[col] = row[col]
        
        gdd_df = pd.concat([gdd_df, actual_stages_df], axis=0)
    gdd_df = gdd_df.drop(['weather_lat', 'weather_lon', 'weather_place', 'weather_distance'], axis =1)   
    gdd_df.to_csv(field_gdd_stage_file, index=False)
    
    
    
    logger.info('Final row count: {r}'.format(r=gdd_df.shape[0]))
#     151756
    logger.info('Final column names: {c}'.format(c=gdd_df.columns.tolist()))
#     ['growth_bin', 'start_date', 'end_date', 'trial_id', 'longitude', 'latitude', 'year', 'planting_date', 'variety_name2', 'yield', 'relative_maturity_2', 'soil_type_2', 'previous_crop_2', 'irrigation', ]
    print('Uploading data in s3 bucket')
    boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, field_gdd_stage_file)).upload_file(field_gdd_stage_file)
    print('Deleting local files')
    os.remove(field_gdd_stage_file)
    
    

if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    parser.add_argument("--fields-file", type=str, default='1_field_raw_data.csv')
    parser.add_argument("--field-weather-locations-file", type=str, default='3_field_weather_locations.csv')
    parser.add_argument("--field-weather-file", type=str, default='4_field_weather.csv')
    parser.add_argument("--field-gdd-stage-file", type=str, default='5_field_gdd_stage.csv')
    args, _ = parser.parse_known_args()
    
   
    main(
        args.bucket,
        args.folder,
        args.fields_file,
        args.field_weather_locations_file,
        args.field_weather_file,
        args.field_gdd_stage_file
    )