import os
import argparse
import boto3
import logging
import datetime as dt
import reverse_geocoder as rg
import pandas as pd

from helper import run_processing

DELETE_COLUMNS = ['start_date', 'end_date', 'longitude', 'latitude', 'planting_date']

RENAME_COLUMNS = ['as2', 'as1','frost_days', 'heat_stress', 'dry_days', 'low_humidity', 'high_humidity', 'evapotranspiration_mm_surface_sum', 'leaf_wetness_probability_pct_2_m_mean', 'precipitation_total_mm_surface_sum', 'shortwave_radiation_w_per_m2_surface_sum', 'sunshine_duration_min_surface_sum', 'vapor_pressure_hpa_2_m_above_gnd_avg', 'photosynthetic_active_radiation_w_per_m2_surface_avg', 'cloud_cover_total_pct_surface_avg', 'cloud_cover_total_pct_surface_max', 'cloud_cover_total_pct_surface_min', 'relative_humidity_pct_2_m_above_gnd_avg', 'relative_humidity_pct_2_m_above_gnd_max', 'relative_humidity_pct_2_m_above_gnd_min', 'soil_moisture_meter3_per_meter3_0_7_cm_down_avg', 'soil_moisture_meter3_per_meter3_0_7_cm_down_max', 'soil_moisture_meter3_per_meter3_0_7_cm_down_min', 'soil_moisture_meter3_per_meter3_7_28_cm_down_avg', 'soil_moisture_meter3_per_meter3_7_28_cm_down_max', 'soil_moisture_meter3_per_meter3_7_28_cm_down_min', 'soil_moisture_meter3_per_meter3_28_100_cm_down_avg', 'soil_moisture_meter3_per_meter3_28_100_cm_down_max', 'soil_moisture_meter3_per_meter3_28_100_cm_down_min', 'soil_temperature_c_0_7_cm_down_avg', 'soil_temperature_c_0_7_cm_down_max', 'soil_temperature_c_0_7_cm_down_min', 'soil_temperature_c_7_28_cm_down_avg', 'soil_temperature_c_7_28_cm_down_max', 'soil_temperature_c_7_28_cm_down_min', 'soil_temperature_c_28_100_cm_down_avg', 'soil_temperature_c_28_100_cm_down_max', 'soil_temperature_c_28_100_cm_down_min', 'temperature_c_surface_avg', 'temperature_c_surface_max', 'temperature_c_surface_min', 'temperature_c_2_m_above_gnd_avg', 'temperature_c_2_m_above_gnd_max', 'temperature_c_2_m_above_gnd_min', 'temperature_c_2_m_above_gnd_avg_halfday_0000', 'temperature_c_2_m_above_gnd_avg_halfday_1200', 'wind_speed_km_per_h_2_m_above_gnd_avg', 'wind_speed_km_per_h_2_m_above_gnd_max', 'wind_speed_km_per_h_2_m_above_gnd_min', 'gdd', 'mean_cvi', 'std_cvi', 'median_cvi', 'q10_cvi', 'q25_cvi', 'q75_cvi', 'q90_cvi', 'mean_cab', 'std_cab', 'median_cab', 'q10_cab', 'q25_cab', 'q75_cab', 'q90_cab', 'mean_ecnorm', 'std_ecnorm', 'median_ecnorm', 'q10_ecnorm', 'q25_ecnorm', 'q75_ecnorm', 'q90_ecnorm', 'mean_evi2', 'std_evi2', 'median_evi2', 'q10_evi2', 'q25_evi2', 'q75_evi2', 'q90_evi2', 'mean_gndvi', 'std_gndvi', 'median_gndvi', 'q10_gndvi', 'q25_gndvi', 'q75_gndvi', 'q90_gndvi', 'mean_gvmi', 'std_gvmi', 'median_gvmi', 'q10_gvmi', 'q25_gvmi', 'q75_gvmi', 'q90_gvmi', 'mean_lai', 'std_lai', 'median_lai', 'q10_lai', 'q25_lai', 'q75_lai', 'q90_lai', 'mean_mcari', 'std_mcari', 'median_mcari', 'q10_mcari', 'q25_mcari', 'q75_mcari', 'q90_mcari', 'mean_msavi', 'std_msavi', 'median_msavi', 'q10_msavi', 'q25_msavi', 'q75_msavi', 'q90_msavi', 'mean_nddi', 'std_nddi', 'median_nddi', 'q10_nddi', 'q25_nddi', 'q75_nddi', 'q90_nddi', 'mean_ndvi', 'std_ndvi', 'median_ndvi', 'q10_ndvi', 'q25_ndvi', 'q75_ndvi', 'q90_ndvi', 'mean_ndwi', 'std_ndwi', 'median_ndwi', 'q10_ndwi', 'q25_ndwi', 'q75_ndwi', 'q90_ndwi', 'mean_pvi', 'std_pvi', 'median_pvi', 'q10_pvi', 'q25_pvi', 'q75_pvi', 'q90_pvi', 'mean_slavi', 'std_slavi', 'median_slavi', 'q10_slavi', 'q25_slavi', 'q75_slavi', 'q90_slavi', 'mean_vsdi', 'std_vsdi', 'median_vsdi', 'q10_vsdi', 'q25_vsdi', 'q75_vsdi', 'q90_vsdi', 'mean_wet', 'std_wet', 'median_wet', 'q10_wet', 'q25_wet', 'q75_wet', 'q90_wet', 'mean_fapar', 'std_fapar', 'median_fapar', 'q10_fapar', 'q25_fapar', 'q75_fapar', 'q90_fapar']

STAGES = ['V0', 'VE', 'bin1', 'bin2', 'bin3', 'bin4', 'R2', 'R3', 'R4', 'R5']

def main(
    bucket: str,
    folder: str,
    final_file:str,
):
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl8.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    logger.info("Downloading file {f} from S3".format(f=final_file))
    final_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=final_file)
    final_df = pd.read_csv(final_file)
    logger.info('Row count: {r}'.format(r=str(final_df.shape[0])))
    final_df = final_df.drop(DELETE_COLUMNS, axis=1)
    
    same_columns = [c for c in final_df.columns.tolist() if c not in RENAME_COLUMNS]
    same_columns.remove('growth_bin')
    stage_df = pd.DataFrame()
    
    for s_ind, stage in enumerate(STAGES):
        logger.info('Processing stage: {s}'.format(s=stage))
        temp_df = final_df[final_df['growth_bin']==stage]
        logger.info('Row count: {r}'.format(r=temp_df.shape[0]))
        
        
        same_df = temp_df[same_columns]
        rename_df = temp_df[RENAME_COLUMNS + [ 'trial_id', 'variety_name2']]
        
        rename_columns = {c:'_'.join([stage,c]) for c in RENAME_COLUMNS}
        rename_df = rename_df.rename(rename_columns, axis = 1)
        
        if s_ind ==0:
            stage_df = pd.merge(same_df, rename_df, how='inner',  on=['trial_id', 'variety_name2'])
        else:
            stage_df = pd.merge(stage_df, rename_df, how='inner', on=[ 'trial_id', 'variety_name2'])
        
        output_df = stage_df.drop(['trial_id'], axis =1)
        logger.info('Final row count:{r}'.format(r=output_df.shape))
#         logger.info('Final column name: {c}'.format(c=output_df.columns.tolist()))
        
        final_file = 'all_data.csv'
        logger.info('Saving data as csv file')
        output_df.to_csv(final_file, index=False)
        
        logger.info('Uploading data in s3 bucket')
        boto3.Session().resource('s3').Bucket(bucket).Object(os.path.join(folder, '8_stages/{s}/'.format(s=stage),final_file)).upload_file(final_file)
        
        logger.info('Deleting local files')
        os.remove(final_file)

        
        
        

if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield')
    
    parser.add_argument("--final-file", type=str, default='7_final_data.csv')
    
    args, _ = parser.parse_known_args()
    main(
        args.bucket,
        args.folder,
        args.final_file
    )