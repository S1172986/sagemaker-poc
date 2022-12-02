import os
import boto3
import argparse
import boto3
import logging
import tarfile
import pickle
import pandas as pd
import numpy as np

from typing import List

columns = ['global_trend', 'V0_temperature_c_surface_avg', 'V0_temperature_c_surface_max', 'V0_temperature_c_surface_min', 
           'V0_mean_cvi', 'V0_std_cvi', 'V0_median_cvi', 'V0_q10_cvi', 'V0_q25_cvi', 'V0_q75_cvi', 'V0_q90_cvi',
           'V0_mean_cab', 'V0_std_cab', 'V0_median_cab', 'V0_q10_cab', 'V0_q25_cab', 'V0_q75_cab', 'V0_q90_cab',
           'V0_mean_ecnorm', 'V0_std_ecnorm', 'V0_median_ecnorm', 'V0_q10_ecnorm', 'V0_q25_ecnorm', 'V0_q75_ecnorm', 'V0_q90_ecnorm',
           'V0_mean_evi2', 'V0_std_evi2', 'V0_median_evi2', 'V0_q10_evi2', 'V0_q25_evi2', 'V0_q75_evi2', 'V0_q90_evi2',
           'V0_mean_gndvi', 'V0_std_gndvi', 'V0_median_gndvi', 'V0_q10_gndvi', 'V0_q25_gndvi', 'V0_q75_gndvi', 'V0_q90_gndvi',
           'V0_mean_gvmi', 'V0_std_gvmi', 'V0_median_gvmi', 'V0_q10_gvmi', 'V0_q25_gvmi', 'V0_q75_gvmi', 'V0_q90_gvmi',
           'V0_mean_lai', 'V0_std_lai', 'V0_median_lai', 'V0_q10_lai', 'V0_q25_lai', 'V0_q75_lai', 'V0_q90_lai',
           'V0_mean_mcari', 'V0_std_mcari', 'V0_median_mcari', 'V0_q10_mcari', 'V0_q25_mcari', 'V0_q75_mcari', 'V0_q90_mcari',
           'V0_mean_msavi', 'V0_std_msavi', 'V0_median_msavi', 'V0_q10_msavi', 'V0_q25_msavi', 'V0_q75_msavi', 'V0_q90_msavi',
           'V0_mean_nddi', 'V0_std_nddi', 'V0_median_nddi', 'V0_q10_nddi', 'V0_q25_nddi', 'V0_q75_nddi', 'V0_q90_nddi',
           'V0_mean_ndvi', 'V0_std_ndvi', 'V0_median_ndvi', 'V0_q10_ndvi', 'V0_q25_ndvi', 'V0_q75_ndvi', 'V0_q90_ndvi',
           'V0_mean_ndwi', 'V0_std_ndwi', 'V0_median_ndwi', 'V0_q10_ndwi', 'V0_q25_ndwi', 'V0_q75_ndwi', 'V0_q90_ndwi',
           'V0_mean_pvi', 'V0_std_pvi', 'V0_median_pvi', 'V0_q10_pvi', 'V0_q25_pvi', 'V0_q75_pvi', 'V0_q90_pvi',
           'V0_mean_slavi', 'V0_std_slavi', 'V0_median_slavi', 'V0_q10_slavi', 'V0_q25_slavi', 'V0_q75_slavi', 'V0_q90_slavi',
           'V0_mean_vsdi', 'V0_std_vsdi', 'V0_median_vsdi', 'V0_q10_vsdi', 'V0_q25_vsdi', 'V0_q75_vsdi', 'V0_q90_vsdi',
           'V0_mean_wet', 'V0_std_wet', 'V0_median_wet', 'V0_q10_wet', 'V0_q25_wet', 'V0_q75_wet', 'V0_q90_wet',
           'V0_mean_fapar', 'V0_std_fapar', 'V0_median_fapar', 'V0_q10_fapar', 'V0_q25_fapar', 'V0_q75_fapar', 'V0_q90_fapar'
          ]

def get_mape(Y_actual,Y_Predicted):
    mape = np.mean(np.abs((Y_actual - Y_Predicted)/Y_actual))*100
    return mape


def main(
    bucket:str,
    folder:str,
    trained_model_file:str, 
    test_data_file:str
):
    
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(lineno)d ==>> %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
#     file_handler = logging.FileHandler('etl1_2.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)

    
    logger.info("Unzipping file {f} from S3".format(f=trained_model_file))
    field_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=trained_model_file)
    with tarfile.open('model.tar.gz') as tar:
        tar.extractall(path=".")
    print("Loading model")
    model_obj = pickle.load(open("model.pkl", 'rb'))
    model = model_obj['model']
    hp = model_obj['hyperparameter']
    
    logger.info("Downloading all_data file  in folder{f} from S3".format(f=test_data_file))
    test_data_file = 's3://{b}/{l}/{f}'.format(b=bucket, l=folder,f=test_data_file)
    test_data_df = pd.read_csv(test_data_file)
    logger.info('Row count: {r}'.format(r=str(test_data_df.shape[0])))
    
    test_data_df = test_data_df.drop(columns, axis=1)
    
    X_test = test_data_df.drop(['yield'], axis=1)
    y_test = test_data_df['yield']
    
    y_predicted = []
    for i, row in X_test.iterrows():
        y_predicted.append(model.predict(row))
        
    mape = get_mape(y_test, y_predicted)
    logger.info(f'Final mape for test data is {mape}')
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default='cad-alok-singh')
    parser.add_argument("--folder", type=str, default='us_in_season_corn_yield/8_stages/V0/train_test_2020')
    parser.add_argument("--trained_model_file", type=str, default='model/cad-2022-09-29-02-29-54-915/output/model.tar.gz')
    parser.add_argument("--test_data_file", type=str, default='test/test.csv')
    args, _ = parser.parse_known_args()
    
    
    main(
        args.bucket,
        args.folder,
        args.trained_model_file,
        args.test_data_file
    )