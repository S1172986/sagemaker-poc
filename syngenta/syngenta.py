import os
import json
import argparse
import yaml
from pathlib import Path
from cloudpathlib import S3Path
from datetime import datetime
import tarfile
import sys
import logging
import boto3
from sagemaker import get_execution_role
from sagemaker import Session
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.s3 import S3Downloader

logger = logging.getLogger(__file__)
logger.setLevel(int(os.getenv("SM_LOG_LEVEL", logging.INFO)))
logger.addHandler(logging.StreamHandler(sys.stdout))

def train(
    role,
    train_params,
    hyperparameters
):
    sklearn_estimator = SKLearn(
        source_dir = train_params['source_dir'],
        entry_point=train_params["entry_point"],
        framework_version=train_params["framework_version"], 
        instance_type=train_params["instance_type"],
        role=role,
        instance_count=train_params["instance_count"],
        tags=train_params["tags"],
        base_job_name=train_params["base_job_name"],
        output_path=train_params["output_path"],
        hyperparameters=hyperparameters,
        container_log_level=train_params["container_log_level"],
        volume_size=train_params["volume_size"],
        max_run=train_params["max_run"],
        max_wait=train_params["max_wait"],
        enable_sagemaker_metrics=train_params["enable_sagemaker_metrics"],
        metric_definitions=train_params["metric_definitions"],
        use_spot_instances=train_params["use_spot_instances"],
        security_group_ids= train_params["security_group_ids"],
        subnets= train_params["subnets"],
    )
    # time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')

    sklearn_estimator.fit(
        # job_name=f'{train_params["job_name"]}-{time_stamp}',
        inputs=train_params["inputs"],
        wait=train_params["wait"]
    )

    return sklearn_estimator

def get_training_job_artifacts(sklearn_estimator, output_dir):
    model_data = S3Path(sklearn_estimator.model_data)
    output_data = model_data.parent / "output.tar.gz"
    model_path = output_dir / "model"
    output_path = output_dir / "output"
    sagemaker_session = Session()

    S3Downloader().download(str(model_data), local_path=str(model_path), sagemaker_session=sagemaker_session)
    S3Downloader().download(str(output_data), local_path=str(output_path), sagemaker_session=sagemaker_session)

    tar = tarfile.open(model_path / "model.tar.gz", "r:gz")
    tar.extractall(path=str(model_path))
    tar.close()

    tar = tarfile.open(output_path / "output.tar.gz", "r:gz")
    tar.extractall(path=str(output_path))
    tar.close()

def main():

    sagemaker_params = yaml.safe_load(open('params.yaml'))
    train_params = sagemaker_params["train"]

    params = yaml.safe_load(open(Path(train_params["source_dir"]) / 'params.yaml'))
    hyperparameters = params["hyperparameters"]

    parser = argparse.ArgumentParser()

    # hyper parameters
    parser.add_argument('--instance_type', type=str, default=train_params["instance_type"])
    parser.add_argument('--output_dir', type=Path, default=Path("./sagemaker"))

    args, _ = parser.parse_known_args()

    boto_session = boto3.Session()
    sagemaker_client = boto_session.client("sagemaker")
    sagemaker_session = Session(
        boto_session=boto_session,
        sagemaker_client=sagemaker_client
    )
    
    role = get_execution_role(sagemaker_session=sagemaker_session)

    train_params['instance_type'] = args.instance_type

    if "local" in train_params['instance_type']:
        train_params["use_spot_instances"] = False
        train_params["max_wait"] = None

    if train_params["use_spot_instances"] is False:
        train_params["max_wait"] = None

    sklearn_estimator = train(
        role,
        train_params,
        hyperparameters
    )

    get_training_job_artifacts(sklearn_estimator, args.output_dir)

if __name__ =='__main__':
    main()


