#!/usr/bin/env bash

# This script shows how to pull the Docker image from ECR to be ready for use
# by SageMaker.


# Get the account number associated with the current IAM credentials
account=$(aws sts get-caller-identity --query Account --output text)

if [ $? -ne 0 ]
then
    exit 255
fi


# Get the region defined in the current configuration (default to us-west-2 if none defined)
region=$(aws configure get region)
region=${region:-eu-central-1}

# Get the login command from ECR and execute it directly
aws ecr get-login-password --region "${region}" | docker login --username AWS --password-stdin "${account}".dkr.ecr."${region}".amazonaws.com


# Get the login command from ECR in order to pull down the SageMaker PyTorch image
aws ecr get-login-password --region $region | docker login --username AWS --password-stdin 492215442770.dkr.ecr.$region.amazonaws.com
