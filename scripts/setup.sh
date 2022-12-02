#!/bin/bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CONDA_ENV=sagemaker-poc

find_in_conda_env(){
    conda env list | grep "${@}" >/dev/null 2>/dev/null
}

echo "Conda Environment: ${CONDA_ENV}"

if find_in_conda_env ".*${CONDA_ENV}.*" ; then
    conda env update -f ${SCRIPT_DIR}/../environment.yaml 
else
   conda env create -f ${SCRIPT_DIR}/../environment.yaml
   source activate ${CONDA_ENV}
   jupyter nbextension enable --py widgetsnbextension
   npm install -g catboost-widget
fi
