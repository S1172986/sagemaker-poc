import sys
import io
import logging
import numpy as np
import json
from catboost import CatBoostRegressor
from pathlib import Path

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


def model_fn(model_dir):
    """Deserialized and return fitted model.

    Note that this should have the same name as the serialized model in the _xgb_train method
    """
    model_file = Path(model_dir) / "model"
    model = CatBoostRegressor().load_model(model_file)
    return model


def input_fn(request_body, request_content_type):
    """An input_fn that loads a pickled numpy array"""

    if request_content_type == "text/csv":
        return request_body
    elif request_content_type=="application/json":
        return json.loads(request_body)
    elif request_content_type == "application/python-pickle":
        array = np.load(io.StringIO(request_body))
        return array
    elif request_content_type == "application/x-npy":
        array = np.load(io.BytesIO(request_body), allow_pickle=True)
        return array
    else:
        raise ValueError(f"{request_content_type} not supported by script!")