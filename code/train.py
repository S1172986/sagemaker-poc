import os
import json
import argparse
from textwrap import indent
import yaml
import logging
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
import matplotlib.pyplot as plt
from sklearn import metrics
from catboost import CatBoostRegressor, Pool
import sys
import logging

logger = logging.getLogger(__file__)
logger.setLevel(int(os.getenv("SM_LOG_LEVEL", logging.INFO)))
logger.addHandler(logging.StreamHandler(sys.stdout))


def train(
    train_file: str, 
    test_file: str,
    y_column: str,
    drop_columns: list,
    hyperparameters: dict, 
    model_dir: Path,
    output_dir: Path,
    output_data_dir: Path,
    output_intermediate_dir: Path
):
    plot_dir = output_intermediate_dir / "plots"
    artifacts_dir = output_intermediate_dir / "artifacts"

    # make directories
    model_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_data_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    hyperparameters["train_dir"].mkdir(parents=True, exist_ok=True)

    train_df = pd.read_csv(train_file)
    test_df = pd.read_csv(test_file)

    if drop_columns:
        train_df = train_df.drop(drop_columns, axis=1)
        test_df = test_df.drop(drop_columns, axis=1)

    train_df.to_csv(output_data_dir / "train.csv")

    X_train = train_df.drop([y_column], axis=1)
    y_train = train_df[y_column] 

    test_df.to_csv(output_data_dir / "test.csv")

    X_test = test_df.drop([y_column], axis=1)
    y_test = test_df[y_column]

    categorical_features_indices = np.where((X_test.dtypes != float) & (X_test.dtypes != int))[0]
    cat_cols = list(X_test.columns[categorical_features_indices])
    
    pool_train = Pool(X_train, y_train, cat_features = cat_cols)

    cat_regr = CatBoostRegressor(**hyperparameters)

    model = cat_regr.fit(pool_train)
    cat_regr.save_model(model_dir / "model")

    model_obj = {'model': model, 'hyperparameter':params}
    model_file = artifacts_dir / 'model.joblib'
    joblib.dump(model_obj, filename=model_file)

    y_pred = model.predict(X_test)

    mae = metrics.mean_absolute_error(y_test, y_pred)
    mape = metrics.mean_absolute_percentage_error(y_test, y_pred)
    rmse = np.sqrt(metrics.mean_squared_error(y_test, y_pred))
    r2 = metrics.r2_score(y_test, y_pred)

    logger.info("Testing performance")
    logger.info(f"mae: {mae:.2f};")
    logger.info(f"ampe: {mape:.2f};")
    logger.info(f"rmse: {rmse:.2f};")
    logger.info(f"r2: {r2:.2f};")

    metrics_results = {
        "mae": mae,
        "mape": mape,
        "rmse": rmse,
        "r2": r2
    }

    with (output_dir / "metrics.json").open('w') as fh:
        json.dump(metrics_results, fh)

    sorted_feature_importance = model.feature_importances_.argsort()

    plt.figure(figsize=(16,16))
    plt.barh(X_test.columns[sorted_feature_importance], 
            model.feature_importances_[sorted_feature_importance], 
            color='turquoise')
    plt.xlabel("CatBoost Feature Importance")
    plt.grid(which="both")
    plt.tight_layout()
    plt.savefig(plot_dir / "feature_importance.png")

    df = pd.read_csv(hyperparameters["train_dir"] / 'learn_error.tsv', sep='\t', header=0)
    data = {"MAE": df.to_dict(orient="records")}

    with (plot_dir / 'mae.json').open('w') as fh:
        json.dump(data ,fh)

if __name__ =='__main__':

    project_root_dir = Path(__file__).parent.resolve() / "../"

    params = yaml.safe_load(open('params.yaml'))
    hyperparameters = params["hyperparameters"]
    directories = params["directories"]
    datasets = params["datasets"]

    parser = argparse.ArgumentParser()

    # hyper parameters
    parser.add_argument('--learning_rate', type=float, default=hyperparameters["learning_rate"])
    parser.add_argument('--iterations', type=int, default=hyperparameters["iterations"])
    parser.add_argument('--max_depth', type=int, default=hyperparameters["max_depth"])
    parser.add_argument('--l2_leaf_reg', type=int, default=hyperparameters["l2_leaf_reg"])
    parser.add_argument('--subsample', type=float, default=hyperparameters["subsample"])
    parser.add_argument('--random_state', type=int, default=hyperparameters["random_state"])
    parser.add_argument('--loss_function', type=str, default=hyperparameters["loss_function"])
    parser.add_argument('--train_dir', type=Path, default=os.getenv('SM_OUTPUT_DIR', project_root_dir / 'output/train'))

    # sagemaker arguments
    parser.add_argument('--model_dir', type=Path, default=os.getenv('SM_MODEL_DIR', project_root_dir / directories["model_dir"]))
    parser.add_argument('--output_dir', type=Path, default=os.getenv('SM_OUTPUT_DIR', project_root_dir / directories["output_dir"]))
    parser.add_argument('--output_data_dir', type=Path, default=os.getenv('SM_OUTPUT_DATA_DIR', project_root_dir / directories["output_data_dir"]))
    parser.add_argument('--output_intermediate_dir', type=Path, default=os.getenv('SM_OUTPUT_INTERMEDIATE_DIR', project_root_dir / directories["output_intermediate_dir"]))

    # Datasets
    parser.add_argument('--train', type=Path, default=os.getenv('SM_CHANNEL_TRAIN', project_root_dir / datasets["train"]))
    parser.add_argument('--train_file', type=Path, default=datasets["train_file"])
    parser.add_argument('--test', type=Path, default=os.getenv('SM_CHANNEL_TEST', project_root_dir / datasets["test"]))
    parser.add_argument('--test_file', type=Path, default=datasets["test_file"])
    parser.add_argument('--y_column', type=str, default=datasets["y_column"])
    parser.add_argument('--drop_columns', type=list, default=datasets["drop_columns"])

    args, _ = parser.parse_known_args()

    # update hyperparameters
    for key in hyperparameters:
        hyperparameters[key] = args.__dict__[key]
    
    hyperparameters["train_dir"] = args.train_dir

    train(
        args.train / args.train_file, 
        args.test / args.test_file, 
        args.y_column,
        args.drop_columns,
        hyperparameters, 
        args.model_dir, 
        args.output_dir, 
        args.output_data_dir,
        args.output_intermediate_dir
        )
