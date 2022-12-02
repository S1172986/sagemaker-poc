import pandas as pd
import os
from pathlib import Path
from sklearn.model_selection import train_test_split

input_data_path = os.path.join("/opt/ml/processing/input", "dataset.csv")
df = pd.read_csv(input_data_path)
print("Shape of data is:", df.shape)
train, test = train_test_split(df, test_size=0.2)
train, validation = train_test_split(train, test_size=0.2)

Path("/opt/ml/processing/output/train").mkdir(parents=True, exist_ok=True)
Path("/opt/ml/processing/output/validation").mkdir(parents=True, exist_ok=True)
Path("/opt/ml/processing/output/test").mkdir(parents=True, exist_ok=True)

try:
    train.to_csv("/opt/ml/processing/output/train/train.csv")
    validation.to_csv("/opt/ml/processing/output/validation/validation.csv")
    test.to_csv("/opt/ml/processing/output/test/test.csv")
    print("Wrote files successfully")
except Exception as e:
    print("Failed to write the files")
    print(e)
    pass

print("Completed running the processing job")
