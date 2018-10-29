import os
import json
import pandas as pd
from pandas.io.json import json_normalize

pd.options.mode.chained_assignment = None
pd.options.display.max_columns = 999

train_csv = './data/train.csv'
test_csv = './data/test.csv'

nrows = None

def load_df(csv_path, nrows):
    JSON_COLUMNS = ['device', 'geoNetwork', 'totals', 'trafficSource']

    print(f'Processing {csv_path}')

    df = pd.read_csv(csv_path,
                     converters={column: json.loads for column in JSON_COLUMNS},
                     dtype={'fullVisitorId': 'str'}, # Important!!
                     nrows=nrows)

    for column in JSON_COLUMNS:
        column_as_df = json_normalize(df[column])
        column_as_df.columns = [f"{column}.{subcolumn}" for subcolumn in column_as_df.columns]
        df = df.drop(column, axis=1).merge(column_as_df, right_index=True, left_index=True)
    print(f"Loaded {os.path.basename(csv_path)}. Shape: {df.shape}")
    return df


#%%time
train_df = load_df(train_csv, nrows)
test_df = load_df(test_csv, nrows)

print('Writing to CSV .....')
test_df.to_csv('./data/test_expanded.csv')
train_df.to_csv('./data/train_expanded.csv')
print('Done')
