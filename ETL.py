# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 19:52:40 2018

@author: Maher Deeb
"""

import pandas as pd
import json 


data_path = '../'

def separate_json(series: pd.Series) -> pd.DataFrame():
    """
    
    Args:
        series: Series before json parsing 

    Returns: DataFrame

    """
    # TODO: Write TypeException
    
    if isinstance(series[0], str):
        return pd.DataFrame(json.loads(s) for s in series)
    return pd.DataFrame(s for s in series)

json_col = ['device', 'geoNetwork', 'totals', 'trafficSource']
nest_json_col = ['adwordsClickInfo']

df_train = pd.read_csv('{}train.csv'.format(data_path), engine='python')
df_train = df_train.join(separate_json(
    df_train[col_name]) for col_name in json_col).drop(json_col, axis=1)
df_train = df_train.join(separate_json(
    df_train[nest_json_col[0]])).drop(nest_json_col, axis=1)

df_test = pd.read_csv('{}test.csv'.format(data_path), engine='python')
df_test = df_test.join(separate_json(
    df_test[col_name]) for col_name in json_col).drop(json_col, axis=1)
df_test = df_test.join(separate_json(
    df_test[nest_json_col[0]])).drop(nest_json_col, axis=1)

df_train.to_csv('{}train_expanded.csv'.format(data_path),index = False)
df_test.to_csv('{}test_expanded.csv'.format(data_path),index = False)