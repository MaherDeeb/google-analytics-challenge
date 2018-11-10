# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 19:52:40 2018

@author: Maher Deeb
"""

import pandas as pd
import json 


data_path = './data/'

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

def apply_sepration(df_org):

    json_col = ['customDimensions','device', 'geoNetwork', 'totals', 'trafficSource',
                'trafficSource.adwordsClickInfo','customDimensions.0',
               'trafficSource.adwordsClickInfo.targetingCriteria']
    for col_name in json_col:
        if col_name == 'customDimensions.0':
            df_org[col_name]=df_org[col_name].\
            map(lambda x: {'index':'None'} if x is None else x)

        df = separate_json(df_train[col_name])
        df.columns = ['{}.{}'.format(col_name,x) for x in list(df.columns)]
    
        df_org = df_org.join(df)
        df_org = df_org.drop(col_name,axis=1)

    return df_org

nest_json_col = ['hits']
df_train = pd.read_csv('{}train_v2.csv'.format(data_path) ,nrows=20000,engine='python')
df_train = df_train.drop('hits',axis=1)
df_train ['customDimensions']= df_train['customDimensions'].map(lambda x: str(x).replace("\'", "\""))

df_train = apply_sepration(df_train)
#df_train.columns

df_test = pd.read_csv('{}test_v2.csv'.format(data_path),nrows=20000,engine='python')
df_test ['customDimensions']= df_test['customDimensions'].map(lambda x: str(x).replace("\'", "\""))
df_test = df_test.drop('hits',axis=1)

df_test = apply_sepration(df_test)

df_train.to_csv('{}train_expanded.csv'.format(data_path),index = False)
df_test.to_csv('{}test_expanded.csv'.format(data_path),index = False)