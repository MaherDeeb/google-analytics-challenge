# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 21:39:05 2018

@author: Maher Deeb
"""

from sklearn.model_selection import train_test_split
import lightgbm as lgb 
import pandas as pd
import numpy as np
import itertools as it
from catboost import CatBoostRegressor
from sklearn.metrics import mean_squared_error

data_path = './data/'

df_train = pd.read_csv('{}train_feature_engineering.csv'.format(data_path), engine='python',dtype={'fullVisitorId': 'object'})
df_test = pd.read_csv('{}test_feature_engineering.csv'.format(data_path), engine='python',dtype={'fullVisitorId': 'object'})



Y_train = df_train['totals.transactionRevenue']
Y_test = df_test['totals.transactionRevenue']

df_train = df_train.drop(['totals.transactionRevenue'],axis = 1)
df_test = df_test.drop(['totals.transactionRevenue'],axis = 1)

Y_train = Y_train.fillna(0).astype('int64')
fullVisitorId_test = df_test.fullVisitorId
df_train = df_train.drop(['fullVisitorId'],axis = 1)
df_test = df_test.drop(['fullVisitorId'],axis = 1)
    
# =============================================================================
# except:
#     df_train = df_train.drop(['Unnamed: 0'],axis = 1)
#     df_test = df_test.drop(['Unnamed: 0'],axis = 1)
# 
#     Y_train= pd.read_csv('{}target_clean.csv'.format(data_path), engine='python',header=None)
#     Y_train = Y_train.fillna(0).astype('int64')
#     fullVisitorId_test = pd.read_csv('{}test_id_clean.csv'.format(data_path), engine='python',header=None)
#     
# =============================================================================
    
def replace_strings_integer(df_train, df_test):
    df_total = pd.concat([df_train,df_test])
    df_total.index=range(len(df_total['date']))
    df_train_decoded = df_train
    df_test_decoded= df_test
    for col_i in df_train.columns[df_train.dtypes == 'object']:
            
        df_total[col_i] = df_total[col_i].factorize()[0]
        df_train_decoded[col_i] = df_total.loc[range(df_train.shape[0]),col_i].values
        df_test_decoded[col_i] =  df_total.loc[range(df_train.shape[0],
                                                     df_train.shape[0]+df_test.shape[0]),
                                               col_i].values
    return df_train_decoded, df_test_decoded

df_train, df_test = replace_strings_integer(df_train, df_test)
Y_train_b = (Y_train > 0)*1

random_state = 0
x_train, x_cv, y_train, y_cv= train_test_split(df_train,np.array(Y_train),
                       test_size=0.1,stratify=Y_train_b,random_state=random_state)

#x_train_n = (x_train - np.mean(x_train))/np.std(x_train)
#x_cv = (x_cv - np.mean(x_train))/np.std(x_train)
#df_test = (df_test - np.mean(x_train))/np.std(x_train)



def rmse(y_true, y_pred):
    return round(np.sqrt(mean_squared_error(y_true, y_pred)), 5)



model = CatBoostRegressor(iterations=1000, learning_rate=0.05, depth=10, eval_metric='RMSE', random_seed = 42, bagging_temperature = 0.2, od_type='Iter', metric_period = 50, od_wait=20)

model.fit(x_train, y_train, eval_set=(x_cv, y_cv), use_best_model=True, verbose=True)
y_pred_train = model.predict(x_train)
y_pred_val = model.predict(x_cv)
y_pred_submit = model.predict(df_test)

print(f"CatB: RMSE val: {rmse(y_cv, y_pred_val)}  - RMSE train: {rmse(y_train, y_pred_train)}")



solution = pd.DataFrame({ 'fullVisitorId': fullVisitorId_test.values,'PredictedLogRevenue': y_pred_submit })
solution = solution.groupby('fullVisitorId')['PredictedLogRevenue'].sum().reset_index()
print('Saving data')
solution.to_csv('randomForest' + '.csv', float_format='%.8f', index=False)
print('Saved data')
