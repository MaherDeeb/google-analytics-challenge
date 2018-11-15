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
data_path = './data/'

df_train = pd.read_csv('{}train_Localfeatures.csv'.format(data_path), engine='python',dtype={'fullVisitorId': 'object'})
df_test = pd.read_csv('{}test_localfeatures.csv'.format(data_path), engine='python',dtype={'fullVisitorId': 'object'})



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
    df_total = pd.concat([df_train,df_test],sort=False)
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

def map_features(X, map_degree,maped_fea):
    V=np.zeros((len(maped_fea),1))
    cor_f=pd.DataFrame(maped_fea)
    com_x_f=[]
    for i in range(2,map_degree+1):
        com_x=list(it.combinations_with_replacement(range(0,1), i))#(range(n_x), i))
        for j in range(len(com_x)):
            if com_x[j][0]!=com_x[j][1] or com_x[j][0]!=com_x[j][1]:
                V[:,0]= X[:,com_x[j][0]]*X[:,com_x[j][1]]
                cor_f['V']=V
                X=np.append(X.T,np.array(X[:,com_x[j][0]]*X[:,com_x[j][1]]).reshape(1,-1),axis=0).T
                com_x_f.append(com_x[j])
    return X,com_x_f


def map_features_test(X, com_x_f):
    com_x=com_x_f
    for j in range(len(com_x)):
        X=np.append(X.T,np.array(X[:,com_x[j][0]]*X[:,com_x[j][1]]).reshape(1,-1),axis=0).T
    return X
#x_train_map,com_x_f=map_features(np.array(x_train),2,y_train)
#df_train = df_train.fillna(0).astype('int64')
#df_test = df_test.fillna(0).astype('int64')

random_state = 0
x_train, x_cv, y_train, y_cv= train_test_split(df_train,np.array(Y_train),
                       test_size=0.1,stratify=Y_train_b,random_state=random_state)

x_train_map,com_x_f=map_features(np.array(x_train),2,y_train)
x_cv_map=map_features_test(np.array(x_cv), com_x_f)
x_test_map=map_features_test(np.array(df_test), com_x_f)

#x_train_n = (x_train - np.mean(x_train))/np.std(x_train)
#x_cv = (x_cv - np.mean(x_train))/np.std(x_train)
#df_test = (df_test - np.mean(x_train))/np.std(x_train)

lgb_params = {"objective" : "regression", "metric" : "root_mean_squared_error",
              "num_leaves" : 5, "learning_rate" : 0.01, 
              "bagging_fraction" : 0.99, "feature_fraction" : 0.99, "bagging_frequency" : 9,
              'max_bin': 255, 'max_depth': -1,'boosting': 'dart','num_rounds': 900,'min_data_in_leaf': 30}

lgb_train = lgb.Dataset(x_train_map, label=np.log1p(y_train.reshape(len(y_train))))
lgb_val = lgb.Dataset(x_cv_map, label=np.log1p(y_cv.reshape(len(y_cv))))
model = lgb.train(lgb_params, train_set=lgb_train, valid_sets=[lgb_val], verbose_eval=50)

preds = model.predict(df_test, num_iteration=model.best_iteration)
pred_sub = pd.DataFrame(fullVisitorId_test)
pred_sub['PredictedLogRevenue'] = preds
x =pred_sub[pred_sub.PredictedLogRevenue < 0].index
pred_sub.loc[x,'PredictedLogRevenue']=0
pred_sub.columns=['fullVisitorId','PredictedLogRevenue']

pred_sub.to_csv(data_path+'predict.csv',index=False)

print(len(df_train.groupby(['fullVisitorId']).sum()))
print(len(df_test.groupby(['fullVisitorId']).sum()))