# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 19:57:39 2018

@author: Maher Deeb
"""
import pandas as pd

data_path = './data/'

df_train = pd.read_csv('{}train_expanded.csv'.format(data_path), engine='python')
df_test = pd.read_csv('{}test_expanded.csv'.format(data_path), engine='python')

expected_column_to_detet=['socialEngagementType','browserSize','browserVersion','flashVersion',
                         'language','mobileDeviceBranding','mobileDeviceInfo','mobileDeviceMarketingName',
                         'mobileDeviceModel','mobileInputSelector','operatingSystemVersion','screenColors',
                          'screenResolution','cityId','latitude','longitude','networkLocation','visits',
                          'campaignCode','criteriaParameters','targetingCriteria']

def drop_unnecessary_columns(expected_column_to_detet,df_train,df_test):
    for col in expected_column_to_detet:
        df_train = df_train.drop([col],axis = 1)
    for col in expected_column_to_detet:
        try:
            df_test = df_test.drop([col],axis = 1)
        except:
            print('Column {} is not exiting in the test dataset'.format(col))
    return df_train, df_test

df_train, df_test = drop_unnecessary_columns(expected_column_to_detet,df_train,df_test)

Y_train = df_train.transactionRevenue
df_train = df_train.drop(['transactionRevenue'],axis = 1)
Y_train = Y_train.fillna(0).astype('int64')
fullVisitorId_test = df_test.fullVisitorId
df_train = df_train.drop(['fullVisitorId'],axis = 1)
df_test = df_test.drop(['fullVisitorId'],axis = 1)

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

df_train.to_csv('{}train_clean.csv'.format(data_path),index = False)
df_test.to_csv('{}test_clean.csv'.format(data_path),index = False)
Y_train.to_csv('{}target_clean.csv'.format(data_path),index = False)
fullVisitorId_test.to_csv('{}test_id_clean.csv'.format(data_path),index = False)