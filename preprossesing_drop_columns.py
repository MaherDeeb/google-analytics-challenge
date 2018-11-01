# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 19:57:39 2018

@author: Maher Deeb
"""
import pandas as pd

data_path = '../'

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

df_train.to_csv('{}train_clean.csv'.format(data_path),index = False)
df_test.to_csv('{}test_clean.csv'.format(data_path),index = False)