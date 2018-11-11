# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 19:57:39 2018

@author: Maher Deeb
"""
import pandas as pd

data_path = './data/'

df_train = pd.read_csv('{}train_expanded.csv'.format(data_path), engine='python',dtype={'fullVisitorId': 'object'})
df_test = pd.read_csv('{}test_expanded.csv'.format(data_path), engine='python',dtype={'fullVisitorId': 'object'})


def drop_unnecessary_columns(df_train,df_test):
    const_cols_train = [c for c in df_train.columns
              if df_train[c].nunique(dropna=False)==1 ]
    const_cols_test = [c for c in df_test.columns 
                     if df_test[c].nunique(dropna=False)==1 ]

    # Drop the columns with constant values
    df_train = df_train.drop(const_cols_train,
                                         axis=1, inplace=False)
    df_test = df_test.drop(const_cols_test,
                                       axis=1, inplace=False)
    return df_train, df_test

df_train, df_test = drop_unnecessary_columns(df_train,df_test)
df_train = df_train.drop(['trafficSource.campaignCode'],axis = 1)

df_train.to_csv('{}train_clean.csv'.format(data_path),index = False)
df_test.to_csv('{}test_clean.csv'.format(data_path),index = False)

print(len(df_train.groupby(['fullVisitorId']).sum()))
print(len(df_test.groupby(['fullVisitorId']).sum()))