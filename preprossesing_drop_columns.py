# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 19:57:39 2018

@author: Maher Deeb
"""
import pandas as pd

data_path = './data/'

df_train = pd.read_csv('{}train_expanded.csv'.format(data_path), engine='python',dtype={'fullVisitorId': 'object'})
df_test = pd.read_csv('{}test_expanded.csv'.format(data_path), engine='python',dtype={'fullVisitorId': 'object'})





df_train.to_csv('{}train_clean.csv'.format(data_path),index = False)
df_test.to_csv('{}test_clean.csv'.format(data_path),index = False)

print(len(df_train.groupby(['fullVisitorId']).sum()))
print(len(df_test.groupby(['fullVisitorId']).sum()))