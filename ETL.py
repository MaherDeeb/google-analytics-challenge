# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 19:52:40 2018

@author: Maher Deeb
"""

import pandas as pd
import json 

import sys
import csv
maxInt = sys.maxsize
decrement = True

while decrement:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/10)
        decrement = True
data_path = './data/'



df_train = apply_sepration(df_train)
#df_train.columns

df_test = pd.read_csv('{}test_v2.csv'.format(data_path),nrows=2000,engine='python',dtype={'fullVisitorId': 'object'})
df_test ['customDimensions']= df_test['customDimensions'].map(lambda x: str(x).replace("\'", "\""))
df_test = df_test.drop('hits',axis=1)

df_test = apply_sepration(df_test)

print(len(df_train.groupby(['fullVisitorId']).sum()))
print(len(df_test.groupby(['fullVisitorId']).sum()))

df_train.to_csv('{}train_expanded.csv'.format(data_path),index = False)
df_test.to_csv('{}test_expanded.csv'.format(data_path),index = False)