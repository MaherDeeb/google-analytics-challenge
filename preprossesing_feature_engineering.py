# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 20:11:49 2018

@author: Maher Deeb
"""

import pandas as pd
from datetime import datetime

data_path = '../'

df_train = pd.read_csv('{}train_clean.csv'.format(data_path), engine='python')
df_test = pd.read_csv('{}test_clean.csv'.format(data_path), engine='python')

def one_hot(df,column):
    
    df_ohc = pd.get_dummies(df[column])
    df_ohc.columns = ['_'.join((column, str(x))) for x in range(len(df_ohc.columns))]
    df = pd.concat([df,df_ohc],axis = 1)
   
    return df

def new_features(df):
    df['month'] = df['date'].map(lambda x: str(x)[4:6])
    df['day'] = df['date'].map(lambda x: str(x)[6:8])
    
    df['sessionId_r'] = df['sessionId'].map(lambda x: str(x).split('_')[0])
    df['sessionId_l'] = df['sessionId'].map(lambda x: str(x).split('_')[1])
    
    df['dayname'] = df['date'].map(lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%A'))
    df['weekday'] = (( df['dayname']=='Saturday') | (df['dayname']=='Sunday'))*1
    
    df['visitNumber_10'] = (df.visitNumber <= 10)*1
    df['visitNumber_50'] = ((df.visitNumber <= 50) & (df.visitNumber > 10))*1
    df['visitNumber_100'] = ((df.visitNumber <= 100) & (df.visitNumber > 50))*1
    df['visitNumber_150'] = ((df.visitNumber <= 150) & (df.visitNumber > 100))*1
    df['visitNumber_200'] = ((df.visitNumber <= 200) & (df.visitNumber > 150))*1
    df['visitNumber_250'] = ((df.visitNumber <= 250) & (df.visitNumber > 200))*1
    df['visitNumber_300'] = ((df.visitNumber > 250))*1
    
    df['start_hour'] = df['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%H'))
    df['start_min'] = df['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%M'))
    df['start_sec'] = df['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%S'))
    df['start_am_pm'] = df['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%p'))
    df['daynr'] = df['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%j'))
    df['weeknr'] = df['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%U'))
    
    df['Chrome'] = (df['browser']=='Chrome')*1
    df['Safari'] = ((df['browser']=='Safari') | (df['browser']=='Safari (in-app)'))*1
    df['Firefox'] = (df['browser']=='Firefox')*1
    df['Internet Explorer'] = (df['browser']=='Internet Explorer')*1
    df['Android'] = ((df['browser']=='Android Webview') | (df['browser']=='Android Browser') | (df['browser']=='Samsung Internet'))*1
    df['Edge'] = (df['browser']=='Edge')*1
    
    df['Windows'] = ((df['operatingSystem']=='Windows') | (df['operatingSystem']=='Windows Phone'))*1
    df['Macintosh'] = ((df['operatingSystem']=='Macintosh') | (df['operatingSystem']=='iOS'))*1
    df['Android'] = ((df['operatingSystem']=='Android') | (df['operatingSystem']=='Chrome OS'))*1
    df['Linux'] = (df['operatingSystem']=='Linux')*1
    
    df['city_none'] = ((df['city']=='not available in demo dataset') | (df['city']=='(not set)'))*1
    df['metro_none'] = ((df['metro']=='not available in demo dataset') | (df['metro']=='(not set)'))*1
    
    #df['networkDomain_none'] = ((df['networkDomain']=='unknown.unknown') | (df['networkDomain']=='(not set)'))*1
    df['networkDomain_start'] = df['networkDomain'].map(lambda x: str(x).split('.')[0])
    df['networkDomain_end'] = df['networkDomain'].map(lambda x: str(x).split('.')[1] if len([str(x).split('.'),'other'])>2 else None)
    df['networkDomain_end_end'] = df['networkDomain'].map(lambda x: str(x).split('.')[2] if len([str(x).split('.'),'other'])>3 else None)
    
    #df['region_none'] = ((df['region']=='not available in demo dataset') | (df['region']=='(not set)'))*1
    
    df['hits_10'] = (df.hits <= 10)*1
    df['hits_50'] = ((df.hits <= 50) & (df.hits > 10))*1
    df['hits_100'] = ((df.hits <= 100) & (df.hits > 50))*1
    df['hits_150'] = ((df.hits <= 150) & (df.hits > 100))*1
    df['hits_200'] = ((df.hits <= 200) & (df.hits > 150))*1
    df['hits_250'] = ((df.hits <= 250) & (df.hits > 200))*1
    df['hits_300'] = ((df.hits > 250))*1
    
    df['pageviews_10'] = (df.pageviews <= 10)*1
    df['pageviews_50'] = ((df.pageviews <= 50) & (df.pageviews > 10))*1
    df['pageviews_100'] = ((df.pageviews <= 100) & (df.pageviews > 50))*1
    df['pageviews_150'] = ((df.pageviews <= 150) & (df.pageviews > 100))*1
    df['pageviews_200'] = ((df.pageviews <= 200) & (df.pageviews > 150))*1
    df['pageviews_250'] = ((df.pageviews <= 250) & (df.pageviews > 200))*1
    df['pageviews_300'] = ((df.pageviews > 250))*1
    
    df['adContent_na'] = (df['adContent'].isnull())*1
    df['adContent_google'] =(df['adContent'].str.contains('Google'))*1
    
    return df

#'browser',campaign,'page'
columns = ['channelGrouping','deviceCategory',
           'continent', 'subContinent','medium']

for column in columns:
    
    df_train = one_hot(df_train,column)
    df_test = one_hot(df_test,column)
    print(column)
    print(len(df_train.columns))
    print(len(df_test.columns))
    
df_train = new_features(df_train)
df_test = new_features(df_test)

df_train.to_csv('{}train_one_hot.csv'.format(data_path),index = False)
df_test.to_csv('{}test_one_hot.csv'.format(data_path),index = False)
    
    
    
    
    
    
    
    
    
