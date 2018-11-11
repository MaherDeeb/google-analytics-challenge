# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 20:11:49 2018

@author: Maher Deeb
"""

import pandas as pd
from datetime import datetime
import re

data_path = './data/'

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
    
    #df['sessionId_r'] = df['sessionId'].map(lambda x: str(x).split('_')[0])
    #df['sessionId_l'] = df['sessionId'].map(lambda x: str(x).split('_')[1])
    
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
    
    df['Chrome'] = (df['device.browser']=='Chrome')*1
    df['Safari'] = ((df['device.browser']=='Safari') | (df['device.browser']=='Safari (in-app)'))*1
    df['Firefox'] = (df['device.browser']=='Firefox')*1
    df['Internet Explorer'] = (df['device.browser']=='Internet Explorer')*1
    df['Android'] = ((df['device.browser']=='Android Webview') | (df['device.browser']=='Android device.browser') | (df['device.browser']=='Samsung Internet'))*1
    df['Edge'] = (df['device.browser']=='Edge')*1
    
    df['Windows'] = ((df['device.operatingSystem']=='Windows') | (df['device.operatingSystem']=='Windows Phone'))*1
    df['Macintosh'] = ((df['device.operatingSystem']=='Macintosh') | (df['device.operatingSystem']=='iOS'))*1
    df['Android'] = ((df['device.operatingSystem']=='Android') | (df['device.operatingSystem']=='Chrome OS'))*1
    df['Linux'] = (df['device.operatingSystem']=='Linux')*1
    
    df['geoNetwork.city_none'] = ((df['geoNetwork.city']=='not available in demo dataset') | (df['geoNetwork.city']=='(not set)'))*1
    df['geoNetwork.metro_none'] = ((df['geoNetwork.metro']=='not available in demo dataset') | (df['geoNetwork.metro']=='(not set)'))*1
    
    #df['geoNetwork.networkDomain_none'] = ((df['geoNetwork.networkDomain']=='unknown.unknown') | (df['geoNetwork.networkDomain']=='(not set)'))*1
    df['geoNetwork.networkDomain_start'] = df['geoNetwork.networkDomain'].map(lambda x: str(x).split('.')[0])
    df['geoNetwork.networkDomain_end'] = df['geoNetwork.networkDomain'].map(lambda x: str(x).split('.')[1] if len([str(x).split('.'),'other'])>2 else None)
    df['geoNetwork.networkDomain_end_end'] = df['geoNetwork.networkDomain'].map(lambda x: str(x).split('.')[2] if len([str(x).split('.'),'other'])>3 else None)
    
    #df['geoNetwork.region_none'] = ((df['geoNetwork.region']=='not available in demo dataset') | (df['geoNetwork.region']=='(not set)'))*1
    
    df['totals.hits_10'] = (df['totals.hits'] <= 10)*1
    df['totals.hits_50'] = ((df['totals.hits'] <= 50) & (df['totals.hits'] > 10))*1
    df['totals.hits_100'] = ((df['totals.hits'] <= 100) & (df['totals.hits'] > 50))*1
    df['totals.hits_150'] = ((df['totals.hits'] <= 150) & (df['totals.hits'] > 100))*1
    df['totals.hits_200'] = ((df['totals.hits'] <= 200) & (df['totals.hits'] > 150))*1
    df['totals.hits_250'] = ((df['totals.hits'] <= 250) & (df['totals.hits'] > 200))*1
    df['totals.hits_300'] = ((df['totals.hits'] > 250))*1
    
    df['totals.pageviews_10'] = (df['totals.pageviews'] <= 10)*1
    df['totals.pageviews_50'] = ((df['totals.pageviews'] <= 50) & (df['totals.pageviews'] > 10))*1
    df['totals.pageviews_100'] = ((df['totals.pageviews'] <= 100) & (df['totals.pageviews'] > 50))*1
    df['totals.pageviews_150'] = ((df['totals.pageviews'] <= 150) & (df['totals.pageviews'] > 100))*1
    df['totals.pageviews_200'] = ((df['totals.pageviews'] <= 200) & (df['totals.pageviews'] > 150))*1
    df['totals.pageviews_250'] = ((df['totals.pageviews'] <= 250) & (df['totals.pageviews'] > 200))*1
    df['totals.pageviews_300'] = ((df['totals.pageviews'] > 250))*1
    
    df['trafficSource.adContent_na'] = (df['trafficSource.adContent'].isnull())*1
    df['trafficSource.adContent_google'] =(df['trafficSource.adContent'].str.contains('Google'))*1
    
    df['gcdID_5_last'] =df['trafficSource.adwordsClickInfo.gclId'].map(lambda x: str(x)[-5:-1] if len(str(x))>3 else None)
    
    df['google'] = df['trafficSource.keyword'].map(lambda x: (re.match(r'.*go+gle.*', str(x).lower()) is not None)*1)
    df['youtube'] =df['trafficSource.keyword'].map(lambda x: (re.match(r'.*youtube.*', str(x).lower()) is not None)*1)
    df['store']= df['trafficSource.keyword'].map(lambda x: (re.match(r'.*store.*', str(x).lower()) is not None)*1)
    df['trafficSource.keywords_notprovided']= df['trafficSource.keyword'].map(lambda x: (re.match(r'.*(not provided).*', str(x).lower()) is not None)*1)+(df['trafficSource.keyword'].isnull())*1
    
    return df

#'device.browser',trafficSource.campaign,'page'
columns = ['channelGrouping','device.deviceCategory',
           'geoNetwork.continent', 'geoNetwork.subContinent','trafficSource.medium']

for column in columns:
    
    df_train = one_hot(df_train,column)
    df_test = one_hot(df_test,column)
    print(column)
    print(len(df_train.columns))
    print(len(df_test.columns))
    
df_train = new_features(df_train)
df_test = new_features(df_test)

df_train.to_csv('{}train_feature_engineering.csv'.format(data_path),index = False)
df_test.to_csv('{}test_feature_engineering.csv'.format(data_path),index = False)

print(len(df_train.groupby(['fullVisitorId']).sum()))
print(len(df_test.groupby(['fullVisitorId']).sum()))