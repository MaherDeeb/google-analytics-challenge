# -*- coding: utf-8 -*-
"""
Created on Sun Jun 24 22:02:44 2018

@author: Maher Deeb
"""

import pandas as pd
import datetime, time

df_best_submit1 = pd.read_csv('1530501616_submit.csv')
df_best_submit2 = pd.read_csv('1530502716_submit.csv')

df_best_submit = df_best_submit1

df_best_submit['TARGET'] = df_best_submit['TARGET']+df_best_submit2['TARGET']
df_best_submit['TARGET'] = df_best_submit['TARGET']/2

df_best_submit.to_csv('{}_submit.csv'.format(str(round(time.mktime((datetime.datetime.now().timetuple()))))),index=False)

