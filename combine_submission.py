# -*- coding: utf-8 -*-
"""
Created on Sun Jun 24 22:02:44 2018

@author: Maher Deeb
"""

import pandas as pd
import datetime, time

path = './data/'
df_best_submit1 = pd.read_csv('{}1542461280_submit.csv'.format(path))
df_best_submit2 = pd.read_csv('{}1542402842_submit.csv'.format(path))

df_best_submit = df_best_submit1

df_best_submit['PredictedLogRevenue'] = (1/1.6)*df_best_submit['PredictedLogRevenue']+(1/0.8)*df_best_submit2['PredictedLogRevenue']
df_best_submit['PredictedLogRevenue'] = df_best_submit['PredictedLogRevenue']/((1/1.6)+(1/0.8))

df_best_submit.to_csv('{}{}_submit.csv'.format(path,str(round(time.mktime((datetime.datetime.now().timetuple()))))),index=False)

