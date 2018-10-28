# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 12:56:39 2018

@author: Maher Deeb
"""
import datetime, time
import pandas as pd
import sys

def aggregate_pred(pred):
    '''
    The compentition asks to retrun the the sum of all transactions per user.
    The function aggregates the results by user and sum the PredictedLogRevenue
    columns. The function will be called by the "submit_to_kaggle" function
    inputs:
        pred: pandas dataframe with 2 columns and 804684 rows
    output:
        pred_to_sub: pandas dataframe with 2 columns and 617242 rows 
    '''
    pred_to_sub = pred.groupby(['fullVisitorId']).sum()
    return pred_to_sub

def submit_to_kaggle(path = './data/', pred_file = 'predict.csv' ):
    '''
    The function prepare the finial csv file that should be submitted to kaggle
    inputs:
        path: where the predict.csv file and where the submissoin should be 
        saved
        pred_file: the file that contain the predictions without aggregations
    output:
        nothing
    '''
    pred = pd.read_csv(''.join((path,pred_file)))
    pred_to_sub = aggregate_pred(pred)
    pred_to_sub.to_csv(path + '{}_submit.csv'.format(str(round(time.mktime((datetime.datetime.now().timetuple()))))))
    
if __name__ == '__main__':
    
    submit_to_kaggle(*sys.argv[1:] )