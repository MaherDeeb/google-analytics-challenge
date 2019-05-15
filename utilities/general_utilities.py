import pandas as pd
import datetime
import time


def what_does_the_code_do_now(answer="the code is doing something"):
    print("=" * (len(answer) + 2))
    print("*{}*".format(answer))
    print("=" * (len(answer) + 2))

    return


def aggregate_predictions(predictions):
    """
    The competition asks to return the the sum of all transactions per user.
    The function aggregates the results by user and sum the PredictedLogRevenue
    columns. The function will be called by the "submit_to_kaggle" function
    :param predictions: pandas dataframe with 2 columns and 804684 rows
    :return:
        prediction_to_submission: pandas dataframe with 2 columns and 617242 rows
    """
    prediction_to_submission = predictions.groupby(['fullVisitorId']).sum()
    return prediction_to_submission


def submit_to_kaggle(path='./data/', prediction_file='predict.csv'):
    """
    The function prepare the finial csv file that should be submitted to kaggle
    :param path: where the predict.csv file and where the submission should be saved
    :param prediction_file: the file that contain the predictions without aggregations
    :return:
        nothing
    """
    print("Start creating the submission file... Please wait")
    predictions = pd.read_csv(''.join((path, prediction_file)), dtype={'fullVisitorId': 'object'})
    prediction_to_submission = aggregate_predictions(predictions)
    print('The number of the rows of the grouped prediction is --> {}',format(len(prediction_to_submission)))
    file_name = str(round(time.mktime((datetime.datetime.now().timetuple()))))
    print('The name of the submission file is {}_submit.csv'.format(file_name))
    prediction_to_submission.to_csv(path + '{}_submit.csv'.format(file_name))
    print("The submission file is ready...!! Done!!")
