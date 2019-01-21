import pandas as pd
import json
from utilities import general_utilities
from datetime import datetime
import re

def separate_json(series: pd.Series) -> pd.DataFrame():
    """

    Args:
        series: Series before json parsing

    Returns: DataFrame
    """
    if isinstance(series[0], str):
        return pd.DataFrame(json.loads(s) for s in series)
    return pd.DataFrame(s for s in series)


def load_data(path, file_name, rows_amount):
    """
    This function loads or read a part of the csv file. Loading the whole file requires a machine with large memory
    :param path: (string) the path where the data is
    :param file_name: (string) the name of the file that should be loaded e.g. test_v2.csv
    :param rows_amount: (int) this is the amount of the rows that should be read from the csv file to avoid memory error
    :return: pandas dataframe
    """
    if rows_amount:
        general_utilities.what_does_the_code_do_now(
            answer="reading {} rows from the file {}".format(rows_amount, file_name))
        # the column "fullVisitorId" should be explicitly given object type otherwise it will be defined as integer
        # which cause an error when submitting the results.
        dataframe = pd.read_csv('{}{}'.format(path, file_name), nrows=rows_amount, engine='python',
                                dtype={'fullVisitorId': 'object'})
    else:
        general_utilities.what_does_the_code_do_now(
            answer="reading all data from the file {}".format(file_name))
        dataframe = pd.read_csv('{}{}'.format(path, file_name), engine='python', dtype={'fullVisitorId': 'object'})
    # because the hits columns is too large and nested we drop it for now.
    # If you want to keep it, a memory error can be expected
    dataframe = dataframe.drop('hits', axis=1)
    # the single quote should be changed to double quote in the column customDimensions otherwise an error while flatten
    # the nested json columns will be obtained
    dataframe['customDimensions'] = dataframe['customDimensions'].map(lambda x: str(x).replace("\'", "\""))

    return dataframe


def apply_separation(dataframe_original):
    """
    This function will take the columns that contain nested json structure and flat them to different columns with no
    json structure inside it
    :param dataframe_original: pandas dataframe that contains nested json columns
    :return: pandas dataframe with flatten json columns
    """
    # those columns contain nested json structure that should be flatten
    columns = ['customDimensions', 'device', 'geoNetwork', 'totals', 'trafficSource',
               'trafficSource.adwordsClickInfo', 'customDimensions.0',
               'trafficSource.adwordsClickInfo.targetingCriteria']
    for column_name in columns:
        if column_name == 'customDimensions.0':
            general_utilities.what_does_the_code_do_now(
                answer="replacing invalid values that cause error "
                       "while flatten the json structure in column {}".format(column_name))
            dataframe_original[column_name] = dataframe_original[column_name]. \
                map(lambda x: {'index': 'None'} if x is None else x)
        general_utilities.what_does_the_code_do_now(
            answer="flatten the json structure in column {} and assigning reasonable columns names".format(column_name))
        dataframe = separate_json(dataframe_original[column_name])
        dataframe.columns = ['{}.{}'.format(column_name, x) for x in list(dataframe.columns)]
        print("there are {} columns will be added to the dataframe after"
              " flatten the original column".format(len(list(dataframe.columns))))
        print("the columns are:")
        print(list(dataframe.columns))
        general_utilities.what_does_the_code_do_now(
            answer="include the flatten columns in the original dataframe and drop the old column with json structure")
        dataframe_original = dataframe_original.join(dataframe)
        dataframe_original = dataframe_original.drop(column_name, axis=1)
    # the column 'trafficSource.adwordsClickInfo.targetingCriteria.0' will be dropped because it has invalid values
    # which raise errors later. The problem can be solved with more investigation.
    dataframe_original = dataframe_original.drop('trafficSource.adwordsClickInfo.targetingCriteria.0', axis=1)
    return dataframe_original


def drop_unnecessary_columns(dataframe):
    """
    This function drops the columns that their values doesn't change
    :param dataframe: pandas data frame after flatting the columns that have json structure in the dataset
    :return: the dataframe after dropping the columns that should be dropped
    """
    const_cols_train = [c for c in dataframe.columns if dataframe[c].nunique(dropna=False) == 1]
    print("The following columns will be dropped because their values doesn't change in all rows:")
    print(const_cols_train)
    # Drop the columns with constant values
    dataframe = dataframe.drop(const_cols_train, axis=1, inplace=False)

    return dataframe


def one_hot_code(dataframe, column):
    """
    This function add columns that contain the one hot coding (ohc) of the categories in the columns which their names
    are given in the 'column' list
    :param dataframe: pandas dataframe. It can be the training or testing dataset
    :param column: list of strings. It contains the columns names that have categorical data
    :return: pandas dataframe that is a combination of both the original and the ohc columns
    """
    dataframe_ohc = pd.get_dummies(dataframe[column])
    dataframe_ohc.columns = ['_'.join((column, str(x))) for x in range(len(dataframe_ohc.columns))]
    dataframe = pd.concat([dataframe, dataframe_ohc], axis=1)

    return dataframe


def create_new_features(dataframe):
    """
    This function creates new features from the current data
    :param dataframe: the pandas dataframe before extracting the new features
    :return: a dataframe that contains the original columns and the new columns after creating the features.
    """
    general_utilities.what_does_the_code_do_now(answer="create new feature from dates")
    # to know how many columns will be added we calculate current_columns_number
    current_columns_number = len(list(dataframe.columns))
    dataframe['month'] = dataframe['date'].map(lambda x: str(x)[4:6])
    dataframe['day'] = dataframe['date'].map(lambda x: str(x)[6:8])
    dataframe['dayname'] = dataframe['date'].map(lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%A'))
    dataframe['weekday'] = ((dataframe['dayname'] == 'Saturday') | (dataframe['dayname'] == 'Sunday')) * 1
    dataframe['start_hour'] = dataframe['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%H'))
    dataframe['start_min'] = dataframe['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%M'))
    dataframe['start_sec'] = dataframe['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%S'))
    dataframe['start_am_pm'] = dataframe['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%p'))
    dataframe['daynr'] = dataframe['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%j'))
    dataframe['weeknr'] = dataframe['visitStartTime'].map(lambda x: datetime.fromtimestamp(x).strftime('%U'))
    print("there are {} new columns added".format(len(list(dataframe.columns))-current_columns_number))
    # to know how many columns will be added we calculate current_columns_number
    current_columns_number = len(list(dataframe.columns))
    general_utilities.what_does_the_code_do_now(answer="create new feature using data binning")
    dataframe['visitNumber_10'] = (dataframe.visitNumber <= 10) * 1
    dataframe['visitNumber_50'] = ((dataframe.visitNumber <= 50) & (dataframe.visitNumber > 10)) * 1
    dataframe['visitNumber_100'] = ((dataframe.visitNumber <= 100) & (dataframe.visitNumber > 50)) * 1
    dataframe['visitNumber_150'] = ((dataframe.visitNumber <= 150) & (dataframe.visitNumber > 100)) * 1
    dataframe['visitNumber_200'] = ((dataframe.visitNumber <= 200) & (dataframe.visitNumber > 150)) * 1
    dataframe['visitNumber_250'] = ((dataframe.visitNumber <= 250) & (dataframe.visitNumber > 200)) * 1
    dataframe['visitNumber_300'] = (dataframe.visitNumber > 250) * 1
    dataframe['totals.hits'] = dataframe['totals.hits'].astype('int64')
    dataframe['totals.hits_10'] = (dataframe['totals.hits'] <= 10) * 1
    dataframe['totals.hits_50'] = ((dataframe['totals.hits'] <= 50) & (dataframe['totals.hits'] > 10)) * 1
    dataframe['totals.hits_100'] = ((dataframe['totals.hits'] <= 100) & (dataframe['totals.hits'] > 50)) * 1
    dataframe['totals.hits_150'] = ((dataframe['totals.hits'] <= 150) & (dataframe['totals.hits'] > 100)) * 1
    dataframe['totals.hits_200'] = ((dataframe['totals.hits'] <= 200) & (dataframe['totals.hits'] > 150)) * 1
    dataframe['totals.hits_250'] = ((dataframe['totals.hits'] <= 250) & (dataframe['totals.hits'] > 200)) * 1
    dataframe['totals.hits_300'] = (dataframe['totals.hits'] > 250) * 1
    dataframe['totals.pageviews'] = dataframe['totals.pageviews'].fillna(0).astype('int64')
    dataframe['totals.pageviews_10'] = (dataframe['totals.pageviews'] <= 10) * 1
    dataframe['totals.pageviews_50'] = ((dataframe['totals.pageviews'] <= 50) & (
                dataframe['totals.pageviews'] > 10)) * 1
    dataframe['totals.pageviews_100'] = ((dataframe['totals.pageviews'] <= 100) & (
                dataframe['totals.pageviews'] > 50)) * 1
    dataframe['totals.pageviews_150'] = ((dataframe['totals.pageviews'] <= 150) & (
                dataframe['totals.pageviews'] > 100)) * 1
    dataframe['totals.pageviews_200'] = ((dataframe['totals.pageviews'] <= 200) & (
                dataframe['totals.pageviews'] > 150)) * 1
    dataframe['totals.pageviews_250'] = ((dataframe['totals.pageviews'] <= 250) & (
                dataframe['totals.pageviews'] > 200)) * 1
    dataframe['totals.pageviews_300'] = (dataframe['totals.pageviews'] > 250) * 1
    print("there are {} new columns added".format(len(list(dataframe.columns)) - current_columns_number))
    # to know how many columns will be added we calculate current_columns_number
    current_columns_number = len(list(dataframe.columns))
    general_utilities.what_does_the_code_do_now(answer="create new feature regular expressions")
    dataframe['Chrome'] = (dataframe['device.browser'] == 'Chrome') * 1
    dataframe['Safari'] = ((dataframe['device.browser'] == 'Safari') |
                           (dataframe['device.browser'] == 'Safari (in-app)')) * 1
    dataframe['Firefox'] = (dataframe['device.browser'] == 'Firefox') * 1
    dataframe['Internet Explorer'] = (dataframe['device.browser'] == 'Internet Explorer') * 1
    dataframe['Android'] = ((dataframe['device.browser'] == 'Android Webview') | (
            dataframe['device.browser'] == 'Android device.browser') |
                            (dataframe['device.browser'] == 'Samsung Internet')) * 1
    dataframe['Edge'] = (dataframe['device.browser'] == 'Edge') * 1
    dataframe['Windows'] = ((dataframe['device.operatingSystem'] == 'Windows') | (
            dataframe['device.operatingSystem'] == 'Windows Phone')) * 1
    dataframe['Macintosh'] = ((dataframe['device.operatingSystem'] == 'Macintosh') |
                              (dataframe['device.operatingSystem'] == 'iOS')) * 1
    dataframe['Android'] = ((dataframe['device.operatingSystem'] == 'Android') |
                            (dataframe['device.operatingSystem'] == 'Chrome OS')) * 1
    dataframe['Linux'] = (dataframe['device.operatingSystem'] == 'Linux') * 1
    dataframe['geoNetwork.city_none'] = ((dataframe['geoNetwork.city'] == 'not available in demo dataset') | (
            dataframe['geoNetwork.city'] == '(not set)')) * 1
    dataframe['geoNetwork.metro_none'] = ((dataframe['geoNetwork.metro'] == 'not available in demo dataset') | (
            dataframe['geoNetwork.metro'] == '(not set)')) * 1
    dataframe['geoNetwork.networkDomain_start'] = dataframe[
        'geoNetwork.networkDomain'].map(lambda x: str(x).split('.')[0])
    dataframe['geoNetwork.networkDomain_end'] = dataframe['geoNetwork.networkDomain'].map(
        lambda x: str(x).split('.')[1] if len([str(x).split('.'), 'other']) > 2 else None)
    dataframe['geoNetwork.networkDomain_end_end'] = dataframe['geoNetwork.networkDomain'].map(
        lambda x: str(x).split('.')[2] if len([str(x).split('.'), 'other']) > 3 else None)
    dataframe['trafficSource.adContent_na'] = (dataframe['trafficSource.adContent'].isnull()) * 1
    dataframe['trafficSource.adContent_google'] = (dataframe['trafficSource.adContent'].str.contains('Google')) * 1
    dataframe['gcdID_5_last'] = dataframe['trafficSource.adwordsClickInfo.gclId'].map(
        lambda x: str(x)[-5:-1] if len(str(x)) > 3 else None)
    dataframe['google'] = dataframe['trafficSource.keyword'].map(
        lambda x: (re.match(r'.*go+gle.*', str(x).lower()) is not None) * 1)
    dataframe['youtube'] = dataframe['trafficSource.keyword'].map(
        lambda x: (re.match(r'.*youtube.*', str(x).lower()) is not None) * 1)
    dataframe['store'] = dataframe['trafficSource.keyword'].map(
        lambda x: (re.match(r'.*store.*', str(x).lower()) is not None) * 1)
    dataframe['trafficSource.keywords_notprovided'] = dataframe['trafficSource.keyword'].map(
        lambda x: (re.match(r'.*(not provided).*', str(x).lower()) is not None) * 1) + (
                                                   dataframe['trafficSource.keyword'].isnull()) * 1
    print("there are {} new columns added".format(len(list(dataframe.columns)) - current_columns_number))
    # to know how many columns will be added we calculate current_columns_number
    return dataframe
