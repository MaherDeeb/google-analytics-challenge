import pandas as pd
import json
from utilities import general_utilities


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
    general_utilities.what_does_the_code_do_now(
        answer="reading {} rows from the file {}".format(rows_amount, file_name))
    # the column "fullVisitorId" should be explicitly given object type otherwise it will be defined as integer
    # which cause an error when submitting the results.
    dataframe = pd.read_csv('{}{}'.format(path, file_name), nrows=rows_amount, engine='python',
                            dtype={'fullVisitorId': 'object'})
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
