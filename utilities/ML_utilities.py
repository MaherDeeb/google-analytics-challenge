import pandas as pd


def decode_strings_with_integers(train_dataframe, test_dataframe):
    """
    This function is going to replace the strings inside the categorical columns with integers
    :param train_dataframe: pandas dataframe that contains the original data with new created features
    :param test_dataframe: pandas dataframe that contains the original data with new created features
    :return: the dataframes after the decoding process besides a total dataframe which is the concatenation of both the
    training and testing dataframes
    """
    print("concatenating both the train and the test dataset in one frame before the decoding process")
    total_dataframe = pd.concat([train_dataframe, test_dataframe], sort=False)
    total_dataframe.index = range(len(total_dataframe['channelGrouping']))
    train_decoded_dataframe = train_dataframe
    test_decoded_dataframe = test_dataframe
    print("The column 'fullVisitorId' will not be decoded since this column should be the index when submitting"
          " the results to Kaggle")
    for column_i in train_dataframe.columns[train_dataframe.dtypes == 'object']:
        if column_i != 'fullVisitorId':
            print("decoding the column {}".format(column_i))
            total_dataframe[column_i] = total_dataframe[column_i].factorize()[0]
            train_decoded_dataframe[column_i] = total_dataframe.loc[range(train_dataframe.shape[0]), column_i].values
            test_decoded_dataframe[column_i] = total_dataframe.loc[
                range(train_dataframe.shape[0], train_dataframe.shape[0] + test_dataframe.shape[0]), column_i].values

    return train_decoded_dataframe, test_decoded_dataframe, total_dataframe


def decode_strings_with_appearance_frequency(train_dataframe, test_dataframe):
    total_dataframe = pd.concat([train_dataframe, test_dataframe], sort=False)
    total_dataframe.index = range(len(total_dataframe['channelGrouping']))
    train_decoded_dataframe = train_dataframe
    test_decoded_dataframe = test_dataframe
    print("Please NOTICE that the column 'fullVisitorId' is considered")
    for column_i in train_dataframe.columns[train_dataframe.dtypes == 'object']:
        print("decoding the column {} based on value appearance frequency".format(column_i))
        encoding = total_dataframe.groupby([column_i]).size()
        encoding /= len(total_dataframe)
        total_dataframe[column_i + '_freq'] = total_dataframe[column_i].map(encoding)
        train_decoded_dataframe[column_i + '_freq'] = total_dataframe.loc[
            range(train_dataframe.shape[0]), column_i + '_freq'].values
        test_decoded_dataframe[column_i + '_freq'] = total_dataframe.loc[
            range(train_dataframe.shape[0], train_dataframe.shape[0] +
                  test_dataframe.shape[0]), column_i + '_freq'].values

    return train_decoded_dataframe, test_decoded_dataframe
