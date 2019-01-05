import pandas as pd
import numpy as np
import itertools as it


def target_preparation(train_dataframe, test_dataframe):
    """
    this function prepare the target as the competition in Kaggle suggests
    :param train_dataframe: pandas dataframe that contains the training dataset
    :param test_dataframe: pandas dataframe that contains the testing dataset
    :return: the training and testing dataframes after dropping the target from them and two other series for the
     targets in both training and testing datasets
    """
    print("defining the regression problem: the target is 'totals.transactionRevenue'")
    print("replacing the missing values of the target with 0s and define the type of the data as 'int64'")
    train_target = train_dataframe['totals.transactionRevenue'].fillna(0).astype('int64')
    test_target = test_dataframe['totals.transactionRevenue'].fillna(0).astype('int64')
    print("applying the log1p on the target ('totals.transactionRevenue') and the column "
          "('totals.totalTransactionRevenue') since their values are in same range")
    train_target = np.log1p(train_target)
    test_target = np.log1p(test_target)
    train_dataframe['totals.totalTransactionRevenue'] = np.log1p(train_dataframe['totals.totalTransactionRevenue'])
    test_dataframe['totals.totalTransactionRevenue'] = np.log1p(test_dataframe['totals.totalTransactionRevenue'])
    print("dropping the target from the train and test datasets")
    train_dataframe = train_dataframe.drop('totals.transactionRevenue', axis=1)
    test_dataframe = test_dataframe.drop('totals.transactionRevenue', axis=1)
    return train_dataframe, test_dataframe, train_target, test_target


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


def train_features_mapping(x_train, map_degree=2, terms_mix_degree=2, y_train=[], minimum_correlation = 0.05,
                           features_numbers_list=[]):
    """
    this function introduces the possible nonlinearity and dependency between the independent features. For example
    if we have 2 features x1 and x2, after applying the function, we get the following features if the map_degree = 2
    x1, x2, x1^2, x2^2, x1*x2
    it is important to apply the map_features_test function after applying this function to be sure that the test data
    are mapped exactly as the train datasets
    :param x_train: numpy arrary with size (nxm) where n is the number of rows and m is the number of the features
    :param map_degree: the nonlinearty degree. For example map_degree = 2 --> x and x^2 will be considered. If
     map_degree = 3 --> x, x^2 and x^3 will be considered. If 0 is passed, this type of nonlinearity will be not
     considered
    :param terms_mix_degree: the degree of the introduced dependency between features.
    For example, if mix_degree = 2 --> x1, x2, x3, x1*x2 x1*x3 and x2*x3 will be considered.
     If mix_degree = 3 -> x1, x2, x3, x1*x2 x1*x3, x2*x3, x1*x2*x3. If 0 is passed, this type of nonlinearty will not
     be considered
    :param y_train: numpy array with size (nx1). If it is passed, the minimum_correlation has to be passed. In this
     case the correlation between the new created term (e.g. x^2 or x1*x2) and the target y_train will be calculated.
      If the calculated correlation is less than the minimum_correlation, the new term will be ignored.
       Otherwise it will be considered and will be added to the x_train at the end
    :param minimum_correlation: a number between 0 and 1 which is the threshold where the new calculated terms will be
    important considered or they will be ignored.
    :param features_numbers_list: list of integers that represents the features that should be considered by applying
    this function. if not passed, all features will be considered. For example, if there is 3 features x1, x2, x3 which
     has the following order indexes [0, 1, 2] in x_train, and features_numbers_list = [1, 2] are passed, then only
     x2 and x3 (x_train[:,1], x_train[:,2]) will be considered when applying the function.
    :return:
    """
    # consider only the required features when applying the function
    if len(features_numbers_list) == 0:
        features_numbers_list = list(range(x_train.shape[1]))
    # initiate the correlation between the new term and the target y_train
    if len(y_train) > 0:
        correlation_term_i_y_train = pd.DataFrame(y_train)
        print(correlation_term_i_y_train.size)
    # initiate the list that contains the shapes which used to combine the features e.g. x1*x2
    terms_shape_list = []
    if map_degree > 1:
        for degree_i in range(2, map_degree+1):
            x_train = np.concatenate((x_train, x_train[:, features_numbers_list]**degree_i), axis=1)

    if terms_mix_degree > 1:
        for i in range(2, terms_mix_degree + 1):
            if map_degree > 1:
                mapping_shape = list(it.combinations_with_replacement(range(features_numbers_list[0],
                                                                    features_numbers_list[-1]+1), i))
            else:
                mapping_shape = list(it.combinations(range(features_numbers_list[0], features_numbers_list[-1] + 1), i))
            print(mapping_shape)
            # initiate the term that should be calculated
            for mapping_shape_i in mapping_shape:
                if len(set(mapping_shape_i)) > 1:
                    term_i = np.ones((len(x_train), 1))
                    for feature_i in mapping_shape_i:
                        term_i[:, 0] = np.multiply(term_i[:, 0], x_train[:, feature_i])
                    x_train = np.append(x_train.T, term_i.reshape(1, -1), axis=0).T
                    terms_shape_list.append(mapping_shape_i)
    return x_train.shape, terms_shape_list

train_features_mapping(x,2,2,y,0.05)


def map_features_test(X, com_x_f):
    com_x = com_x_f
    for j in range(len(com_x)):
        X = np.append(X.T, np.array(X[:, com_x[j][0]] * X[:, com_x[j][1]]).reshape(1, -1), axis=0).T
    return X
