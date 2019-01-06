import csv
import sys
from utilities import general_utilities
from utilities import ETL_utilities
from utilities import ML_utilities
import pandas as pd
import numpy as np

# define the path where the data are
data_path = './data/'

general_utilities.what_does_the_code_do_now(answer="Preparing the environment to work with large csv files")
maxInt = sys.maxsize
decrement = True
while decrement:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.
    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt / 10)
        decrement = True
# ETL job
general_utilities.what_does_the_code_do_now(answer="Applying the ETL job")
print("starting the ETL job...")
file_name = "train_v2.csv"
rows_amount = 2000
print("loading data from the file {}. Only {} rows will be read...".format(file_name, rows_amount))
train_dataframe = ETL_utilities.load_data(data_path, file_name, rows_amount)
print("flatting the columns that contains nested json structure in the training dataset")
train_dataframe = ETL_utilities.apply_separation(train_dataframe)
print("done...")
file_name = "test_v2.csv"
rows_amount = 2000
print("loading data from the file {}. Only {} rows will be read...".format(file_name, rows_amount))
test_dataframe = ETL_utilities.load_data(data_path, file_name, rows_amount)
print("flatting the columns that contains nested json structure in the training dataset")
test_dataframe = ETL_utilities.apply_separation(test_dataframe)
print("done...")
general_utilities.what_does_the_code_do_now(answer="Saving the expanded dataframes as csv files")
print("saving the training dataset under the name train_expanded.csv")
train_dataframe.to_csv('{}train_expanded.csv'.format(data_path), index=False)
print("saving the testing dataset under the name test_expanded.csv")
test_dataframe.to_csv('{}test_expanded.csv'.format(data_path), index=False)

# Feature Engineering
general_utilities.what_does_the_code_do_now(answer="Dropping unnecessary columns")
print("Dropping unnecessary columns from the training dataset")
train_dataframe = ETL_utilities.drop_unnecessary_columns(train_dataframe)
# it was found that trafficSource.campaignCode column exists in the training dataset but not in the testing dataset.
# Therefore, it will be dropped as well.
if 'trafficSource.campaignCode' in list(train_dataframe.columns):
    train_dataframe = train_dataframe.drop(['trafficSource.campaignCode'], axis=1)
print("Dropping unnecessary columns from the testing dataset")
test_dataframe = ETL_utilities.drop_unnecessary_columns(test_dataframe)
print("saving the training dataset under the name train_clean.csv")
train_dataframe.to_csv('{}train_clean.csv'.format(data_path), index=False)
print("saving the testing dataset under the name test_clean.csv")
test_dataframe.to_csv('{}test_clean.csv'.format(data_path), index=False)

general_utilities.what_does_the_code_do_now(answer="Feature Engineering")
# those columns contain categorical data.
columns = ['channelGrouping', 'device.deviceCategory', 'geoNetwork.continent',
           'geoNetwork.subContinent', 'trafficSource.medium']
# ignored columns are columns that contain more classes in the test dataset than the train dataset.
# In this case, after the OHC many columns, which appear in test dataset, will not appear in the train dataset
ignored_columns = ['device.browser', 'trafficSource.campaign', 'page']
# applying ohc:
for column in columns:
    print("applying ohc on the column {}".format(column))
    train_dataframe = ETL_utilities.one_hot_code(train_dataframe, column)
    print("Number of columns in the train dataset after"
          " applying the ohc on column {} is {}".format(column, len(train_dataframe.columns)))
    test_dataframe = ETL_utilities.one_hot_code(test_dataframe, column)
    print("Number of columns in the test dataset after"
          " applying the ohc on column {} is {}".format(column, len(test_dataframe.columns)))
print('===================================================')
print("create new features from the current data in the train dataset")
df_train = ETL_utilities.create_new_features(train_dataframe)
print("done")
print("create new features from the current data in the train dataset")
df_test = ETL_utilities.create_new_features(test_dataframe)
print("done")
general_utilities.what_does_the_code_do_now(answer="Saving the expanded dataframes as csv files")
print("saving the training dataset under the name train_feature_engineering.csv")
train_dataframe.to_csv('{}train_feature_engineering.csv'.format(data_path), index=False)
print("saving the testing dataset under the name test_feature_engineering.csv")
test_dataframe.to_csv('{}test_feature_engineering.csv'.format(data_path), index=False)
general_utilities.what_does_the_code_do_now(answer="For more feature, run 'geocoding_add_features.R'")
input("After running the R script, press Enter to continue ...")
r_script_run = input("If you want to read the results from R script, press 1. Otherwise, press 0. Then press Enter")
if r_script_run == 1:
    print("You choose to load the results of the R script")
    print("loading the training dataset...train_Localfeatures.csv")
    train_dataframe = pd.read_csv('{}train_Localfeatures.csv'.format(data_path), engine='python',
                                  dtype={'fullVisitorId': 'object'})
    print("done.")
    print("loading the testing dataset...test_localfeatures.csv")
    test_dataframe = pd.read_csv('{}test_localfeatures.csv'.format(data_path), engine='python',
                                 dtype={'fullVisitorId': 'object'})
    print("done.")
else:
    print("You choose NOT to load the results of the R script")
general_utilities.what_does_the_code_do_now(answer="Applying Machine learning")
train_dataframe, test_dataframe, train_target, test_target = ML_utilities.target_preparation(
    train_dataframe, test_dataframe)
general_utilities.what_does_the_code_do_now(answer="Decoding strings with integers")
train_dataframe, test_dataframe, _ = ML_utilities.decode_strings_with_integers(
    train_dataframe, test_dataframe)
general_utilities.what_does_the_code_do_now(answer="Decoding strings based on occurrence frequency")
train_dataframe, test_dataframe, _ = ML_utilities.decode_strings_with_appearance_frequency(
    train_dataframe, test_dataframe)
general_utilities.what_does_the_code_do_now(answer="Splitting data to train and valid datasets")
x_train, x_valid, y_train, y_valid = ML_utilities.split_data(
    train_dataframe, train_target, valid_size=0.1, random_state=0)
general_utilities.what_does_the_code_do_now(answer="Applying features mapping")
map_degree = 0
terms_mix_degree = 2
features_numbers_list = []
# if features_numbers_list = [], all features are considered
print("applying features mapping on the training dataset --> map_degree = {}, terms_mix_degree = {},"
      " features_numbers_list = {}".format(map_degree, terms_mix_degree, features_numbers_list))
x_train, _ = ML_utilities.features_mapping(x_train, map_degree=map_degree, terms_mix_degree=terms_mix_degree,
                                           features_numbers_list=features_numbers_list)
print("applying features mapping on validating dataset --> map_degree = {}, terms_mix_degree = {},"
      " features_numbers_list = {}".format(map_degree, terms_mix_degree, features_numbers_list))
x_valid, _ = ML_utilities.features_mapping(x_valid, map_degree=map_degree, terms_mix_degree=terms_mix_degree,
                                           features_numbers_list=features_numbers_list)
print("applying features mapping on the testing dataset --> map_degree = {}, terms_mix_degree = {},"
      " features_numbers_list = {}".format(map_degree, terms_mix_degree, features_numbers_list))
x_test, _ = ML_utilities.features_mapping(test_dataframe.values,
                                          map_degree=map_degree, terms_mix_degree=terms_mix_degree,
                                          features_numbers_list=features_numbers_list)
general_utilities.what_does_the_code_do_now(answer="adding the bias to the datasets as series of ones")

