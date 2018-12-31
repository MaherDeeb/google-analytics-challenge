import csv
import sys
from utilities import general_utilities
from utilities import ETL_utilities

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
# TODO: list the columns that contain categorical data as well but ignored and explain why they are ignored.
