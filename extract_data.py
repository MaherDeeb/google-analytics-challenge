import json
import pandas as pd
from pandas.io.json import json_normalize
  
# Convert json fields in the train.csv file to flattend csv format
# This function is taken from kernel by SRK
def load_dataframe(csv_path, nrows):
    JSON_COLUMNS = ['device', 'geoNetwork', 'totals', 'trafficSource'] # list of JSON columns    
    # Read data from CSV file
    # fullVisitorID are loaded as strings, in order for all Id's to be properly unique!
    dataframe = pd.read_csv(csv_path, converters = {column: json.loads for column in JSON_COLUMNS},
                            dtype = {'fullVisitorId': 'str'}, nrows = nrows)    
    for column in JSON_COLUMNS:
        column_as_df  = json_normalize(dataframe[column]) # “Normalize” semi-structured JSON data into a flat table 
        column_as_df.columns = [f"{column}.{subcolumn}" for subcolumn in column_as_df.columns]
        dataframe = dataframe.drop(column, axis = 1).merge(column_as_df, right_index = True, left_index = True) #Drop specified labels from columns. 
        # and Merge DataFrame objects by performing a database-style join operation by columns or indexes.     
    return dataframe

# Convert json fields in the test.csv file to flattend csv format
# This function is taken from kernel by SRK
def load_testframe(test_path, nrows):
    JSON_COLUMNS = ['device', 'geoNetwork', 'totals', 'trafficSource'] # list of JSON columns    
    # Read data from CSV file
    # fullVisitorID are loaded as strings, in order for all Id's to be properly unique!
    testframe = pd.read_csv(test_path, converters = {column: json.loads for column in JSON_COLUMNS},
                            dtype = {'fullVisitorId': 'str'}, nrows = nrows)    
    for column in JSON_COLUMNS:
        column_as_tf  = json_normalize(testframe[column]) # “Normalize” semi-structured JSON data into a flat table 
        column_as_tf.columns = [f"{column}.{subcolumn}" for subcolumn in column_as_tf.columns]
        testframe = testframe.drop(column, axis = 1).merge(column_as_tf, right_index = True, left_index = True) #Drop specified labels from columns. 
        # and Merge DataFrame objects by performing a database-style join operation by columns or indexes.     
    return testframe

# Remove unncessary columns from the training set and test set
# Input: train and test csv_path needs be given as input.
# Output: training and test files without_unncessary columns.
def remove_unnecessaryColumns(csv_path, test_path):
      train_df = load_dataframe(csv_path, None) # load dataframe
      train_tf = load_testframe(test_path, None) # load dataframe
      
      # Capture the columns with constant values
      const_cols = [c for c in train_df.columns if train_df[c].nunique(dropna=False)==1 ]
      const_cols_test = [c for c in train_tf.columns if train_tf[c].nunique(dropna=False)==1 ]
      
      # Drop the columns with constant values
      train_df_without_const = train_df.drop(const_cols, axis=1, inplace=False)
      train_tf_without_const = train_tf.drop(const_cols_test, axis=1, inplace=False)
      
      # Dropped columns
      drop_columns = ['trafficSource.campaignCode', 'sessionId', 'visitStartTime', 'device.browser', 'device.deviceCategory', 'device.isMobile', 'device.operatingSystem', 'geoNetwork.metro', 'geoNetwork.networkDomain', 'trafficSource.adContent', 'trafficSource.adwordsClickInfo.adNetworkType', 'trafficSource.adwordsClickInfo.gclId', 'trafficSource.adwordsClickInfo.isVideoAd', 'trafficSource.adwordsClickInfo.page', 'trafficSource.adwordsClickInfo.slot', 'trafficSource.isTrueDirect', 'trafficSource.keyword', 'trafficSource.source']
      drop_columns_test = ['sessionId', 'visitStartTime', 'device.browser', 'device.deviceCategory', 'device.isMobile', 'device.operatingSystem', 'geoNetwork.metro', 'geoNetwork.networkDomain', 'trafficSource.adContent', 'trafficSource.adwordsClickInfo.adNetworkType', 'trafficSource.adwordsClickInfo.gclId', 'trafficSource.adwordsClickInfo.isVideoAd', 'trafficSource.adwordsClickInfo.page', 'trafficSource.adwordsClickInfo.slot', 'trafficSource.isTrueDirect', 'trafficSource.keyword', 'trafficSource.source']

      # Dropping trafficSource.campaignCode column as it contains only one non NA entry
      train_df_without_const = train_df_without_const.drop(columns = drop_columns, axis=1)
      train_tf_without_const1 = train_tf_without_const.drop(columns = drop_columns_test, axis=1)
      
      # Write CSV file without constant columns
      train_df_without_const.to_csv('./data/train_without_unncessarycols.csv')
      train_tf_without_const1.to_csv('./data/trial_test.csv')
      

      
 
