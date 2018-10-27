import json 
import pandas as pd
from pandas.io.json import json_normalize

# Convert json fields in the train.csv file to flattend csv format
# This function is taken from kernel by SRK
def load_dataframe(csv_path = './data/train.csv', nrows = None ):
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

   
train_df = load_dataframe() # load the datadrame

# Capture the columns with constant values
const_cols = [c for c in train_df.columns if train_df[c].nunique(dropna=False)==1 ]

# Drop the columns with constant values
train_df = train_df.drop(const_cols, axis=1, inplace=False)
  
  

