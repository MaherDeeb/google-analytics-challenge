import pandas as pd
 
# Remove unncessary columns from the training set and test set
# Input: flattenned train and test csv file path needs be given as input to the below function.
# Output: training and test files without_unncessary columns will be saved as
#         ./data/trainset_without_unncessarycols.csv and
#         ./data/testset_without_unncessarycols.csv

def remove_unnecessaryColumns(inputFilePath_train_csv, inputFilePath_test_csv):   
      train_df = pd.read_csv(inputFilePath_train_csv) 
      test_df = pd.read_csv(inputFilePath_test_csv) 
      
      # Capture the columns with constant values
      const_cols_train = [c for c in train_df.columns
                          if train_df[c].nunique(dropna=False)==1 ]
      const_cols_test = [c for c in test_df.columns 
                         if test_df[c].nunique(dropna=False)==1 ]
      
      # Drop the columns with constant values
      train_df_without_const = train_df.drop(const_cols_train,
                                             axis=1, inplace=False)
      test_df_without_const = test_df.drop(const_cols_test,
                                           axis=1, inplace=False)
      
      for column in list(train_df_without_const.columns):
          if column not in list(test_df_without_const.columns) and \
          column != 'transactionRevenue':
              train_df_without_const =\
              train_df_without_const.drop(column, axis=1, inplace=False)
      # Write CSV file without constant columns
      train_df_without_const.\
      to_csv('./data/trainset_without_unncessarycols.csv', index = False)
      test_df_without_const.\
      to_csv('./data/testset_without_unncessarycols.csv', index = False)
 
inputFilePath_train_csv = './data/train_expanded.csv'
inputFilePath_test_csv = './data/test_expanded.csv'

remove_unnecessaryColumns(inputFilePath_train_csv, inputFilePath_test_csv)