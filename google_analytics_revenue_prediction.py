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

general_utilities.what_does_the_code_do_now(answer="Applying the ETL job")
print("starting the ETL job...")
print("flatting the columns that contains nested json structure")

ETL_utilities.apply_separation()