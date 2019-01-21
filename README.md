# Google Analytics Customer Revenue Prediction challenge:

## Requirements:

The code is written in Python 3.6<br />
Install all required libraries by running the command<br />
`pip install requirements.txt`<br />
Install R 3.5.1 <br />
If you are using Windows add<br />
`C:\Program Files\R\R-3.5.1\bin`<br />
to your PATH in the environment variables<br />
Because of the nested column "hits", the size of the data is about 27 GB. Therefore, we used a machine with enough
 memory size to process the data. In this script we load only 15000 rows. You can change the number of rows that
 should be loaded by changing the variable `rows_amount` in line 29

## How does it work:

Run the following command:<br />
`python google_analytics_revenue_prediction.py <int>`<br />
`<int>` is the number of rows that you want to read from the csv file. Recommended is 15000 to avoid memory error<br />
If the number of the rows is not given, all the rows will be read.
## Submission to Kaggle

The running time can take little bit time!!

