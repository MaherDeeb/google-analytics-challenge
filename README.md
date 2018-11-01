# Google Analytics Customer Revenue Prediction challenge:

## Submission to Kaggle
the scirpt inside submission.py prepares the finial submission csv file that you can load directly to Kaggle by clicking 'Submit Prediction' in the competition websiet

### Requirements:
python 3.6.2  <br />
pandas framework  <br />
datetime framework  <br />
time framework  <br />
sys framework  <br />
itertools <br />
lightgbm framework (https://pypi.org/project/lightgbm/) <br />

### How does it work:

Run the pipeline in following order <br />
`python ETL.py`<br />
`python preprossesing_drop_columns.py`<br />
`python preprossesing_feature_engineering.py`<br />
`python ML.py`<br />
`python submission.py`<br />

The running time can take little bit time!!

