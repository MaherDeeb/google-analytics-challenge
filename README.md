# Google Analytics Customer Revenue Prediction challenge:

## Submission to Kaggle
the scirpt inside submission.py prepares the finial submission csv file that you can load directly to Kaggle by clicking 'Submit Prediction' in the competition websiet

### Requirements:
python 3.6.2  <br />
pandas framework  <br />
datetime framework  <br />
time framework  <br />
sys framework  <br />

### How does it work:

1. you need to save the 'non-aggregated prediction' csv file somewhere on your local. The predictions should contain 804684 rows <br />
2. If you saved the 'non-aggregated prediction' csv under the path './data' and under the name predict.csv, you can run the script in the terminal as follows: <br />
`python submission.py` <br />
3. If you saved the 'non-aggregated prediction' csv somewhere else than the path './data', for example './data/pred/',  but still under the name predict.csv, you can run the script in the terminal as follows: <br />
`python submission.py './data/pred/' <br />
4. If you saved the 'non-aggregated prediction' csv somewhere else than the path './data', for example './data/pred/',  and under another name than predict.csv, for example 'mypred.csv', you can run the script in the terminal as follows:<br />
`python submission.py './data/pred/' 'mypred.csv'`