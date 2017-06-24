import pandas as pd
import numpy as np
import statsmodels.api as smf
from sklearn import linear_model
from sklearn import cross_validation
from sklearn.metrics import mean_squared_error
from math import sqrt
from sklearn import linear_model



data = pd.read_csv("C:\Python34\data1.csv")
data = data[1:10000]

#train data
traindata = data[0:8899]
Attributes=traindata[['length','score']]
Response=traindata['helpfulness']

#test data
testdata = data[9000:9999]
Actual = testdata[['helpfulness']]
testdata=testdata[['length','score']]

#model

model = smf.OLS(Response,Attributes).fit()
results=model.summary()

#prediction
Predictions=model.predict(testdata)

#MAE

#RMSE
rms = sqrt(mean_squared_error(Actual,Predictions))
