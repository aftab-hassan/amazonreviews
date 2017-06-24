library(Metrics)

# read data from csv
mydata<-read.csv('bigdata551.csv')

rmsearray = NULL
maearray = NULL
# loop counter set to 10
k = 10;
# current time
ptm <- proc.time()
# This loop is for 10 fold cross validation
for(i in 1:k)
{
  # Divide the data into 80:20 ratio
  testIndex = sample(1:nrow(mydata),floor(nrow(mydata)/5))
  trainIndex = -testIndex
  # Training data is 80% of the original data
  traindf = mydata[trainIndex,]
  # Testing data is 20% of the original data
  testdf = mydata[testIndex,]
  model = lm(helpfulness ~ score + length , traindf)
  predictions = predict(model,testdf)
  actual=testdf$helpfulness
  # Rmse using the actual and predicted values
  rmsevalue = rmse(actual,predictions)
  #Mae using the actual and predicted values
  maevalue=mae(actual,predictions)
  cat(paste("RMSE at k==",i,"==",rmsevalue,"\n",sep=""))
  cat(paste("MAE at k==",i,"==",maevalue,"\n",sep=""))
  #Copy rmsevalue of the current iteration into rmsearray
  rmsearray = c(rmsearray,rmsevalue)
  #Copy maevalue of the current iteration into maearray
  maearray = c(maearray,maevalue)
} 
# finished time
time<-proc.time() - ptm
cat(paste("Mean RMSE ==",mean(rmsearray)))
cat(paste("Mean MAE ==",mean(maearray)))

cat(paste("Elapased time in secs==",time["elapsed"]))