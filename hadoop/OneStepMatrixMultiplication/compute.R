library('Metrics')

coefficients = read.csv('/home/aftab/Desktop/matrixinputgen/stepfouroutput.txt',header=FALSE)

data = read.csv('/home/aftab/Downloads/bigdata551.csv')
traindata = data[1:(0.9*nrow(data):nrow(data)),]
testdata = data[(0.9*nrow(data)):nrow(data),]

predictions = c()

for(i in 1:nrow(testdata))
{
	predictions = c(predictions,(coefficients[1,3] + coefficients[2,3]*testdata[i,"score"] + coefficients[3,3]*testdata[i,"length"]))
}
actual = testdata$helpfulness

cat(paste("MAE==",mae(actual,predictions)))
cat(paste("RMSE==",rmse(actual,predictions)))