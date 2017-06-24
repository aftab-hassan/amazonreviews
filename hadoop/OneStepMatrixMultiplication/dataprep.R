data <- readLines("/home/aftab/Downloads/bigdatanoheaders",encoding="UTF-8")

csvdf=data.frame(productId=c(),userId=c(),profileName=c(),helpfulness=c(),score=c(),time=c(),summary=c(),text=c())

row=1
col=1
for(i in 1:length(data))
{
	print(i)

	cat(paste("i==",i,"mod==",i%%9,"word==",data[i],"\n"))

	if((i%%9) == 0)
	{
		row = row+1
		col=1
	}else
	{
		cat(paste("row==",row,"col==",col,"\n"))
		csvdf[row,col] = data[i]
		col = col+1
	}
	cat(paste("\n"))

	if((i%%(100000*9))==0)
	{
		filename = paste0('/home/aftab/Downloads/csvfile',(i/(100000*9)),".csv")
		write.csv(csvdf,filename,row.names=FALSE)
		csvdf = NULL
		csvdf=data.frame(productId=c(),userId=c(),profileName=c(),helpfulness=c(),score=c(),time=c(),summary=c(),text=c())
		row=1
		col=1
	}
}
write.csv(csvdf,'/home/aftab/Downloads/dataset.csv')