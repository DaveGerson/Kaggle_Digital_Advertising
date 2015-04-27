#STATS:  45,840,617 million records

library(doParallel)
getDoParWorkers() # = 1
library("doRSR")
registerDoRSR()
registerDoSEQ()
registerDoParallel(cores=2)


#digital_ad_csv<-"E:/Kaggle/Display Advertising Challenge/train/train.csv"
#digital_ad_dat <- RxTextData(digital_ad_csv, delimiter = ",")
#digital_ad_xdf<- rxImport(digital_ad_dat,outFile = "E:/Kaggle/Display Advertising Challenge/train/train.xdf",overwrite=TRUE)

#iterations = 1018
#foreach(k=0:iterations) %dopar% 
#	{
#	digital_ad_csv<-paste(paste("E:/Kaggle/Display Advertising Challenge/train/train_output",k,sep=""),".txt",sep="")
#	digital_ad_dat <- RxTextData(digital_ad_csv, delimiter = ",")
#	digital_ad_xdf<-rxImport(digital_ad_dat,outFile = paste(paste("E:/Kaggle/Display Advertising Challenge/train/train_xdf",k,sep=""),".xdf",sep=""),overwrite=TRUE)
#	k <- k+1
#	}

library(doParallel)
getDoParWorkers() # = 1
library("doRSR")
registerDoRSR()
registerDoSEQ()
iterations = 1018
digital_ad_head<-c("Id","Label","I1","I2","I3","I4","I5","I6","I7","I8","I9","I10","I11","I12","I13","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13","C14","C15","C16","C17","C18","C19","C20","C21","C22","C23","C24","C25","C26")
#kfolder
iterationLev <- function(x,y) as.integer(x*y)
registerDoRSR()
registerDoSEQ()
rand_file<- runif(40)
iterationLev <- function(x) as.integer(x*iterations)
folderlist<-sapply(rand_file,iterationLev)

train_frame<-data.frame()

foreach(i=1:length(folderlist)) %do% 
	{
	train_file<-rxImport(paste(paste("E:/Kaggle/Display Advertising Challenge/train/train_xdf",folderlist[i],sep=""),".xdf",sep=""))
	train_frame<-rbind(train_frame,train_file)
	out<-i
	}
train_frame<-rxDataStepXdf(train_frame,maxRowsByCols = -1)	
names(train_frame)<-digital_ad_head
train_frame[,2]<-as.factor(train_frame[,2])
foreach(i=16:41) %do% 
	{
	train_frame[,i]<-as.factor(train_frame[,i])
	train_frame[,i]<-addNA(train_frame[,i])
	out<-i
	}
	
train_frame_modeling<-rxDataStepXdf(train_frame,varsToDrop = c('C3','C4','C10','C15','C16','C21','C24','C26','C7','C11','C12','C13','C18','C19'),maxRowsByCols = -1)
	
#decisionForest_output<-rxDForest(Label ~ I1 + I2 + I3 + I4 + I5 + I6 + I7 + I8 + I9 + I10 + I11 + I12 + I13 + C1 + C2 +C5 + C6 + C7 + C8 + C9 + C11 + C12 + C13 + C14 + C17 + C18 + C19 + C20 + C22 + C23 + C25,train_frame_modeling,pruneCp="auto")	
#glm_output<-rxLogit(Label ~ I1 + I2 + I3 + I4 + I5 + I6 + I7 + I8 + I9 + I10 + I11 + I12 + I13 + C1 + C2 +C5 + C6 + C7 + C8 + C9 + C11 + C12 + C13 + C14 + C17 + C18 + C19 + C20 + C22 + C23 + C25,family = binomial(),cube = TRUE,train_frame_modeling)

isNullNumber = Vectorize(is.na)
foreach(i=3:15) %do% 

	{
	#train_frame_modeling[isNullNumber(train_frame_modeling[,i]),"new"]<-0
	train_frame_modeling[,i][isNullNumber(train_frame_modeling[,i])]<-mean((train_frame_modeling[,i]),na.rm=T) 
	#train_frame_modeling[,"new"]<-as.factor(train_frame_modeling[,"new"])
	#names(train_frame_modeling)[i+13]<-paste(paste("I",i-2,sep=""),"flag",sep="_")
	out<-i
	}
	
glm_output<-rxGlm(Label ~ C1 + C2 +C5 + C6 + C8 + C9 + C14 + C17 + C20 + C22 + C23 + C25 +
# I1_flag + I2_flag + I3_flag + I4_flag + I5_flag + I6_flag + I7_flag + I8_flag + I9_flag + I10_flag + I11_flag + I12_flag + I13_flag +
 I1 + I2 + I3 + I4 + I5 + I6 + I7 + I8 + I9 + I10 + I11 + I12 + I13 ,data = train_frame_modeling,maxIterations = 50 , objectiveFunctionTolerance = 0, family=binomial())


save(glm_output,file ="E:/Kaggle/Display Advertising Challenge/Kaggle_Digital_Advertising/incumbent_model.rda")
load("E:/Kaggle/Display Advertising Challenge/Kaggle_Digital_Advertising/incumbent_model.rda")

#iterations=120
#will be split into 121 chunks of 49,935 observations
#foreach(k=0:iterations) %dopar% 
#	{
#	test_digital_ad_csv<-paste(paste("E:/Kaggle/Display Advertising Challenge/test/test_output",k,sep=""),".csv",sep="")
#	test_digital_ad_dat <- RxTextData(test_digital_ad_csv, delimiter = ",", useFastRead = TRUE)
#	test_digital_ad_xdf<- rxImport(test_digital_ad_dat,outFile = paste(paste("E:/Kaggle/Display Advertising Challenge/test/test_xdf",k,sep=""),".xdf",sep=""),overwrite=TRUE)
#	k <- k+1
#	}

model_output<-load("E:/Kaggle/Display Advertising Challenge/Kaggle_Digital_Advertising/incumbent_model.rda")
model_output1<-load("E:/Kaggle/Display Advertising Challenge/Kaggle_Digital_Advertising/incumbent_model.rda")
rm(model_output1)
iterations=120
k=0


foreach(k=0:iterations) %do% {
	test_digital_ad_xdf<- rxImport(paste(paste("E:/Kaggle/Display Advertising Challenge/test/test_xdf",k,sep=""),".xdf",sep=""),overwrite=TRUE)
	names(test_digital_ad_xdf)<-c("Id","I1","I2","I3","I4","I5","I6","I7","I8","I9","I10","I11","I12","I13","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13","C14","C15","C16","C17","C18","C19","C20","C21","C22","C23","C24","C25","C26")
	test_digital_ad_xdf<-rxDataStepXdf(test_digital_ad_xdf,varsToDrop = c('C3','C4','C10','C15','C16','C21','C24','C26'),maxRowsByCols = -1)
	Digital_Ad_pred<-rxPredict(model_output,data = test_digital_ad_xdf)
	pred_out<-data.frame(test_digital_ad_xdf$Id,Digital_Ad_pred[,2])
	names(pred_out)<-c("Id","Predicted")
	ad_predictions<-paste(paste("E:/Kaggle/Display Advertising Challenge/predictions/predictions_output",k,sep=""),".csv",sep="")
	rxDataStep(pred_out,outFile=ad_predictions,overwrite=TRUE)
	out<-k
	}

#########EVALUATION####################
llfun <- function(actual, prediction) {
    epsilon <- .000000000000001
    yhat <- pmin(pmax(prediction, rep(epsilon)), 1-rep(epsilon))
    logloss <- -mean(actual*log(yhat)
                     + (1-actual)*log(1 - yhat))
    return(logloss)
}