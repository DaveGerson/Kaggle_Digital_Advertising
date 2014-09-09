#STATS:  45,840,617 million records

library(doParallel)
getDoParWorkers() # = 1
library("doRSR")
registerDoParallel(cores=4)

#registerDoSEQ()

#digital_ad_csv<-"E:/Kaggle/Display Advertising Challenge/train/train.csv"
#digital_ad_dat <- RxTextData(digital_ad_csv, delimiter = ",")
#digital_ad_xdf<- %dopar%  rxImport(digital_ad_dat,outFile = "E:/Kaggle/Display Advertising Challenge/train/train.xdf",overwrite=TRUE)

iterations = 1018
foreach(k=0:iterations) %dopar% {
digital_ad_csv<-paste(paste("E:/Kaggle/Display Advertising Challenge/train/train_output",k,sep=""),".txt",sep="")
digital_ad_dat <- RxTextData(digital_ad_csv, delimiter = ",")
digital_ad_xdf<-rxImport(digital_ad_dat,outFile = paste(paste("E:/Kaggle/Display Advertising Challenge/train/train_xdf",k,sep=""),".xdf",sep=""),overwrite=TRUE)
k <- k+1
}


iterations = 1018
digital_ad_head<-c("Id","Label","I1","I2","I3","I4","I5","I6","I7","I8","I9","I10","I11","I12","I13","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13","C14","C15","C16","C17","C18","C19","C20","C21","C22","C23","C24","C25","C26")
#kfolder
iterationLev <- function(x,y) as.integer(x*y)

rand_file<- runif(20)
iterationLev <- function(x) as.integer(x*iterations)
folderlist<-sapply(rand_file,iterationLev)

