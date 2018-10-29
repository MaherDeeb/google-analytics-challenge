
## libraries 
require(caret)
require(tidyverse)
require(e1071)


## get the data
#getwd() #  in case it is nut the desired path do the next line
#setwd("C:/kaggle/google-analytics-challenge")
test.id<-as.data.frame(read_csv("./data/test_id_clean.csv"))
target<-as.data.frame(read_csv("./data/target_clean.csv"))
test<-as.data.frame(read_csv("./data/test_clean.csv"))
train<-as.data.frame(read_csv("./data/train_clean.csv"))


# some nas still exist 
# will be replaced by 0 ... it seems ok for this stage 
train[is.na(train)]<-0
test[is.na(test)]<-0

print("removed some NAs in the training and testing files")
# feature isMobile is a character ... need to change it into factorial 
train$isMobile<-as.factor(train$isMobile)
test$isMobile<-as.factor(test$isMobile)

print("feature isMobile is a character ... changed into factorial")

# the target still contains some Nas 
target[is.na(target)]<-0

# the following can be done in later stages
# apply classification to predict which session ID is associated with a Revenue >0
#train$p_revenue<-"NO"
#train$p_revenue[target$`0`>0]<-"Yes"
#train$p_revenue<-as.factor(train$p_revenue)
#summary(train$p_revenue)
#control <- trainControl(method="none")

print("might take some time ... K-fold cross valedation , K=10")

#build the model 
model <- train(x=train,y=as.numeric(c(0,target$`0`)), method="ctree",trControl=trainControl(method='cv',number=10))




#model <- train(x=train,y=as.numeric(c(0,target$`0`)), method="ctree",trControl=trainControl(method='none'))

# predict
revenue<-predict(model,newdata = test)

# fix the ids  
id<-c(names(test.id),as.character(test.id$`6167871330617112363`))

# create the submission dataframe 
ready_to_submit<-cbind.data.frame(id=id,predictions=revenue)

print("submission file created")
summary(ready_to_submit)

# write the csv file 
write.csv(x =ready_to_submit,file = "./data/ready_to_submit.csv",row.names=FALSE)
