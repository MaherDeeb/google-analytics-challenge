## Libraries 

library(tidyverse)
library(jsonlite)


## get data
train <- read_csv("../input/train.csv")
test <- read_csv("../input/test.csv")
subm <- read_csv("../input/sample_submission.csv")

head(train)

t_device<-as.data.frame(t(sapply(train$device,FUN=function(x){unlist(fromJSON(x,flatten = F))},USE.NAMES = F)))
t_geoNetwork<-as.data.frame(t(sapply(train$geoNetwork,FUN=function(x){unlist(fromJSON(x,flatten = F))},USE.NAMES = F)))
t_totals<-as.data.frame(t(sapply(train$totals,FUN=function(x){unlist(fromJSON(x,flatten = F))},USE.NAMES = F)))
t_trafficSource<-as.data.frame(t(sapply(train$trafficSource,FUN=function(x){unlist(fromJSON(x,flatten = F))},USE.NAMES = F)))

ls()

train2<-cbind(select(train,-device, -geoNetwork, -trafficSource, -totals),t_device,t_geoNetwork,t_totals,t_trafficSource)


t_device<-as.data.frame(t(sapply(test$device,FUN=function(x){unlist(fromJSON(x,flatten = F))},USE.NAMES = F)))
t_geoNetwork<-as.data.frame(t(sapply(test$geoNetwork,FUN=function(x){unlist(fromJSON(x,flatten = F))},USE.NAMES = F)))
t_totals<-as.data.frame(t(sapply(test$totals,FUN=function(x){unlist(fromJSON(x,flatten = F))},USE.NAMES = F)))
t_trafficSource<-as.data.frame(t(sapply(test$trafficSource,FUN=function(x){unlist(fromJSON(x,flatten = F))},USE.NAMES = F)))

test2<-cbind(select(test,-device, -geoNetwork, -trafficSource, -totals),t_device,t_geoNetwork,t_totals,t_trafficSource)






ZZ<-unlist(fromJSON(train$device[1],flatten = F))

class(ZZ[1])
dim(ZZ[1])


dim(t_device)
class(t_device[1,1])
t_device[1:10,]

flatten_json <- function(X){ 
  Y<-str_c(X, collapse = ",")  
  str_c("[", Y, "]")  
  fromJSON(str_c("[", Y, "]"),flatten = T)
  
  parse <- . %>% 
    bind_cols(flatten_json(.$device)) %>%
    bind_cols(flatten_json(.$geoNetwork)) %>% 
    bind_cols(flatten_json(.$trafficSource)) %>% 
    bind_cols(flatten_json(.$totals)) %>% 
    select(-device, -geoNetwork, -trafficSource, -totals)
  
  
  flatten_json <- . %>% 
    str_c(., collapse = ",") %>% 
    str_c("[", ., "]") %>% 
    fromJSON(flatten = T)
  
  parse <- . %>% 
    bind_cols(flatten_json(.$device)) %>%
    bind_cols(flatten_json(.$geoNetwork)) %>% 
    bind_cols(flatten_json(.$trafficSource)) %>% 
    bind_cols(flatten_json(.$totals)) %>% 
    select(-device, -geoNetwork, -trafficSource, -totals)