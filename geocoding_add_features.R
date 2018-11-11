

# install lybraries  ------------------------------------------------------


list.of.packages <- c(#"ggplot2",
                      #"caret",
                      "tidyverse",
                      #"e1071",
                      #"jsonlite",
                      "lubridate",
                      "ggmap",
                      "googleway",
                      "lutz",
                      "dplyr",
                      #"RANN",
                      "stringr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages,dependencies = T,repos = "https://cloud.r-project.org")

sapply(X = list.of.packages,FUN=require,character.only=T)

rm(new.packages,list.of.packages)

cat("Libraries installed and loaded /n")
# get the data  -----------------------------------------------------------
cat("getting data /n")

#getwd()
#setwd("C:/kaggle/V2/google-analytics-challenge")
test<-as.data.frame(read_csv("./data/test_feature_engineering.csv"))
train<-as.data.frame(read_csv("./data/train_feature_engineering.csv"))


# geocoding ... get location ----------------------------------------------

cat("geocoding /n")

# replace unused location names with NA 
is_na_val <- function(x) x %in% c("not available in demo dataset", "(not provided)",
                                  "(not set)", "<NA>", "unknown.unknown",  "(none)")
tr_i<-1:nrow(train)
names(train)

needed_cols<-c("fullVisitorId","visitStartTime",
                 "geoNetwork.country",
                 "geoNetwork.region",
               "totals.transactions",
                 "geoNetwork.city")

tr_te<-rbind(train[,needed_cols],test[,needed_cols])

tr_te<-tr_te%>%
  mutate_all(funs(ifelse(is_na_val(.), NA, .))) 


#countries with no revenue
by_Land<-tr_te[,c("geoNetwork.country","totals.transactions")]%>%
  group_by(geoNetwork.country)%>%
  summarize(total_country=sum(totals.transactions,na.rm = T),
            av_country=sum(totals.transactions,na.rm = T),
            pr_country=length(totals.transactions[totals.transactions>0&!is.na(totals.transactions)])/length(totals.transactions))


# I will get the time zones only for countries where purchaces were made in the training dataset 
# this is to reduce time and stay in the limit of the possible requests from dsk "Data Science Toolki"

# the countries with some revenue: 
countries_list<-by_Land$geoNetwork.country[by_Land$pr_country>0]

many_tz<-c("Antarctica",           "Australia",           "Brazil",           "Canada",
           "Chile",           "Democratic Republic of the Congo",           "Denmark",           "Ecuador",
           "Federated States of Micronesia",           "France",           "Indonesia",           "Kazakhstan",
           "Kingdom of the Netherlands",           "Kiribati",           "Mexico",           "Mongolia",
           "New Zealand",           "Papua New Guinea",           "Portugal",           "Russia",
           "South Africa",           "Spain",           "Ukraine",           "United Kingdom",
           "United States")

many_tz<-tolower(many_tz)
# list of woulds countries with more than one tz 
# for these countries we add the region fo a more detailed location 

address<-data.frame(country=tr_te$geoNetwork.country,region=tr_te$geoNetwork.region,city=tr_te$geoNetwork.city)

# remove dublicatedrows 
address<-unique.data.frame(address)

# no need to bother with tz if no revenew is recorded for a specific country 

address<-address[address$country%in%countries_list,]
address$country<-as.character(address$country)
address$region<-as.character(address$region)
address$city<-as.character(address$city)
#address$region[is.na(address$region)]<-""
#address$city[is.na(address$city)]<-""

address$address<-address$country

address$address[!is.na(address$region)&tolower(address$country)%in%many_tz]<-
  paste(address$region[!is.na(address$region)&tolower(address$country)%in%many_tz],
        address$address[!is.na(address$region)&tolower(address$country)%in%many_tz],sep=", ")
address$address[!is.na(address$city)&tolower(address$country)%in%many_tz]<-
  paste(address$city[!is.na(address$city)&tolower(address$country)%in%many_tz],
        address$address[!is.na(address$city)&tolower(address$country)%in%many_tz],sep=", ")

Locations<-geocode(address$address, output = c("latlon"), source = c("dsk"))
address<-cbind.data.frame(address,Locations)

## add tz 

cat("adding local time /n")


address$tz<-tz_lookup_coords(lat =address$lat ,lon = address$lon, method = "fast")

tr_te<-left_join(tr_te,address,
                 by=c("geoNetwork.country"="country","geoNetwork.region"="region","geoNetwork.city"="city"))


#tr_te$visitStartTime<-as.numeric(tr_te$visitStartTime)
tr_te$GMT_time<-as.POSIXct(tr_te$visitStartTime,origin = as.POSIXct("1970-01-01"),tz = "UCT")
tr_te$Local_time<-as.POSIXlt(tr_te$visitStartTime,origin = as.POSIXct("1970-01-01"),tz ="UCT")



i<-levels(as.factor(tr_te$tz))[1]
for (i in levels(as.factor(tr_te$tz))) {
  tr_te$Local_time[tr_te$tz%in%i]<-as.POSIXlt(tr_te$visitStartTime[tr_te$tz%in%i],
                                                          origin = as.POSIXct("1970-01-01"),tz =i)
  tr_te$GMT_time[tr_te$tz%in%i]<-as.POSIXct(tr_te$visitStartTime[tr_te$tz%in%i],
                                                        origin = as.POSIXct("1970-01-01"),tz ="UTC")
  print(i)
}

# out of local time:

tr_te$month<-month(tr_te$Local_time)
tr_te$dow<-wday(label = TRUE,tr_te$Local_time)
tr_te$dom<-mday(tr_te$Local_time)
tr_te$week<-strftime(tr_te$Local_time, format = "%V")
tr_te$Hour<-hour(tr_te$Local_time)
tr_te$AM_PM<-strftime(tr_te$Local_time, format = "%p")



# weekdays ----------------------------------------------------------------
cat("extracting weekdays /n")



# usually the weekend is saturday and sunday
tr_te$weekend<-0
# list of countries were only sunday is weekend: 
V1<-c("Equatorial Guinea","Hong Kong", "Mexico", "Philippines" , "Uganda")
tr_te$weekend[(as.character(tr_te$geoNetwork.country)%in%V1)&(as.character(tr_te$dow)%in%c("Sun"))]<-1

# list of countries were only friday is weekend: 
V2<-c("Iran","Djibouti","Somalia")
tr_te$weekend[(as.character(tr_te$geoNetwork.country)%in%V2)&(as.character(tr_te$dow)%in%c("Fri"))]<-1

# list of countries were only saturday  is weekend: 
V3<-c("Nepal")
tr_te$weekend[(as.character(tr_te$geoNetwork.country)%in%V3)&(as.character(tr_te$dow)%in%c("Sat"))]<-1


# list of countries were friday and saturday  is weekend: 
V4<-c("Bangladesh","Afghanistan","Algeria","Bahrain",      "Egypt",      "Iraq",      "Israel",      
      "Jordan", "Kuwait","Libya","Maldives", "Malaysia", "Oman", "Palestine","Qatar",
      "Saudi Arabia", "Sudan",      "Syria",      "United Arab Emirates",      "Yemen")
tr_te$weekend[(as.character(tr_te$geoNetwork.country)%in%V4)&(as.character(tr_te$dow)%in%c("Fri","Sat"))]<-1
tr_te$weekend[!(as.character(tr_te$geoNetwork.country)%in%c(V1,V2,V3,V4))&(as.character(tr_te$dow)%in%c("Sun","Sat"))]<-1





#### get the time spent between previous session and current session 

cat("extracting lag and lead time per user and visit /n")
cat("This will take some time depending how arge your dataset is  /n")




as.integer(as_datetime(tr_te$GMT_time[1:10]))

df1<-tr_te[,c("fullVisitorId","GMT_time")]%>%
  group_by(fullVisitorId) %>%
  mutate(lag_1=lag((GMT_time),1,order_by = GMT_time),
         lag_2=lag((GMT_time),2,order_by = GMT_time),
         lead_1=lead((GMT_time),1,order_by = GMT_time),
         lead_2=lead((GMT_time),2,order_by = GMT_time))

df1$lagt1<-as.numeric(df1$GMT_time-df1$lag_1)/3600
df1$lagt2<-as.numeric(df1$GMT_time-df1$lag_2)/3600
df1$leadt1<-as.numeric(df1$GMT_time-df1$lead_1)/3600
df1$leadt2<-as.numeric(df1$GMT_time-df1$lead_2)/3600

tr_te$lagt1<-df1$lagt1
tr_te$lagt2<-df1$lagt2
tr_te$leadt1<-df1$leadt1
tr_te$leadt2<-df1$leadt2


# spltit again into training and testing 

train<-cbind(train,tr_te[tr_i,11:23])
train<-cbind(train,tr_te[-tr_i,11:23])

cat("vreating csv output /n")


getwd()

write_csv(x = train,path = "./data/train_Localfeatures.csv")
write_csv(x = test,path = "./data/test_localfeatures.csv")
#
cat("Done =)  /n")


