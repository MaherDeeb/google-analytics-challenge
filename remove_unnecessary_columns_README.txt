Total Columns:

'date' ----- Required Column
'fullVisitorId' -----Required Column 
'sessionId' ----Removed, seems like irrelevant for training
'visitId' ---- With the presence of Visitor Id, this field looks irrelevant
'visitNumber' - Dont'know if this column needs to be removed
'visitStartTime' -- Removed, this field looks irrelevant 
'device.browser' - Removed, this field looks irrelevant
'device.deviceCategory' - Removed, this field looks irrelevant
'device.isMobile' - Removed, this field looks irrelevant
'device.operatingSystem' - Removed, this field looks irrelevant
'geoNetwork.city' - Required Column
'geoNetwork.continent' - Required Column
'geoNetwork.country' - Required Column
'geoNetwork.metro' - Removed, this field looks irrelevant
'geoNetwork.networkDomain' -Removed, this field looks irrelevant
'geoNetwork.region' - Dont'know if this column needs to be removed
'geoNetwork.subContinent' - Dont'know if this column needs to be removed?, Continent already exist. Do we need subcontinent? 
'totals.bounces' - Dont'know if this column needs to be removed. We can remove this column because 453023 rows are NILL
'totals.hits' - Dont'know if this column needs to be removed.
'totals.newVisits' - Dont'know if this column needs to be removed, 200593 NULL values
'totals.pageviews'  - Dont'know if this column needs to be removed, 100 NULL values
'totals.transactionRevenue', Required Column
'trafficSource.adContent' - Removed, 892707 NULL entries
'trafficSource.adwordsClickInfo.adNetworkType' - Removed, 882193 NULL entries, most of the entries are 'Google Search'
'trafficSource.adwordsClickInfo.gclId' - Removed, 882092 NULL entries and remaining values are encrypted
'trafficSource.adwordsClickInfo.isVideoAd' -Removed, 882193 NULL entries and all other entries are with boolean value 'False', information seems to be irrelevant
'trafficSource.adwordsClickInfo.page' - Removed, 882193 NULL entries and information seems irrelevant
'trafficSource.adwordsClickInfo.slot' - Removed, 882193 NULL entries and information seems irrelevant  
'trafficSource.campaign' - Dont'know if this column needs to be removed
'trafficSource.isTrueDirect' - Removed, 629648 NULL entries
'trafficSource.keyword' - Removed, 502929 NULL entries and looks non relevant
'trafficSource.medium', - Dont'know if this column needs to be removed, entries are array(['organic', 'referral', 'cpc', 'affiliate', '(none)', 'cpm', '(not set)'], dtype=object)- looks irrelevant
'trafficSource.referralPath' - Dont'know if this column needs to be removed, 572712 NULL entries 
'trafficSource.source' - Removed, seems like irrelevant

#------------------------------------------------------------------------------------------------	  
Number of null entries in columns:

totals.bounces                                  453023
totals.newVisits                                200593
totals.pageviews                                   100
totals.transactionRevenue                       892138
trafficSource.adContent                         892707
trafficSource.adwordsClickInfo.adNetworkType    882193
trafficSource.adwordsClickInfo.gclId            882092
trafficSource.adwordsClickInfo.isVideoAd        882193
trafficSource.adwordsClickInfo.page             882193
trafficSource.adwordsClickInfo.slot             882193
trafficSource.isTrueDirect                      629648
trafficSource.keyword                           502929
trafficSource.referralPath                      572712

---------------------------------------------------------------------------------------------------





	  
      
	  
	  