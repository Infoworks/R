#Clean up env
rm(list = ls(), envir = .GlobalEnv) 


print("Starting")

#Source infoworks lib
source('infoworks/infoworks.R')

#Source cvs mo code
source('cvs/cvs_mo_phase2_new.R')

Sys.setenv(http_proxy="http://wsgproxy.cvs.com:8080")
Sys.setenv(https_proxy="http://wsgproxy.cvs.com:8080")

#Prepare computation plan
cmpgnMbrOpptyTable <- prepareMoCampaignTable()

print(head(cmpgnMbrOpptyTable))

#Save output to hdfs
saveToHdfs(cmpgnMbrOpptyTable, "IRI1HDPMTL2V.ilab.cvscaremark.com", "/iw/cvs/mo/campaign/")
