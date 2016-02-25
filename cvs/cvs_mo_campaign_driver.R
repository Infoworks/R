#Clean up env
rm(list = ls(), envir = .GlobalEnv) 


print("Starting")

#Source infoworks lib
source('infoworks/infoworks.R')

#Source cvs mo code
source('cvs/cvs_mo_phase2.R')

#Prepare computation plan
cmpgnMbrOpptyTable <- prepareMoCampaignTable()

print(head(cmpgnMbrOpptyTable))

#Save output to hdfs
saveToHdfs(cmpgnMbrOpptyTable, "ip-10-37-200-15.ec2.internal", "/iw/cvs/mo/campaign/")
