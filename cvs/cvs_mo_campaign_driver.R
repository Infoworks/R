rm(list = ls(), envir = .GlobalEnv) 



print("Starting")

source('~/demo/init.R')
print("initialization done")
source('~/demo/spark_api.R')
source('~/demo/mongo_api.R')
source('~/demo/myhelper.R')
source('~/demo/iw_tools.R')
source('~/demo/iw_hdfsUtil.R')

source('cvs_mo_phase2.R')

cmpgnMbrOpptyTable <- prepareMoCampaignTable()

print(head(cmpgnMbrOpptyTable))
saveToHdfs(cmpgnMbrOpptyTable, "ip-10-37-200-15.ec2.internal", "/iw/cvs/mo/campaign/")

