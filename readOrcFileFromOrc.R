#Sys.setenv(SPARK_HOME="/usr/hdp/current/spark-client") # working
#Sys.setenv(SPARK_HOME="/home/rstudio/spark/spark-1.5.1-bin-hadoop2.6")
source("~/iwr/R/init.R")
source("~/iwr/R/mongo_api.R")
source("~/iwr/R/myhelper.R")
source("~/iwr/R/spark_api.R")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)

sc <- sparkR.init("local")
sqlContext = sparkRSQL.init()
hivecontext <- sparkRHive.init(sc)
df <- loadDF( hivecontext, "/data/ingest/sparktest1/56878a17e4b09bf0793b93ad/merged/orc/*/*", "orc")
print(df$ziw_row_id)




