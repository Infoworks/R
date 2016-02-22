convertToSparkRDF <- function(mydf) { # convert to sparkR dataframe
  return <- createDataFrame(hivecontext ,mydf )
}

writeToHdfs <- function(ldf, host, path) {
  print(class(ldf[[1]]))
  if( class(ldf) == 'data.frame') { # TODO - CHeck
    ldf = convertToSparkRDF(ldf)
  }
  fullpath = paste("hdfs://", host, ":8020", path , sep = "")
  saveDF(ldf, fullpath,  "orc", mode = "overwrite")

  #saveDF(sparkDF, "hdfs://ip-10-37-200-15.ec2.internal:8020/tmp/test3", "orc", mode = "append")

}

writeToLocalDiskOrc <- function(mydf, path) {
  write.df(mydf, path, "orc") # or  saveDF(sparkDF, "/tmp/test3", "orc", mode = "append")
}

#source('~/demo/init.R')
#source('~/demo/spark_api.R')
#LoadSources()
#dfSave = SelectDF(skhmodata.V_MBR_ACCT_HIST)

#writeToHdfs(dfSave, "ip-10-37-200-15.ec2.internal", "/tmp/test4")

