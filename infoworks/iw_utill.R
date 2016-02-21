convertToSparkRDF <- function(mydf) { # convert to sparkR dataframe
  return <- createDataFrame(hivecontext ,mydf )
}

writeToHdfs <- function(ldf, host, path) {
  sparkDF = convertToSparkRDF(ldf)  
  fullpath = paste("hdfs://", host, ":8020", path , sep = "")
  saveDF(sparkDF, fullpath,  "orc", mode = "append")

  #saveDF(sparkDF, "hdfs://ip-10-37-200-15.ec2.internal:8020/tmp/test3", "orc", mode = "append")
  
}

writeToLocalDiskOrc <- function(mydf, path) { 
  write.df(mydf, path, "orc") # or  saveDF(sparkDF, "/tmp/test3", "orc", mode = "append")
}


# USAGE : writeToHdfs(dfsave, "ip-10-37-200-15.ec2.internal", "/tmp/test4")
