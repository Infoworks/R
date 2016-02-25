writeToHdfs <- function(ldf, host, path) {
  print(class(ldf[[1]]))
  if( class(ldf) == 'data.frame') { # TODO - CHeck
    ldf = convertToSparkRDF(ldf)
  }
  fullpath = paste("hdfs://", host, ":8020", path , sep = "")
  saveDF(ldf, fullpath,  "orc", mode = "overwrite")

  #saveDF(sparkDF, "hdfs://ip-10-37-200-15.ec2.internal:8020/tmp/test3", "orc", mode = "append")

}