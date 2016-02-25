convertToSparkRDF <- function(mydf) { # convert to sparkR dataframe
  return <- createDataFrame(hivecontext ,mydf )
}

writeToLocalDiskOrc <- function(mydf, path) { 
  write.df(mydf, path, "orc") # or  saveDF(sparkDF, "/tmp/test3", "orc", mode = "append")
}