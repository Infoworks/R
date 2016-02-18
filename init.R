InitMongo <- function(db="infoworks-new", host="localhost", port='27017', username='infoworks', password='IN11**rk')
{
  # get the values from config file available on iw rest api

  # mongo initialize
  library(RJSONIO)

  library(RMongo)
  iwmongo <<- mongoDbConnect(dbName=db, host=host, port=port)
  dbAuthenticate(iwmongo, username, password)

  library(rmongodb)
  iwmongodb <<- mongo.create(
  host=host, name="", username=username, password=password, db=db)

  # load source names (source tables will have to be added explicity later)
  sources = dbGetQueryForKeys(iwmongo, "sources", "", "{'name':1,'hdfs_path':1}", 0, 1000)
  # TO DEBUG for 1 source sources = dbGetQueryForKeys(iwmongo, "sources", '{"name":"sparktest1"}', "{'name':1,'hdfs_path':1, 'sourceType':1}", 0, 1000)
  # assign("iw.sources", sources, envir = .GlobalEnv)
  for (i in 1:length(sources[[1]])) {
    source_name = sources$name[[i]]
    source_id = sources$X_id[[i]]
    hdfs_path = sources$hdfs_path[[i]]
    #print("init hdfs_path")
    #print(hdfs_path)
    #print(source_name)
    varname <- paste("iw.sources", source_name, sep=".")
    assign(varname, list(name=source_name, id=source_id, hdfs_path=hdfs_path), envir = .GlobalEnv)
  }

  # load source names (source tables will have to be added explicity later)
  dms = dbGetQueryForKeys(iwmongo, "datamodels", "", "{'name':1,'hdfs_path':1}", 0, 1000)
  # TO DEBUG for 1 source sources = dbGetQueryForKeys(iwmongo, "sources", '{"name":"sparktest1"}', "{'name':1,'hdfs_path':1, 'sourceType':1}", 0, 1000)
  # assign("iw.sources", sources, envir = .GlobalEnv)
  for (i in 1:length(dms[[1]])) {
    dm_name = dms$name[[i]]
    dm_id = dms$X_id[[i]]
    hdfs_path = dms$hdfs_path[[i]]
    #print("init hdfs_path")
    #print(hdfs_path)
    #print(source_name)
    varname <- paste("iw.datamodels", dm_name, sep=".")
    assign(varname, list(name=dm_name, id=dm_id, hdfs_path=hdfs_path), envir = .GlobalEnv)
  }
}

InitSpark <- function(sparkMaster = 'local', hdfs_url = "hdfs://ip-172-30-0-245.ec2.internal:8020") {
  #------ spark initialize ----

  # Set this to where Spark is installed
  Sys.setenv(SPARK_HOME="/home/rstudio/spark-1.4.0-bin-hadoop2.6")
  #Sys.setenv(SPARK_HOME="/home/rstudio/sparkcode/spark-1.6.0")
  #Sys.setenv(SPARK_HOME="/usr/hdp/current/spark-client")

  # This line loads SparkR from the installed directory
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
  library(SparkR)

  Sys.setenv('SPARKR_SUBMIT_ARGS'='--packages com.databricks:spark-avro_2.10:2.0.1 sparkr-shell')
  #Sys.setenv('SPARKR_SUBMIT_ARGS'='"--jars" " sparkr-shell')

  sc <<- sparkR.init(master=sparkMaster)
  sqlContext <<- sparkRSQL.init(sc)
  hivecontext <<- sparkRHive.init(sc) #[Spark SQL is not built with Hive support]

  # internal ip to be used in hadoop filepath
  hdfs_url <<- hdfs_url
  print(hdfs_url)
}

Init <- function(db="infoworks-new", host="localhost", port='27017', username='infoworks', password='IN11**rk',
sparkMaster = 'local', hdfs_url = "hdfs://ip-172-30-0-245.ec2.internal:8020") {
  InitMongo(db="infoworks-new", host="localhost", port='27017', username='infoworks', password='IN11**rk')
  InitSpark(sparkMaster = 'local', hdfs_url = "hdfs://ip-172-30-0-245.ec2.internal:8020")
}

# ===============================

# Use config file or get config parameters from iw web app api

Init()
