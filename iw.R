Sys.setenv(HADOOP_HOME="/usr/lib/hadoop")
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar")
Sys.setenv(SPARK_HOME="/home/rstudio/spark-1.5.0-bin-hadoop2.4")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
Sys.setenv('SPARKR_SUBMIT_ARGS'='--packages com.databricks:spark-avro_2.10:2.0.1 sparkr-shell')

library(RJSONIO)
library(RMongo)
library(rmongodb)
library(rmr2)
library(rhdfs)
library(SparkR)
library(rhdfs)
# initiate rhdfs package
hdfs.init()
avro.jar = "/home/rstudio/avro-mapred-1.7.4-hadoop1.jar"

db="infoworks-new"
host="localhost"
port='27017'
username='infoworks'
password='IN11**rk'
sparkMaster = 'local'
hdfs_url = "hdfs://ip-10-37-200-15.ec2.internal:8020"
bp  = rmr.options("backend.parameters")
bp$hadoop[1] = "mapreduce.map.java.opts=-Xmx2048M"
bp$hadoop[2] = "mapreduce.reduce.java.opts=-Xmx2048M"
rmr.options(backend.parameters = bp)
rmr.options(hdfs.tempdir = "/user/rstudio")

iwmongo <<- mongoDbConnect(dbName=db, host=host, port=port)
dbAuthenticate(iwmongo, username, password)
iwmongodb <<- mongo.create(host=host, name="", username=username, password=password, db=db)
sources = dbGetQueryForKeys(iwmongo, "sources", "", "{'name':1}", 0, 1000)
for (i in 1:length(sources[[1]])) {
  source_name = sources$name[[i]]
  source_id = sources$X_id[[i]]
  varname <- paste("iw.sources", source_name, sep=".")
  assign(varname, list(name=source_name, id=source_id), envir = .GlobalEnv)
}

sc <<- sparkR.init(master=sparkMaster)
sqlContext <<- sparkRSQL.init(sc)

# internal ip to be used in hadoop filepath
hdfs_url <<- hdfs_url 

#--------------Mongodb API-------------------
LoadSources <- function(...) {
  args = as.list(match.call())
  args_sources = args[2:length(args)]
  args_sources = lapply(args_sources, deparse)
  
  source_names = paste("'", paste(lapply(args_sources, LastDotValue), collapse="', '"), "'", sep="")
  sources = dbGetQueryForKeys(iwmongo, "sources", paste("{name: { $in: [", source_names, " ]}}", sep=""), "{'name':1, 'tables':1}", 0, 1000)
  assign("iw.sources", sources, envir = .GlobalEnv)
  
  sources_loaded = c()
  for (i in 1:length(sources[[1]])) {
    source_name = sources$name[[i]]
    source_id = sources$X_id[[i]]
    tables = sources$tables[[i]]
    
    # removed initial iw.sources from variable names
    varname <- paste("", source_name, sep="")
    assign(varname, list(name=source_name, id=source_id), envir = .GlobalEnv)
    #varname <- paste(varname, "tables", sep=".")
    
    # get table details
    tables = dbGetQueryForKeys(iwmongo, "tables", 
                               paste("{'source': { '$oid': '", source_id, "' }}", sep=""), 
                               "{'table':1, 'columns':1, 'fk':1, 'source':1}", 0, 10000)
    
    
    sources_loaded = c(sources_loaded, paste(source_name, nrow(tables), sep=", tables: "))
    if (nrow(tables) == 0) next
    # for (j in 1:0) runs the loop twice, so the following for loop is still executed
    
    for (j in 1:nrow(tables)) {
      table_name = tables$table[[j]]
      table_id = tables$X_id[[j]]
      table_columns = tables$columns[[j]]
      table_columns_json = fromJSON(table_columns)
      table_col_count = length(table_columns_json)
      fk = tryCatch(fromJSON(tables$fk[[j]]), error = function(e) {''})
      
      varname1 <- paste(varname, table_name, sep=".")
      assign(varname1, 
             list(varname=varname1, name=table_name, id=table_id, 
                  table_col_count=table_col_count, fk=fk, source=tables$source[[j]]),
             envir = .GlobalEnv)
      #varname1 <- paste(varname1, "column", sep=".")
      #assign(varname1, table_columns_json)
      
      if (length(table_columns_json) == 0) next
      for (k in 1:length(table_columns_json)) {
        column_name = table_columns_json[[k]][[1]]
        varname2 <- paste(varname1, column_name, sep=".")
        cols = table_columns_json[[k]]
        cols[["index"]] = k
        cols[["varname"]] = varname2
        cols[["table_col_count"]] = table_col_count
        assign(varname2, cols, envir = .GlobalEnv)
      }
    }
  }
  print("Source definition loaded")
}

UnloadSources <- function() {
  
}

Columns <- function(table) {
  
}

Tables <- function(source) {
  
}

Sources <- function(domain) {
  
}

TableDetails <- function(table) {
  
}

SourceDetails <- function(source) {
  
}

#------- Helper Functions ----------

# extracts last string from a string in dot notation
# 'iw.sources.abc' -> extracts 'abc'
# 'source1.table1.column1' -> extracts 'column1'
LastDotValue <- function(v) {
  parts = strsplit(v, '.', fixed=TRUE)
  if(length(parts) > 0){
    p = parts[[1]]
    return(p[[length(p)]])
  }else{
    return(0)
  }
  
}

iwview <- function(iwdf, collect=TRUE) {
  if (collect) {
    View(collect(iwdf$df))
  }
  else {
    View(head(iwdf$df))    
  }
}

# ---------- SPARK SQL api ---------

# Join 
# join tables and automatically get join conditions
# 
# TODO(deepak)
# ability to specify join condition either in paramter or interactively from a list of choices
#   This is necessary as there will be ambiguous/multiple join criterion among a group of tables
#   e.g. (store_sales, customer, customer_address) -> join can be between store_sales:customer_address
#        or between customer:customer_address
# Usage
# jdf1 = Join(tpcds1gb.store_sales, tpcds1gb.store, tpcds1gb.customer)
#

Join <- function(...) {
  args = as.list(match.call())
  table_names = args[2:length(args)]
  
  # get table name and hdfs file path
  tables = lapply(table_names, 
                  function(x) x <- list(name = strsplit(toString(x), '.', fixed=TRUE)[[1]][[2]], 
                                        filepath = AvroFilePath(get(toString(x)))
                  ))
  
  table_full_names = lapply(table_names, function(x) x <- toString(x))
  print(table_full_names)
  
  # find the join criterion assuming that it is unambiguous
  join_keys = list()
  already_joined = list()
  for (i in 1:length(table_names)) {
    for (j in 1:length(table_names)) {
      if (i == j) next
      joined_tables = paste(tables[[i]][[1]], tables[[j]][[1]], sep='::')
      print(joined_tables)
      tablei = get(toString(table_names[[i]]))
      tablej = get(toString(table_names[[j]]))
      
      for (k in 1:length(tablei$fk)) {
        ikey = print(tablei$fk[[k]]$relations[[1]]$t)
        
        if (ikey == table_full_names[[j]] & is.null(already_joined[[joined_tables]])) {
          print('found join keys')
          join_keys = append(join_keys, tablei$fk[[k]]$relations)
          already_joined[[joined_tables]] = 1
        }
      }
    }
  }
  print(join_keys)
  
  # register tables
  for (i in 1:length(tables)) {
    print(tables[[i]]$name)
    
    assign(tables[[i]]$name, read.df(sqlContext, 
                                     tables[[i]]$filepath, 
                                     'com.databricks.spark.avro'), .GlobalEnv)
    
    # change column name to table__column if the table is original (not derived)
    table_obj = get(toString(table_names[[i]]))
    if (is.null(table_obj$origin) || table_obj$origin == 'original') {
      # change column names using withColumnRenamed
      for (col in names(get(tables[[i]]$name))) {
        assign(tables[[i]]$name, withColumnRenamed(get(tables[[i]]$name), col, paste(tables[[i]]$name, col, sep="__")))
      }
    }
    registerTempTable(get(tables[[i]]$name), tables[[i]]$name)
  }
  # create join query
  sql_query = paste("select * from ", 
                    paste(lapply(tables, function(x) x<-x$name), collapse=', '),
                    "where ",
                    paste(lapply(join_keys, function(x) x<- paste(twolevel(x$f_by), '=', twolevel_(x, x$by), collapse=' ') ), collapse= ' and '),
                    sep=' '
  )
  print(sql_query)
  df = sql(sqlContext, sql_query)
  iwdf = list(df=df, tables=lapply(table_names, toString))
}


# Select
#
# select table and a subset of columns, also create new columns
# TODO(deepak)
# specify columns and column transformation as expression along with string and sql expression
# Usage
# Select(tpcds1gb.store_sales, 'ss_store_sk', 'ss_quantity * ss_sales_price as total_sales_amount')
# storeiw = Select(tpcds1gb.store, TRUE, "concat_ws('::', s_country, s_city) as location")
#
Select <- function(iwdf_or_table, keep_original_columns = TRUE, ...) {
  if (class(iwdf_or_table) == "list" && class(iwdf_or_table$df) == 'DataFrame') {
    # data frame used
    df = iwdf_or_table$df
    tables = iwdf_or_table$tables
  }
  else if (class(iwdf_or_table) == "list" && class(iwdf_or_table$name) == 'character') {
    # table name in dot notation variable e.g. tpcds1gb.store
    # table name used in the format source.table
    table_var = iwdf_or_table
    filepath = AvroFilePath(table_var)
    df = read.df(sqlContext, 
                 filepath, 
                 'com.databricks.spark.avro')
    tables = list(table_var$varname)
    iwdf = list(df=df, tables = tables)
  }
  else {
    print("iwdf_or_table should be either IWDataframe or a table name as dot notation variable")
  }
  
  # keep original columns if required
  if(keep_original_columns) {
    all_args = append(df, names(df))
  }
  else {
    all_args = append(df, list())
  }
  
  args = as.list(match.call())
  if (length(args) > 3) {
    cols = args[4:length(args)]
    all_args = append(all_args, cols)
    df = do.call(SparkR::selectExpr, all_args)
  }
  
  list(df=df, tables = tables)
}

# Filter (where in sql)
#
# Filter on df 
# TODO
# specify where clause as expression using column names
# 

Filter <- function(iwdf, condition) {
  list(df=SparkR::filter(iwdf$df, condition), tables = iwdf$tables)
}


# Aggregate (group by in sql)
#
# aggregate by few columns and get aggregate values
# Usage
# storeagg1 = Aggregate(storeiw, tpcds1gb.store.s_city, count = n(tpcds1gb.store.s_store_id))
# fed1 = Filter(Aggregate(storeiw, tpcds1gb.store.s_city, count = n(tpcds1gb.store.s_store_id)), 's_city = "Midway"')

Aggregate <- function(iwdf, groupBy, ...) {
  args = match.call()
  df = iwdf$df
  
  if(length(args[[3]]) == 1) {
    # group by on single column
    cols = list(ExtractColumnName(groupBy))
  }  
  else {
    # multiple column names
    cols = lapply(groupBy, ExtractColumnName)
  }
  
  # create expression for group by on multiple column
  fullarg = paste("do.call(summarize, list(groupBy(df, '", paste(cols, collapse="', '"), "') ", sep= "")
  
  # aggregate function specified
  # print(args[[4]])
  if (length(args) > 3) {
    for (i in 4:length(args)) {
      #print(args[[i]][[2]])
      print(class(args[[i]][[2]]))
      if (class(args[[i]][[2]]) == 'character') {
        col = args[[i]][[2]]
      }
      else {
        col = ExtractColumnName(get(deparse(args[[i]][[2]])))
      }
      fullarg = paste(fullarg, ',', names(args)[[i]], '=', args[[i]][[1]], '(df$', col, ')', sep='')
    }
  }
  
  fullarg = paste(fullarg, '))', sep='')
  
  print('fullarg')
  print(fullarg)
  
  newdf = eval(parse(text=fullarg)[[1]])
  list(df=newdf, tables = iwdf$tables)
  # do.call(summarize, list(groupBy(df, "s_city", "s_country"), s1 = sum(df$ss_net_paid), s2 = avg(df$ss_net_profit)))
}

# --------- Table schema functions ------

DeleteTableSchema <- function(tablepath) {
  parts = strsplit(tablepath, '.', fixed=TRUE)[[1]]
  table = parts[[2]]
  source = parts[[1]]
  tableObj = dbGetQueryForKeys(iwmongo, "tables", 
                               paste("{table:'", table,"'}, {source:'", source, "'}}", sep=''), 
                               "{'table':1, 'source':1, 'origin':1}", 0, 1000)
  if(nrow(tableObj) == 0) {
    return('No such table exists')
  }
  
  tableid = tableObj$X_id[[1]]
  # tableid = dbGetQueryForKeys(iwmongo, "tables", paste("{table:'", tablename,"'}", sep=''), "{'name':1}", 0, 1000)$X_id[[1]]
  
  if(!is.na(tableObj$origin[[1]]) && tableObj$origin[[1]] == 'derived') {
    mongo.update(iwmongodb, 'infoworks-new.sources',
                 list(name = source),
                 list("$pull" = list(tables=mongo.oid.from.string(tableid))))  
    dbRemoveQuery(iwmongo, 'tables', paste("{table:'", table,"'}", sep=''))
  }
  else {
    print('Can not delete original table schema')
  }
}

SaveTableSchema <- function(iwdf, table) {
  # save columns and foreign keys
  df = iwdf$df
  tables = iwdf$tables
  # assumption: all tables in dataframe are from on source. 
  # For multiple source decide where to save it
  source_id = get(tables[[1]])$source
  
  cols = columns(df)
  collist = list()
  for (i in seq(cols)) {
    collist[[i]] = list(name=cols[[i]], at=i)
  }
  
  source_info = list('$oid'=get(tables[[1]])$source)
  insert_table = list(table=table, source=list('$oid'=get(tables[[1]])$source), 
                      columns=collist, origin='derived', type='table', state='ready')
  
  dbInsertDocument(iwmongo, 'tables', RJSONIO::toJSON(insert_table))
  inserted_table_id = dbGetQueryForKeys(iwmongo, "tables", paste("{table:'", table,"'}", sep=''), "{'name':1}", 0, 1000)$X_id[[1]]
  
  
  mongo.update(iwmongodb, 'infoworks-new.sources', 
               list("_id"=mongo.oid.from.string(source_id)), 
               list("$addToSet" = list(tables=mongo.oid.from.string(inserted_table_id))))
  
  # TODO
  # column type
  # foreign keys
  # sample data / stats
}

SaveTableData <- function(iwdf, table) {
  tableObj = dbGetQueryForKeys(iwmongo, "tables", paste("{table:'", table,"'}", sep=''), "{'name':1, 'source':1}", 0, 1000)
  tableId = tableObj$X_id[[1]]
  sourceId = tableObj$source[[1]]
  filepath = paste(hdfs_url, "/sqoop/", sourceId,"/", tableId, "/merged/avro", sep="")
  write.df(iwdf$df, filepath, "com.databricks.spark.avro")
  return(filepath)
}

# TODO: make it pass tpcds.store instead of "tpcds.store"
DeleteTableData <- function(tablepath) {
  parts = strsplit(tablepath, '.', fixed=TRUE)[[1]]
  table = parts[[2]]
  source = parts[[1]]
  tableObj = dbGetQueryForKeys(iwmongo, "tables", 
                               paste("{table:'", table,"'}, {source:'", source, "'}}", sep=''), 
                               "{'table':1, 'source':1, 'origin':1}", 0, 1000)
  tableId = tableObj$X_id[[1]]
  sourceId = tableObj$source[[1]]
  filepath = paste(hdfs_url, "/sqoop/", sourceId,"/", tableId, "/merged/avro", sep="")
  
  if(!is.null(tableObj$origin[[1]]) && tableObj$origin[[1]] == 'derived') {
    # delete table data
    hdfs.del(filepath)
  }
  else {
    print('Can not delete original table data')
  }
}

# --------- Utility functions ---------

AvroFilePath <-function(table) {
  qualified_name = table$varname
  t1 = strsplit(qualified_name, split='.', fixed=TRUE)[[1]]
  source_id = get(paste("iw.sources", t1[[1]], sep="."))$id
  table_id = table$id
  paste(hdfs_url, "/sqoop/", source_id,"/", table_id, "/merged/avro", sep="")
  #files = hdfs.ls(folder)
  #c(lapply(files$file, function(x) paste(hdfs_root, x, sep=""))[[1]])
}

# ---------- helper functions ---------
ExtractColumnName <- function(col) {
  print(class(col))
  if (class(col) == 'character') {
    return(col)
  }
  else {
    parts = strsplit(col$varname, '.', fixed=TRUE)[[1]]
    return(parts[[length(parts)]])
  }
}

# Couple of helper functions to take care of existing bug
# due to which join key by table is wrong in mongodb
# https://infoworks.atlassian.net/browse/IPD-473
twolevel <- function(x) {
  ret = paste(strsplit(toString(x), '.', fixed=TRUE)[[1]][[2]], strsplit(toString(x), '.', fixed=TRUE)[[1]][[3]], sep='__')
  return(ret)
}

twolevel_ <- function(x, val) {
  ret = paste(strsplit(x$t, '.',fixed=TRUE)[[1]][[2]] , strsplit(val, x$ft,fixed=TRUE)[[1]][[2]], sep='__')
  return(ret)
}

change_schema_to_source <- function(rel, var) {
  sourcename = strsplit(var, '.', fixed=TRUE)[[1]][[1]]
  paste(sourcename, LastDotValue(rel), sep='.')
}