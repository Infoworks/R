#-------- API ---------------------

LoadSources <- function(...) {
  args = as.list(match.call())
  args_sources = args[2:length(args)]
  args_sources = lapply(args_sources, deparse)

  source_names = paste("'", paste(lapply(args_sources, LastDotValue), collapse="', '"), "'", sep="")
  print(source_names)

  sources = dbGetQueryForKeys(iwmongo, "sources", paste("{name: { $in: [", source_names, " ]}}", sep=""), "{'name':1, 'tables':1, 'hdfs_path':1}", 0, 1000)
  assign("iw.sources", sources, envir = .GlobalEnv)

  sources_loaded = c()
  for (i in 1:length(sources[[1]])) {
    source_name = sources$name[[i]]
    source_id = sources$X_id[[i]]
    hdfs_path = sources$hdfs_path[[i]]
    print("hdfs path ")
    print( hdfs_path)
    tables = sources$tables[[i]]

    # removed initial iw.sources from variable names
    #varname <- paste("", source_name, sep="")
    varname <- source_name
    assign(varname, list(name=source_name, id=source_id, hdfs_path=hdfs_path), envir = .GlobalEnv)

    # get table detail
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
        column_name = table_columns_json[[k]][["name"]]
        varname2 <- paste(varname1, column_name, sep=".")
        cols = table_columns_json[[k]]
        cols[["index"]] = k
        cols[["varname"]] = varname2
        cols[["table_col_count"]] = table_col_count
        assign(varname2, cols, envir = .GlobalEnv)
      }
    }
  }

  print("Sources loaded:")
  print(sources_loaded)
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

LoadDatamodels <- function(...) {
  args = as.list(match.call())
  args_datamodels = args[2:length(args)]
  args_datamodels = lapply(args_datamodels, deparse)

  datamodel_names = paste("'", paste(lapply(args_datamodels, LastDotValue), collapse="', '"), "'", sep="")
  print(datamodel_names)

  datamodels = dbGetQueryForKeys(iwmongo, "datamodels", paste("{name: { $in: [", datamodel_names, " ]}}", sep=""), "{'name':1, 'tables':1, 'hdfs_path':1}", 0, 1000)
  assign("iw.datamodels", datamodels, envir = .GlobalEnv)

  datamodels_loaded = c()
  for (i in 1:length(datamodels[[1]])) {
    datamodel_name = datamodels$name[[i]]
    datamodel_id = datamodels$X_id[[i]]
    hdfs_path = datamodels$hdfs_path[[i]]
    print("hdfs path ")
    print( hdfs_path)
    tables = datamodels$tables[[i]]

    # removed initial iw.datamodels from variable names
    #varname <- paste("", datamodel_name, sep="")
    varname <- datamodel_name
    assign(varname, list(name=datamodel_name, id=datamodel_id, hdfs_path=hdfs_path), envir = .GlobalEnv)

    # get table detail
    tables = dbGetQueryForKeys(iwmongo, "tables",
    paste("{'datamodel': { '$oid': '", datamodel_id, "' }}", sep=""),
    "{'table':1, 'columns':1, 'fk':1, 'datamodel':1}", 0, 10000)

    datamodels_loaded = c(datamodels_loaded, paste(datamodel_name, nrow(tables), sep=", tables: "))
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
      table_col_count=table_col_count, fk=fk, datamodel=tables$datamodel[[j]]),
      envir = .GlobalEnv)
      #varname1 <- paste(varname1, "column", sep=".")
      #assign(varname1, table_columns_json)

      if (length(table_columns_json) == 0) next
      for (k in 1:length(table_columns_json)) {
        column_name = table_columns_json[[k]][["name"]]
        varname2 <- paste(varname1, column_name, sep=".")
        cols = table_columns_json[[k]]
        cols[["index"]] = k
        cols[["varname"]] = varname2
        cols[["table_col_count"]] = table_col_count
        assign(varname2, cols, envir = .GlobalEnv)
      }
    }
  }

  print("Datamodels loaded:")
  print(datamodels_loaded)
}

#------- Helper Functions ----------

# extracts last string from a string in dot notation
# 'iw.sources.abc' -> extracts 'abc'
# 'source1.table1.column1' -> extracts 'column1'
LastDotValue <- function(var) {
  parts = strsplit(var, '.', fixed=TRUE)[[1]]
  return(parts[[length(parts)]])
}
