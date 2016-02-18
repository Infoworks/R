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
  filepath = OrcFilePath(get(toString(x)))
  ))

  table_full_names = sapply(table_names, function(x) x <- toString(x))
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
        ikey = tablei$fk[[k]]$relations[[1]]$t
        cat('ikey: ')
        cat(ikey)
        cat('\n')

        if (strsplit(ikey, '.', fixed=TRUE)[[1]][[2]] == strsplit(table_full_names[[j]], '.', fixed=TRUE)[[1]][[2]] & is.null(already_joined[[joined_tables]])) {
          print('found join keys')
          join_keys = append(join_keys, tablei$fk[[k]]$relations)
          already_joined[[joined_tables]] = 1
        }
      }
    }
  }
  print(join_keys)
  print(already_joined)

  # register tables
  for (i in 1:length(tables)) {
    print(tables[[i]]$name)

    jdf <- loadDF(hivecontext, tables[[i]]$filepath, "orc") #TODO - Test
    #assign(tables[[i]]$name, read.df(sqlContext,
    #                                 tables[[i]]$filepath,
    #                                 'com.databricks.spark.avro'), .GlobalEnv)
    assign(tables[[i]]$name, jdf, .GlobalEnv)

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
  paste(lapply(join_keys, function(x) x<- paste(modify_colname(x$f_by), '=', modify_colname(x$by), collapse=' ') ), collapse= ' and '),
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
print("data frame used")
df = iwdf_or_table$df
tables = iwdf_or_table$tables
}
else if (class(iwdf_or_table) == "list" && class(iwdf_or_table$name) == 'character') {
  # table name in dot notation variable e.g. tpcds1gb.store
# table name used in the format source.table
print("table name used in the format source.table")
table_var = iwdf_or_table
filepath = OrcFilePath(table_var)
print("filepath is " + filepath)
#df = read.df(sqlContext, filepath, 'com.databricks.spark.avro')
df <- loadDF(hivecontext, filepath, "orc")

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

SelectDF <- function(iwdf_or_table, keep_original_columns = TRUE, ...) {
  if (class(iwdf_or_table) == "list" && class(iwdf_or_table$df) == 'DataFrame') {
  # data frame used
print("data frame used")
df = iwdf_or_table$df
#tables = iwdf_or_table$tables
}
else if (class(iwdf_or_table) == "list" && class(iwdf_or_table$name) == 'character') {
  # table name in dot notation variable e.g. tpcds1gb.store
# table name used in the format source.table
print("table name used in the format source.table")
table_var = iwdf_or_table
filepath = OrcFilePath(table_var)
print(filepath)
#df = read.df(sqlContext, filepath, 'com.databricks.spark.avro')
df <- loadDF(hivecontext, filepath, "orc")

#tables = list(table_var$varname)
#iwdf = list(df=df, tables = tables)
}
else if (class(iwdf_or_table) == 'character') {
  print("hdfs path passed directly as character")
filepath = iwdf_or_table
print(filepath)
#df = read.df(sqlContext, filepath, 'com.databricks.spark.avro')
df <- loadDF(hivecontext, filepath, "orc")
}
else {
  print("iwdf_or_table should be either IWDataframe or a table name as dot notation variable")
}

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

joins = helper.SaveFkey(iwdf, table)
print(joins)

insert_table = list(table=table, source=list('$oid'=get(tables[[1]])$source), fk=joins,
columns=collist, origin='derived', type='table', state='ready')

dbInsertDocument(iwmongo, 'tables', RJSONIO::toJSON(insert_table))
inserted_table_id = dbGetQueryForKeys(iwmongo, "tables", paste("{table:'", table,"'}", sep=''), "{'name':1}", 0, 1000)$X_id[[1]]


mongo.update(iwmongodb, 'infoworks-new.sources',
list("_id"=mongo.oid.from.string(source_id)),
list("$addToSet" = list(tables=mongo.oid.from.string(inserted_table_id))))

# TODO
# save column type

# save sample data / stats


}

helper.SaveFkey <- function(iwdf, table) {
  # - save all fkeys of participating tables
joins = list()
for(tab in iwdf$tables) {
  print(tab)
tabvar = get(tab)

# fk.relation.f_by is of the form "tpcds1gb.store_sales.ss_sold_time_sk"
# if this column is present, persist the fkey

for(fkey in tabvar$fk) {
  colname = fkey$relations[[1]]$f_by
parts = strsplit(colname, '.', fixed=TRUE)[[1]]
new_colname = paste(parts[[2]], parts[[3]], sep = '__')
old_colname = parts[[3]]
if (!is.na(match(new_colname, names(iwdf$df))) || !is.na(match(old_colname, names(iwdf$df)))) {
  joins = append(fkey, joins)
}
}
}

return(joins)
}

SaveTableData <- function(iwdf, table) {
  tableObj = dbGetQueryForKeys(iwmongo, "tables", paste("{table:'", table,"'}", sep=''), "{'name':1, 'source':1}", 0, 1000)
tableId = tableObj$X_id[[1]]
sourceId = tableObj$source[[1]]
# TODO - Test
#filepath = paste(hdfs_url, "/sqoop/", sourceId,"/", tableId, "/merged/avro", sep="")
#write.df(iwdf$df, filepath, "com.databricks.spark.avro")
filepath = OrcFilePath(table_var)
write.df(iwdf$df, filepath, "orc") # This line specifically need to test

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

# --------- Metadata / Statistics generation -------
# Generate statistics and metadata using spark sql and
# store it in mongo
#---------------------

FindDuplicates <- function(iwdf_or_table) {
  df = helper.getDF(iwdf_or_table)

if (is.null(df)) {
  print('Invalid iwdf or table')
return(NULL)
}
registerTempTable(df, "tempdf")
colnames = names(df)
collist = paste(colnames, collapse=", ")

query = paste("select ", collist,
", count(1) as count from tempdf group by ", collist,
" having count(1) > 1 ", sep="")

# print(query)
df_non_unique_rows = sql(sqlContext, query)

return(nrow(df_non_unique_rows))
}

GetRowCount <- function(iwdf_or_table) {
  df = helper.getDF(iwdf_or_table)

if (is.null(df)) {
  print('Invalid iwdf or table')
return(NULL)
}

registerTempTable(df, "tempdf")

query = "select count(*) as rowcount from tempdf"

df_row_count = sql(sqlContext, query)

return(collect(df_row_count)$rowcount[[1]])
}

GetSample <- function(iwdf_or_table, percent, count) {
  df = helper.getDF(iwdf_or_table)

if (is.null(df)) {
  print('Invalid iwdf or table')
return(NULL)
}

if(missing(percent)) {
  if(missing(count)) {
  count = 100
}

# max sample size = 1000
if(count > 1000) {
  count = 1000
}

total_rows = GetRowCount(iwdf_or_table)
fraction = count/total_rows
}
else {
  fraction = percent/100
}

df_local_sample = collect(sample(df, withReplacement = FALSE, fraction))
return(df_local_sample)
}

DetectCategoricalColumns <- function(iwdf_or_table) {
  df = helper.getDF(iwdf_or_table)

if (is.null(df)) {
  print('Invalid iwdf or table')
return(NULL)
}
}



helper.getDF <- function(iwdf_or_table) {
  if (class(iwdf_or_table) == "list" && class(iwdf_or_table$df) == 'DataFrame') {
  # data frame used
df = iwdf_or_table$df
}
else if (class(iwdf_or_table) == "list" && class(iwdf_or_table$name) == 'character') {
  # table name in dot notation variable e.g. tpcds1gb.store
# table name used in the format source.table
table_var = iwdf_or_table
filepath = OrcFilePath(table_var)
#df = read.df(sqlContext, filepath, 'com.databricks.spark.avro')
df <- loadDF(hivecontext, filepath, "orc")
}
else {
  print("iwdf_or_table should be either IWDataframe or a table name as dot notation variable")
df = NULL
}

return(df)
}


# --------- Utility functions ---------

OrcFilePath <-function(table) {
  qualified_name = table$varname
t1 = strsplit(qualified_name, split='.', fixed=TRUE)[[1]]
source_id = get(paste("iw.sources", t1[[1]], sep="."))$id
hdfspath = get(paste("iw.sources", t1[[1]], sep="."))$hdfs_path
table_id = table$id
paste(hdfs_url, hdfspath, "/", table_id, "/merged/orc/*/*/*", sep="")

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


# if colname is of the format table.table__col return as it is
# if colname is of format table.col return table.table__col

modify_colname <- function(colname) {
  parts = strsplit(colname, '.', fixed=TRUE)[[1]]
col = parts[[3]]
colparts = strsplit(col, '__', fixed=TRUE)[[1]]
if (length(colparts) > 1) {
  return(paste(parts[[2]], parts[[3]], sep='.'))
}
else {
  return(paste(parts[[2]], '.', parts[[2]], '__', parts[[3]], sep=''))
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
