#Util methods for easier querying

###########################################################################################################
#select the table
#params
#iwTable Infoworks table reference
#filterCondition Any filter condition to be applied
#colPrefix Prefix applied for all columns in the table passed
#
iwSelectTable <- function(iwTable, filterCondition = NULL, colPrefix = NULL) {
  
  tblDf <- SelectDF(iwTable)
  
  #filter if required
  if (!is.null(filterCondition) && !missing(filterCondition)){
    tblDf <- filter(tblDf, filterCondition)
  }

  #apply col name prefix if required
  if (!is.null(colPrefix) && !missing(colPrefix)){
    tblDf <- iwSetColNamePrefix(tblDf, colPrefix, TRUE)
  }
  
  return (tblDf)
}
###########################################################################################################


###########################################################################################################
#join tables
#params
#iwTable1 Infoworks table reference
#iwTable2 Infoworks table reference
#joinCondition Condition for joining iwTable1 and iwTable2
#joinType Type of Join: "inner", "outer", "left_outer", "right_outer", "semijoin". Default joinType: "inner"
#filterCondition Any filter condition to be applied
#selectCols List of columns to be selected from the join output
#outColPrefix Prefix applied for all columns in the table passed. Applied last
#
iwJoinTables <- function(iwTable1, iwTable2, joinCondition, joinType = 'inner', filterCondition = NULL, selectCols=NULL, outColPrefix=NULL) {
  
  #TODO allow joincondition with table alias
  #Cant use sparkr api since we cant apply joincondition as a string
  #joinedTableDf <- join(iwTable1, iwTable2, joinCondition, joinType)
  
  randomString <- as.character(floor(abs(rnorm(1)) * 100000))
  #Register both tables as tempTables and then perform sql query
  tbl1Alias <- paste('t1_', randomString, sep='')
  tbl2Alias <- paste('t2_', randomString, sep='')
  #register tables

  registerTempTable(iwTable1, tbl1Alias)
  registerTempTable(iwTable2, tbl2Alias)
  
  sqlToRun <- paste("SELECT * FROM ", tbl1Alias, " JOIN ", tbl2Alias, " WHERE ", joinCondition)
  joinedTableDf <- sql(hivecontext, sqlToRun)
  
  #filter if required
  if (!is.null(filterCondition) && !missing(filterCondition)){
    joinedTableDf <- filter(joinedTableDf, filterCondition)
  }
  
  #select the required cols
  if(!is.null(selectCols) && !missing(selectCols)){
    joinedTableDf <- select(joinedTableDf, lapply(selectCols, function(x) x))
  }
  
  #apply col name prefix if required
  if (!is.null(outColPrefix) && !missing(outColPrefix)){
    joinedTableDf <- iwSetColNamePrefix(joinedTableDf, outColPrefix, TRUE)
  }

  return (joinedTableDf)
}
###########################################################################################################


###########################################################################################################
#group by on columns and provide select columns
#params
#iwTable Infoworks table reference
#groupByCondition Condition for grouping by
#outColPrefix Prefix applied for all columns in the table passed
#select multiple select expressions with alias for each column
#
iwGroupBy <- function(iwTable, groupByCondition, outColPrefix=NULL, ...) {

  randomString <- as.character(floor(abs(rnorm(1)) * 100000))
  #Register both tables as tempTables and then perform sql query
  tbl1Alias <- paste('t1_', randomString, sep='')
  #register tables
  registerTempTable(iwTable, tbl1Alias)

  cols <- paste(groupByCondition, list(...), sep = ", ", collapse = "")
  sqlToRun <- paste("SELECT ", cols," FROM ", tbl1Alias, " GROUP BY ", groupByCondition)
  groupedTableDf <- sql(hivecontext, sqlToRun)
  
  #apply col name prefix if required
  if (!is.null(outColPrefix) && !missing(outColPrefix)){
    groupedTableDf <- iwSetColNamePrefix(groupedTableDf, outColPrefix, TRUE)
  }
  
  return (groupedTableDf)
}
###########################################################################################################