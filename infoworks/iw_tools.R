#Tools / Utils methods

iwSetColNamePrefix <- function(df, alias_prefix, uppercase = FALSE) {  
  col_names <- names(df)
  new_col_names <- lapply(col_names,
  function(x) {
    c <- paste(alias_prefix, "_", x, sep = "")
    if(uppercase) {
      c <- toupper(c)
    }
    return (c)
  })
  for(c in 1:length(col_names)){
    df <- withColumnRenamed(df, col_names[[c]], new_col_names[[c]])
  }
  return (df)
}

aliasDF <- function(df, alias_prefix, uppercase = FALSE) {
  return (iwSetColNamePrefix(df, alias_prefix, uppercase))
}


iwPrintTable <- function(df, name, numRows = 20) {
  print("VVVVVVVVVVVVVVVVVVVV")
  showDF(df, numRows)
  print(name)
  #print(paste(name, ", numRows : ", count(df)))
  print("^^^^^^^^^^^^^^^^^^^^^^^^")
}

printDf <- function(df, name, numRows = 20) {
  iwPrintTable(df, name, numRows)
}