#Tools / Utils methods

aliasDF <- function(df, alias_prefix, uppercase = FALSE) {
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

printDf <- function(df, name, numRows=20) {
  print("VVVVVVVVVVVVVVVVVVVV")
  print(name)
  print(names(df))
  showDF(df, numRows)
  print(name)
  print("^^^^^^^^^^^^^^^^^^^^^^^^")
}
