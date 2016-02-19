#Tools / Utils methods

aliasDF <- function(df, alias_prefix) {
  col_names <- names(df)
  new_col_names <- lapply(col_names, function(x) paste(alias_prefix, "_", x, sep = ""))
  for(c in 1:length(col_names)){
    df <- withColumnRenamed(df, col_names[[c]], new_col_names[[c]])
  }
  return (df)
}
