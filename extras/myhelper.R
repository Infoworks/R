
iwview <- function(iwdf, collect=FALSE) {
  if (collect) {
    View(collect(iwdf$df))
  }
  else {
    View(head(iwdf$df))    
  }
}
