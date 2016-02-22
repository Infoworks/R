library(igraph)

#x[substring(x,1,nchar(sname))==sname]
# sname = iw.sources.tpcds1gb$name
#sname = paste(iw.sources.NorthwindOracle$name, '.', sep='')
sname = paste(iw.sources.tpcds1gb$name, '.', sep='')
tp = ls()
tp = tp[substring(tp,1,nchar(sname))==sname]
# lengths(strsplit(tp, split='.', fixed=TRUE)) == 2
tables = tp[lengths(strsplit(tp, split='.', fixed=TRUE)) == 2]
table_vars = lapply(tables, get)

tm = matrix(0,nrow=length(tables), ncol=length(tables))
rownames(tm) = tables
colnames(tm) = tables

lapply(table_vars, function(x) for(fk in x$fk) if (table_vars$varname != fk$relations[[1]]$t) {return(list(table_vars$varname, fk$relations[[1]]$t, 1))} )
lapply(table_vars, function(x) length(x$fk) )

cat('---- FOREIGN KEYS ----\n')
for(tvar in table_vars) {
  if (class(tvar$fk) != 'list') {
    cat(tvar$varname)
    cat(": No fkeys\n")
    next
  }
  count=0
  for(fk in tvar$fk) {
    if (tvar$varname != change_schema_to_source(fk$relations[[1]]$t, tvar$varname)) {
      count = count+1
      cat(count)
      cat('. ')
      cat(tvar$varname)
      cat(' ---> ')
      cat(fk$relations[[1]]$t)
      cat('\n')
      cat('[')
      cat(fk$relations[[1]]$f_by)
      cat(' : ')
      cat(fk$relations[[1]]$by)
      cat(']')
      cat('\n\n')
      source_column = change_schema_to_source(fk$relations[[1]]$t, tvar$varname)
      tm[tvar$varname, source_column] = 1
    }
  }
  if (count == 0) {
    cat(tvar$varname)
    cat(": No fkeys\n")
  }
  cat('------------------------------------------------------\n')
}

change_schema_to_source <- function(rel, var) {
  sourcename = strsplit(var, '.', fixed=TRUE)[[1]][[1]]
  paste(sourcename, LastDotValue(rel), sep='.')
}

g <- graph.adjacency(tm)
plot(g, vertex.size=2, vertex.color='orange', layout=layout.circle)


