source("iw.R")
library(igraph)

LoadSources(iw.sources.pbm_teradata)
sname = paste(iw.sources.pbm_teradata$name, '.', sep='')

tp = ls()
tp = tp[substring(tp,1,nchar(sname))==sname]
tables = tp[lengths(strsplit(tp, split='.', fixed=TRUE)) == 2]
table_vars = lapply(tables, get)

tm = matrix(0,nrow=length(tables), ncol=length(tables))
rownames(tm) = tables
colnames(tm) = tables

for(tvar in table_vars) {
  if (class(tvar$fk) != 'list') {
    next
  }
  for(fk in tvar$fk) {
    if(!is.null(fk["relations"][[1]]["t"]) && !is.na(fk["relations"][[1]]["t"])){
      if (tvar$varname != change_schema_to_source(fk$relations[[1]]$t, tvar$varname)) {
        source_column = change_schema_to_source(fk$relations[[1]]$t, tvar$varname)
        tm[tvar$varname, source_column] = 1
      }
    }
  }
}
rownames(tm) = gsub("pbm_teradata.", "", rownames(tm))
colnames(tm) = gsub("pbm_teradata.", "", colnames(tm))

g <- graph.adjacency(tm)

plot(g, vertex.size=4, vertex.color='orange', layout=layout_nicely, edge.width=2,
     edge.arrow.size=0.5,edge.arrow.width=0.5,vertex.label.dist=0.25,
     vertex.label.cex=0.75,vertex.label.color='black',edge.color='black')