Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")
require(rJava)
require(rhdfs)
require(RJSONIO)
require(jsonlite)
require(graphics)

hdfs.init()

json_file <- hdfs.read.text.file('/RX_TB/CVS.json')
lines = strsplit(json_file, "\n")
json_data0 = paste(lines, collapse=', ')
json_data = paste(c("[", json_data0, "]"), collapse = '')
json <- fromJSON(json_data)
tmp = as.data.frame(do.call(rbind, json))
tmp.df <- data.frame(tmp)
x <- tmp.df[,c(18)]
plot(density(x))