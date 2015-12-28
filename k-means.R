Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")
require(rJava)
require(rhdfs)
require(RJSONIO)
require(jsonlite)
require(graphics)

hdfs.init()

json_file <- hdfs.read.text.file('/RX_TB/DW.json')
json <- fromJSON(json_file)
tmp = as.data.frame(do.call(rbind, json))
tmp.df <- data.frame(tmp)
x <- tmp.df[,c(4,6,7,39)]
View(x)
km <- kmeans(x, 3, 50)
plot(x, col = km$cluster)
points(km$centers, col = 1:2, pch = "+", cex = 2)