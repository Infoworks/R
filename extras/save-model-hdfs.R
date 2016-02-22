Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")
require(rJava)
require(rhdfs)
require(graphics)

hdfs.init()
## Annette Dobson (1990) "An Introduction to Generalized Linear Models".
## Page 9: Plant Weight Data.
ctl <- c(4.17,5.58,5.18,6.11,4.50,4.61,5.17,4.53,5.33,5.14)
trt <- c(4.81,4.17,4.41,3.59,5.87,3.83,6.03,4.89,4.32,4.69)
group <- gl(2, 10, 20, labels = c("Ctl","Trt"))
weight <- c(ctl, trt)

##Save on HDFS
model <- lm(weight ~ group)
modelfilename <- "D9"
modelfile <- hdfs.file(modelfilename, "w")
hdfs.write(model, modelfile)
hdfs.close(modelfile)

model <- lm(weight ~ group - 1)
modelfilename <- "D90"
modelfile <- hdfs.file(modelfilename, "w")
hdfs.write(model, modelfile)
hdfs.close(modelfile)

##Read from HDFS
modelfilename <- "D9"
modelfile = hdfs.file(modelfilename, "r")
m <- hdfs.read(modelfile)
modelD9 <- unserialize(m)
hdfs.close(modelfile)

modelfilename <- "D90"
modelfile = hdfs.file(modelfilename, "r")
m <- hdfs.read(modelfile)
modelD90 <- unserialize(m)
hdfs.close(modelfile)


anova(modelD9)
summary(modelD90)

opar <- par(mfrow = c(2,2), oma = c(0, 0, 1.1, 0))
plot(modelD9, las = 1)      # Residuals, Fitted, ...
par(opar)