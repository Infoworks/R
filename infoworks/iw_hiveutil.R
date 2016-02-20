#InitSpark()
#InitMongo()

#loading libraries
library("DBI")
library("rJava")
library("RJDBC")

options( java.parameters = "-Xmx8g" )


#hivecontext = 
drv <- JDBC("org.apache.hive.jdbc.HiveDriver",
            c( list.files("/usr/lib/hadoop/client",pattern="jar$",full.names=T),
              #list.files("/home/rstudio/jars/",pattern="jar$",full.names=T),
              list.files("/usr/lib/hive/lib",pattern="jar$",full.names=T)
              #list.files("/home/rstudio/hive121",pattern="jar$",full.names=T)
            ))

conn <- dbConnect(drv, "jdbc:hive2://localhost:10000/default", "", "")
show_databases <- dbSendQuery(conn, "show databases")
data <- fetch(show_databases, n = -1)
print(data)

