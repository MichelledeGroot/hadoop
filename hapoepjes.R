Sys.setenv(HADOOP_OPTS="-Djava.library.path=/opt/hadoop/lib/native")
Sys.setenv(HADOOP_HOME="/opt/hadoop")
Sys.setenv(HADOOP_CMD="/opt/hadoop/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar")
Sys.setenv(JAVA_HOME="/home/michelle/.sdkman/candidates/java/8.0.252.hs-adpt/")

install.packages("./rhdfs_1.0.8.tar.gz", repos=NULL, type="source", dependencies = TRUE)
install.packages("./rmr2_3.3.1.tar.gz", repos=NULL, type="source", dependencies = TRUE)
#install.packages("data.table", dependencies=TRUE)

#library(data.table)
library(rhdfs)
library(rmr2)

setwd("/home/michelle/Documents/hadoep/")
gdp <- read.csv("data/avocado.csv")
head(gdp)
# 
hdfs.init()
gdp.values <- to.dfs(gdp)

localData <- "data/avocado.csv"
hdfs.mkdir("/data")
hdfs.put(localData, "/data/avocado.csv")

calc = mapreduce(input = gdp.values,
                 map = function(k, v) cbind(v, 2*v))



