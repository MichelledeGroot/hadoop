# This script accepts avocado.csv data from https://www.kaggle.com/neuromusic/avocado-prices
# It utilises the hadoop environment to calculate the average price of organic avocados per season.
# Authors: Stephan Gui, Michelle de Groot, Sander Geurts, Joshua Koopmans
# Date: 21-06-2020

# Set environment for hadoop to work.
Sys.setenv(HADOOP_OPTS="-Djava.library.path=/opt/hadoop/lib/native")
Sys.setenv(HADOOP_HOME="/opt/hadoop")
Sys.setenv(HADOOP_CMD="/opt/hadoop/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar")
Sys.setenv(JAVA_HOME="/home/stephan/.sdkman/candidates/java/8.0.252.hs-adpt/jre")

# Add library to current project
library(rhdfs)
library(rmr2)
library(dplyr)

# Add seasons to the data
# Converts date into season
setwd("/home/stephan/Documents/hadoop/hadoop")
avocado_df <- read.csv("data/avocado.csv", sep=",")
avocado_df$Date <- as.Date(avocado_df$Date, format="%Y-%m-%d")

getSeason <- function(DATES) {
  WS <- as.Date("2012-12-15", format = "%Y-%m-%d") # Winter Solstice
  SE <- as.Date("2012-3-15",  format = "%Y-%m-%d") # Spring Equinox
  SS <- as.Date("2012-6-15",  format = "%Y-%m-%d") # Summer Solstice
  FE <- as.Date("2012-9-15",  format = "%Y-%m-%d") # Fall Equinox
  
  # Convert dates from any year to 2012 dates
  d <- as.Date(strftime(DATES, format="2012-%m-%d"))
  
  ifelse (d >= WS | d < SE, "Winter",
          ifelse (d >= SE & d < SS, "Spring",
                  ifelse (d >= SS & d < FE, "Summer", "Fall")))
}
seasons <- getSeason(avocado_df$Date)
avocado_df2 <- cbind(seasons, avocado_df)

#Make csv file from new dataframe ONLY RUN THIS ONCE!
write.csv(avocado_df2, file = "data/avocado2.csv", row.names = FALSE)

# Initialise hdfs
hdfs.init()
avocado.values <- to.dfs(avocado_df2)

# Put the avocado2.csv file in hdfs.
localData <- "data/avocado.csv"
hdfs.mkdir("/data")
hdfs.put(localData, "/data/avocado2.csv")

# Mapper function
avocado.map.fn <- function(k,v) {
  key <- v[1]
  val <- avocado_df2$AveragePrice
  keyval(key,val)
}

# Reduce function
count.reduce.fn <- function(k,v) {
  keyval(k,mean(v))
}

# Calculate the average price of avocados per season
count <- mapreduce(input=avocado.values,
                   map=avocado.map.fn,
                   reduce=count.reduce.fn)
results <- from.dfs(count)

# Make barplot to visualise the results
barplot(results$val,
        main= "Price of avocados per season in USD",
        xlab = "season",
        names.arg = c("Fall", "Spring", "Summer", "Winter"),
        ylab = "Price in USD",
        col = "green"
        )