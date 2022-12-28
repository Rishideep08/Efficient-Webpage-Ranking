import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,lit
import time
import os
import sys

#we are using inbuilt hash function which is polynomial rolling hash function
def myHash(data):
  return hash(data)

#function to get the contribution of source to each of its destination and its self contribution.
def getContribution(source,destinationList, rank):
  myTuplesArr = [];
  #for each of its destination updaing their rank with the dampling factor.
  for item in destinationList:
    myTuplesArr.append((item, rank/len(destinationList) * 0.85))

  #updating its own rank and adding it to the tuple
  myTuplesArr.append((source, rank * 0.15))
  return myTuplesArr

#we are getting the input and outfiles path from the arguments and starting the timer to check the computation time
start = time.time()
inputDir = sys.argv[1]
outputDir = sys.argv[2]

#we are setting the no of partitions
partitions =100

#we are creating the spark application to read the data.
spark = SparkSession.builder.appName("hdfs_OptimizedVersionWithCache").getOrCreate()
sc=spark.sparkContext

#we are setting the log level to warn to reduce the I/O operations. If it is set to INFO then we used to have many operations.
sc.setLogLevel("WARN")

#reading the data.
data_rdd=sc.textFile(inputDir, 100)

#we are filtering the first few lines which represents the information and maping the remaining lines for source and desctination pairs.
linksData_rdd= data_rdd\
        .map(lambda line: [line.split("\t")[0].lower(),[line.split("\t")[1].lower()]])\
        .filter(lambda sourceDestPair:(":" not in sourceDestPair[0] or sourceDestPair[0].split(":")[0] == "category") and\
        (":" not in sourceDestPair[1][0] or sourceDestPair[1][0].split(":")[0] == "category"))
        
#reducing based on the key and cache the data so that it will help in optimizing by reducing the disk writes.
combinedlinksData_rdd = linksData_rdd.reduceByKey(lambda v1, v2: v1+v2, partitions, myHash).cache()

#initializing the ranks
ranks_rdd= combinedlinksData_rdd.map(lambda data: (data[0],1),True)

#we are iterating it 10 times
for i in range(10):
  #joining the ranks of the source with its destination links.
  #then we are doing the contribution for each of the destination from its source and updaing its own rank in the getContribution.
  #later we made a flatMap which make all content in to a single array so we can add them directly.
  contributions_rdd = combinedlinksData_rdd\
  .join(ranks_rdd)\
  .flatMap(lambda combinedData: getContribution(combinedData[0],combinedData[1][0],combinedData[1][1]))

  #adding all ranks for the source key and using this updated rank RDD for next iterations.
  #we are also caching the data so it will help in optimizing the code by reducing the disk writes.
  ranks_rdd = contributions_rdd.reduceByKey(lambda v1,v2: v1+v2, partitions, myHash).cache()

#sorting the ranks in decrease order of their ranks.
ranks_rdd.sortBy(lambda pageRank: pageRank[1],False).saveAsTextFile(outputDir)

#getting the time so we can calculate the total process time.
end= time.time()
print(f"Page Rank algorithm completed with exection time: {end-start} seconds")
