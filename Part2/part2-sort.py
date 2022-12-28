import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os
import sys

#we are getting the input and outfiles path from the arguments.
inputfile = sys.argv[1]
outputfile = sys.argv[2]

#we are creating the spark application to read the data.
spark = SparkSession.builder.appName("hdfs_sort").getOrCreate()
data=spark.read.csv(inputfile,header=True)

#savePath = "hdfs://10.10.1.1:9000/SortData/SortedResponse.csv"

#sorting the algorithm and storing it in the outputfile.
data.sort(["cca2","timestamp"],ascending=True).write.save(outputfile, format='csv', mode='overwrite')
print("data processing successful!! \n Saved the Sorted results at "+ outputfile )

