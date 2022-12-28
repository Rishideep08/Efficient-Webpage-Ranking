#!/bin/bash


# need to give input details from command line 
#bash {pathTorun.sh} {Path to Spark-submit} {code file path} {hdfs file path for input} {hdf file path for output}
#Example:
#bash ass1-code/Part3/Task4/run.sh /mnt/data/spark-3.3.0-bin-hadoop3/bin/spark-submit ass1-code/Part3/Task4/Ass1_P3task4.py hdfs://10.10.1.1:9000/PageRankData/enwiki-pages-articles/\* hdfs://10.10.1.1:9000/PageRankData/OptiResultsTask4
cmd="$1 $2 $3 $4"
eval ${cmd}

