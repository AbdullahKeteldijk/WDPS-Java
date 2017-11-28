#!/usr/bin/env bash

ATT=${1:-"WARC-Record-ID"}
INFILE=${2:-"hdfs:///user/bbkruit/CommonCrawl-sample.warc.gz"}
JARFILE=${3:-"/home/wdps1703/testJava/WDPS-Java/target/NLP-jar-with-dependencies.jar"}
LOCALMODE=${4:-"false"}


~/spark-2.1.2-bin-without-hadoop/bin/spark-submit --class Spark.Archiv --executor-memory 5g --num-executors 40 --conf spark.memory.fraction=0.8 --conf spark.yarn.am.memory=6g --master yarn $JARFILE $INFILE $LOCALMODE
