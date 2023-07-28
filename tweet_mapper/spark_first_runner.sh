#!/bin/bash

# $1 := input (in HDFS)
# $2 := output (in HDFS)

/opt/mapr/spark/spark-2.0.1/bin/spark-submit \
	--master yarn --deploy-mode cluster \
	--jars /home/devanshjain/hadoopPERMA/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar \
	--num-executors 5 --executor-cores 2 --executor-memory 7g \
	--conf spark.yarn.exclude.nodes=[hadoop-d8] \
        spark_overall.py "$1" "$2"  
        #spark_first.py "$1" "$2"
        #createCountry.py "$1" "$2" --user-location-match-only --coordinate-match-only --create-geo-locs
#--py-files /home/juhimittal/spark_india/reverse_geocoder.zip 
#/opt/mapr/spark/spark-2.0.1/bin/spark-submit --master yarn --deploy-mode cluster --jars /home/juhimittal/hadoopPERMA/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar  --num-executors 12 --executor-cores 5 --executor-memory 7g spark_first.py
