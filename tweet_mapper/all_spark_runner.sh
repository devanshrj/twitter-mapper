#!/bin/bash

for month in $(seq -w 01 11)
    do 
    echo "Mapping tweets for $month/2022"
    bash spark_first_runner.sh msgs_2022_${month}.csv mapped_msgs_2022_${month}.csv
    echo "Tweets for $month/2022 mapped!"
    done
