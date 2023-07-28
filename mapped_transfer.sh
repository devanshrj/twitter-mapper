#!/bin/bash

echo "Transferring mapped messages for 12/2020"
hadoop fs -cat /user/devanshjain/mapped_msgs_2020_12.csv/* | pv | ssh devanshjain@wwbp "cat -> /sandata/devanshjain/africa_covid/new_data_dump/mapped_msgs_2020_12.csv"
echo "Messages for 12/2020 transferred!"

for month in $(seq -w 01 12)
     do 
     echo "Transferring mapped messages for $month/2021"
     hadoop fs -cat /user/devanshjain/mapped_msgs_2021_${month}.csv/* | pv | ssh devanshjain@wwbp "cat -> /sandata/devanshjain/africa_covid/new_data_dump/mapped_msgs_2021_${month}.csv"
     echo "Messages for $month/2021 transferred!"
     done

for month in $(seq -w 01 11)
     do 
     echo "Transferring mapped messages for $month/2022"
     hadoop fs -cat /user/devanshjain/mapped_msgs_2022_${month}.csv/* | pv | ssh devanshjain@wwbp "cat -> /sandata/devanshjain/africa_covid/new_data_dump/mapped_msgs_2022_${month}.csv"
     echo "Messages for $month/2022 transferred!"
     done
