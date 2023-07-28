# script to filter original mapping files for a given list of countries

#!/bin/bash

country_filter=$1
source_dir=$2
target_dir=$3

for in_file in $source_dir/*.csv
do
	out_file=`basename $in_file`
    head -n 1 $in_file > $target_dir/$out_file
	grep -wFf $country_filter $in_file >> $target_dir/$out_file
	echo "Filtered $out_file!"
done
