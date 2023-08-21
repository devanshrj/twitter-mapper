# Twitter Mapper

`overall_files` has the full list of mappings for every country → use to filter the countries that are needed

- `grep -wFf {input-file} overall_files/{file_name}.csv > africa_filters/{file_name}.txt`
- `input-file` is the list of required countries
- [Read a list of input strings to search for](https://stackoverflow.com/a/17863387)

## Steps

1. Move SQL tables to HDFS csv
    - `./mysqlToCSV.bash covid_vaccine "select * from msgs_2020_12" | gzip -c | pv | ssh [devanshjain@hadoop.wwbp.org](mailto:devanshjain@hadoop.wwbp.org) 'cat - | gunzip | hadoop fs -put - /user/devanshjain/msgs_2020_12.csv'`
    - Use `transfer_hadoop.sh` to transfer all tables with one command
2. Filter mappings for required countries
    - Use `filter_mapping.sh country_filter.txt overall_files/ ssa_filters/` to filter based on an input list of countries
    - Use `modify_map_dicts.py` to modify mapping dictionaries → convert all to lowercase and remove duplicates (reduces size of dicts by half)
    - Put mappings on HDFS → `hadoop fs -put mod_ssa_filters/ /user/devanshjain/`
3. Run `spark_first_runner.sh {input-name} {output-name}`
4. Transfer back to `/sandata` on Venti
    - `hadoop fs -cat mapped_msgs_2020_12.csv/* | pv | ssh devanshjain@wwbp "cat -> /sandata/devanshjain/africa_covid/new_data_dump/mapped_msgs_2020_12.csv"` → to transfer single file
    - Can also use `mapped_transfer.sh` to move all files with one command
5. Transfer from `/sandata` to SQL
    - Use `copy_sandata_africa.py`

## Misc

- Remove files from HDFS → `hadoop fs -rm -skipTrash /user/devanshjain/{file-name}`
- `./mysqlToCSV.bash covid_vaccine "select * from msgs_2021_02" | gzip -c | pv | ssh [devanshjain@hadoop.wwbp.org](mailto:devanshjain@hadoop.wwbp.org) 'cat - | gunzip | hadoop fs -put - /user/devanshjain/msgs_2021_02.csv'`
- `hadoop fs -cat mapped_msgs_2021_01.csv/* | pv | ssh devanshjain@wwbp "cat -> /sandata/devanshjain/africa_covid/mapped_msgs_2020_12.csv"`

## Notes

- Create filters: `./filter_mapping.sh country_filter.txt overall_files/ africa_filters/`
- Move filters to HDFS: `hadoop fs -put africa_filters/ /user/devanshjain/`
- Run mapper: `./spark_first_runner.sh msgs_2020_12.csv mapped_msgs_2020_12.csv`
- Remove folder: `hadoop fs -rm -r /user/devanshjain/africa_filters`