import argparse
import re
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder \
        .appName('row_counter') \
        .getOrCreate()

def user_lookup(args):
    ## here args is the dictionaries mapping the location to whatever info you can get in terms of city, district, state, country.

    def f(user_location):
        location = str(user_location)
        location_comma = re.sub('\W+,',' ', location)
        location_comma = location_comma.lower().split(',')
        location_comma = [l.strip() for l in location_comma]
        len_loc = len(location_comma)

        # replace common abbreviations
        for i in range(len_loc):
            if location_comma[i] == 'usa':
                location_comma[i] = 'united states'
            elif location_comma[i] == 'uk':
                location_comma[i] = 'united kingdom'

        if len_loc == 1:
            # only one entity in user_location, so check only single entity dicts
            valid_keys = ['country_mapping', 'state_mapping', 'city_mapping_overall']
            for key, value in args.items():
                if key in valid_keys:
                    subloc = location_comma[0]
                    if subloc in value.keys():
                        return value.get(subloc)

        elif len_loc == 2:
            # two entities in user_location, so check only two entity dicts
            valid_keys = ['state_country_mapping', 'district_country_mapping', 'district_state_mapping', 'city_country_mapping', 'city_state_mapping', 'city_admin2_mapping']
            for key, value in args.items():
                if key in valid_keys:
                    subloc = ' '.join(location_comma)
                    if subloc in value.keys():
                        return value.get(subloc)
            # if location not found, check sub-location (skip first entity)
            valid_keys = ['country_mapping', 'state_mapping', 'city_mapping_overall']
            for key, value in args.items():
                if key in valid_keys:
                    subloc = ' '.join(location_comma[1:])
                    if subloc in value.keys():
                        return value.get(subloc)

        elif len_loc == 3:
            # three entities in user_location, so check only three entity dicts
            valid_keys = ['district_state_country_mapping', 'city_state_country_mapping']
            for key, value in args.items():
                if key in valid_keys:
                    subloc = ' '.join(location_comma)
                    if subloc in value.keys():
                        return value.get(subloc)
            # if location not found, check sub-location (skip first entity)
            valid_keys = ['state_country_mapping', 'district_country_mapping', 'district_state_mapping', 'city_country_mapping', 'city_state_mapping', 'city_admin2_mapping']
            for key, value in args.items():
                if key in valid_keys:
                    subloc = ' '.join(location_comma[1:])
                    if subloc in value.keys():
                        return value.get(subloc)
            # if location not found, check sub-location (skip first two entities)
            valid_keys = ['country_mapping', 'state_mapping', 'city_mapping_overall']
            for key, value in args.items():
                if key in valid_keys:
                    subloc = ' '.join(location_comma[2:])
                    if subloc in value.keys():
                        return value.get(subloc)
        
        return None

    return F.udf(f)

def main():
    parser = argparse.ArgumentParser(description='Create tables for countries')
    parser.add_argument('table', help='Table name (a CSV file in HDFS) containing Twitter data.')
    parser.add_argument('output', help='output file')
    # parser.add_argument('map_dict', help='path for location mapping dictionaries')
    args = parser.parse_args()
    
    print("building spark app...")
    spark = SparkSession.builder \
        .appName('Geomap') \
        .getOrCreate()
    
    # First read input data, on which you have to perform the mapping
    print("reading table...")
    table = spark\
            .read\
            .option("header","true")\
            .option('escape','"')\
            .format("csv").load(args.table)
    table.printSchema() 

    maps = ['city_admin2_mapping','city_state_mapping','city_state_country_mapping','city_country_mapping','city_mapping_overall','district_state_mapping','district_state_country_mapping','district_country_mapping','state_mapping','state_country_mapping','country_mapping']     
    dict_list = []
    ## Read the csv files from hadoop to and create maps and then broadcats them,
    for i in range(0, len(maps)):
        # path = 'mod_overall_files/' + maps[i] + '.csv'
        path = 'mod_ssa_india_filters/' + maps[i] + '.csv'
        mp = spark\
          .read\
          .option('header',True)\
          .option('inferSchema',True)\
          .csv(path)
        print(maps[i])
        mp.show(4)
        dict_list.append({x['name']: x['mapped_data'] for x in mp.select('name', 'mapped_data').collect()})

    city_admin2_mapping = dict_list[0]
    city_state_mapping = dict_list[1]
    city_state_country_mapping = dict_list[2]
    city_country_mapping = dict_list[3]
    city_mapping_overall = dict_list[4]
    district_state_mapping =  dict_list[5]
    district_state_country_mapping = dict_list[6]
    district_country_mapping =dict_list[7] 
    state_mapping = dict_list[8]
    state_country_mapping = dict_list[9]
    country_mapping = dict_list[10]

    # #6. CITY ALIAS 2 CITY
    # mp = spark\
    #  .read\
    #  .option('header',True)\
    #  .option('inferSchema',True)\
    #  .csv("sa_filters/cityAlias2city.csv")
    # city_alias_map = {x['city_alias']: x['city'] for x in mp.select('city_alias','city').collect()}

    # # 7. CITY ALIAS clean 2 CITY
    # mp = spark\
    #  .read\
    #  .option('header',True)\
    #  .option('inferSchema',True)\
    #  .csv("sa_filters/cityAlias2city.csv")
    # city_alias_clean_map = {x['city alias clean']: x['city'] for x in mp.select('city alias clean', 'city').collect()}
 
    output_name = args.output

    new_map_dict = {
        'country_mapping': country_mapping,
        'state_mapping': state_mapping,
        'city_mapping_overall': city_mapping_overall,
        'state_country_mapping': state_country_mapping,
        'district_country_mapping': district_country_mapping,
        'district_state_mapping': district_state_mapping,
        'city_country_mapping': city_country_mapping,
        'city_state_mapping': city_state_mapping,
        'city_admin2_mapping': city_admin2_mapping,
        'district_state_country_mapping': district_state_country_mapping,
        'city_state_country_mapping': city_state_country_mapping,
    }

    # call the user_lookup to resolve the GEOLOCATION first followed by  user_location, here we pass on the respective dictionaries too.
    table_new = table.withColumn('state_abbreviation', user_lookup(new_map_dict)(F.col('user_location')))
    table_new = table_new.filter(table_new['state_abbreviation'].isNotNull())
    table_new = table_new.withColumn("user_city", F.split(F.col("state_abbreviation"), ",").getItem(0)).withColumn("user_admin2", F.split(F.col("state_abbreviation"), ",").getItem(1)).withColumn("user_admin1", F.split(F.col("state_abbreviation"), ",").getItem(2)).withColumn("user_country", F.split(F.col("state_abbreviation"), ",").getItem(3))
    table_new_filtered = table_new.filter(table_new["state_abbreviation"].isNotNull())
    table_new_filtered.write.csv(output_name, mode = 'append')
    print("mapped successfully...")

if __name__ == "__main__":
    main()
