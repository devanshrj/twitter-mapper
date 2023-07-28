import sys
import unicodedata
import argparse
import os
import re
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType
spark = SparkSession.builder \
        .appName('row_counter') \
        .getOrCreate()

def user_lookup(*args):
    ## here args is the dictionaries mapping the location to whatever info you can get in terms of city, district, state, country.
    ## we start looking up from the most granular level, if we have something like Mumbai, India -> we would like to resolve it at the city level and not at the country level becuase from city we get all teh info about city, district, state, country. from country we only know which country.
    ## Order followed in resolving

    # city_district_map --> gives more info on resolving as compared to just city 
    # city_state_map   
    # city_state_country_map       
    # city_country_map
    # city_map
    # district_state_map
    # district_state_country_map 
    # district_country_map
    # state_country_map
    # country_map  --> useful for country level analysis.
    # city_alias_map  --> Bombay -> Mumbai -> lod	okup in city_map. Design choice to put in end as the cases are rare.
    # city_alias_clean_map

    def f(user_location):

        location = str(user_location)
        location = re.sub('\W+',' ',location)
        location_lower = location.lower()
        location_lower = location_lower.replace("south africa", "South Africa")
        location_lower = location_lower.replace("nigeria", "Nigeria")
        location_lower = location_lower.replace("kenya", "Kenya")
        location_lower = location_lower.replace("ghana", "Ghana")
        location_lower = location_lower.replace("uganda", "Uganda")
        location_lower = location_lower.split()
        location = location.split()
        le = len(location)
        for value in args:
            ### If the given location entered by the user is a city alias name, then
            ## Get the city corresponding to the alias (eg: Bombay is mapped to Mumbai)
            ## once you get the actual city lookuo in the city_map. if it is a fampus city in world, it will be present in the city_map. 
            ## Mumbai gets resolved to Mumbai in India and not Mumbai in US.
            if value == 'city_alias_map' or value == 'city_alias_clean_map':
                location = str(x)
                location = re.sub('\W+',' ',location)
                location = location.split()
                le = len(location)
                ct = ''
                for i in range(le, 0, -1):
                    for j in range(0,i):
                        subloc = ' '.join(location[j:i])
                        if subloc in value.keys():
                            ct =  value.get(subloc)

                if ct != '':
                    city = re.sub('\W+',' ', ct)
                    city = city.split()
                    le = len(city)
                    for i in range(le, 0, -1):
                        for j in range(0,i):
                            subloc = ' '.join(city[j:i])
                            if subloc in city_map.keys():
                                return city_map.get(subloc)
            ### for others, we simply iterate over all other combinations, and check for lower and upper case both.
            else:
                for case in range(0,2):
                    if case == 0:
                        for i in range(le, 0, -1):
                            for j in range(0,i):
                                subloc = ' '.join(location[j:i])
                                if subloc in value.keys():
                                    return value.get(subloc)
                    if case == 1:
                        for i in range(le, 0, -1):
                            for j in range(0,i):
                                subloc = ' '.join(location_lower[j:i])
                                if subloc in value.keys():
                                    return value.get(subloc)


        return None
        return " , , , "

    return F.udf(f)

def main():
    parser = argparse.ArgumentParser(description='Create tables for countries')
    parser.add_argument(
       'table', help='Table name (a CSV file in HDFS) containing Twitter data.')
    parser.add_argument(
        'output', help='output file')
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
        path = 'africa_filters/' + maps[i] + '.csv'
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



    #6. CITY ALIAS 2 CITY
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("africa_filters/cityAlias2city.csv")
    city_alias_map = {x['city_alias']: x['city'] for x in mp.select('city_alias','city').collect()}

    # 7. CITY ALIAS clean 2 CITY
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("africa_filters/cityAlias2city.csv")
    city_alias_clean_map = {x['city alias clean']: x['city'] for x in mp.select('city alias clean', 'city').collect()}
 
    output_name = args.output

    ## call the user_lookup to resolve the GEOLOCATION first followed by  user_location, here we pass on the respective dictionaries too.
    table_new = table.withColumn('state_abbreviation', user_lookup(city_admin2_mapping, city_state_mapping, city_state_country_mapping, city_country_mapping, city_mapping_overall, district_state_mapping, district_state_country_mapping, district_country_mapping, state_mapping, state_country_mapping, country_mapping, city_alias_map, city_alias_clean_map)(F.col('user_location')))

    table_new = table_new.filter(table_new['state_abbreviation'].isNotNull())
    table_new = table_new.withColumn("user_city", F.split(F.col("state_abbreviation"), ",").getItem(0)).withColumn("user_admin2", F.split(F.col("state_abbreviation"), ",").getItem(1)).withColumn("user_admin1", F.split(F.col("state_abbreviation"), ",").getItem(2)).withColumn("user_country", F.split(F.col("state_abbreviation"), ",").getItem(3))
    table_new_filtered = table_new.filter(table_new["state_abbreviation"].isNotNull())
    table_new_filtered.write.csv(output_name, mode = 'append')

if __name__ == "__main__":
    main()
