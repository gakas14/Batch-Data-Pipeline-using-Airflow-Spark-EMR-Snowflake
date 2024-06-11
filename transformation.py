import sys


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *


spark = SparkSession \
        .builder \
        .appName("airflow_with_emr") \
        .getOrCreate()
        


def main():
    df_wifi_locations1="s3://gakas-mta-etl-job/input_files/wifi_locations.csv";
    df_wifi_locations2="s3://gakas-mta-etl-job/input_files/hourly_ridership_full.csv"

    
    # df_hourly_ridership_c = spark.read.csv('hourly_ridership_full.csv', header=True, escape="\"")
    # df_hourly_ridership1 = df_hourly_ridership_c[['transit_timestamp', 'station_complex', 'ridership']]

    # df_wifi_locations1 = spark.read.csv('wifi_locations.csv', header=True, escape="\"")
    # df_wifi_locations2 = df_wifi_locations1[['station_complex', 'historical', 'at_t', 'sprint', 't_mobile', 'verizon']]



    df_wifi_locations1 = spark.read.format("csv").option("inferSchema","true").load(df_wifi_locations1).toDF('station_complex', 'historical', 'at_t', 'sprint', 't_mobile', 'verizon');
    df_hourly_ridership1 = spark.read.format("csv").option("inferSchema","true").load(df_wifi_locations2).toDF('transit_timestamp', 'station_complex', 'ridership');
    
    # ms=iris.groupBy("CLASS_NAME").count()

    
    df_spark = df_wifi_locations1.withColumn('provider_available',
                                             f.when(f.col('at_t') == 'Yes', "Yes").when(f.col('sprint') == 'Yes',
                                                                                        "Yes").when(
                                                 f.col('t_mobile') == 'Yes', "Yes").when(f.col('verizon') == 'Yes',
                                                                                         "Yes").otherwise("No"))
    clean_df_wifi_locations = df_spark[['station_complex', 'historical', 'provider_available']]

    df_hourly_ridership1 = df_hourly_ridership1.withColumn("ridership",
                                                           df_hourly_ridership1["ridership"].cast('float'))

    clean_df_hourly_ridership = df_hourly_ridership1.withColumn("transit_timestamp",
                                                                df_hourly_ridership1["transit_timestamp"].cast('date'))

    clean_df_wifi_locations.createOrReplaceTempView("temp_table_wifi")
    clean_df_hourly_ridership.createOrReplaceTempView("temp_table_hourly_ridership")

    hourly_ridership1 = spark.sql(
        "select transit_timestamp, historical, provider_available,ridership from temp_table_hourly_ridership thr, temp_table_wifi tw  where tw.station_complex = thr.station_complex")
    hourly_ridership1.createOrReplaceTempView("table_hourly_ridership1")
    #spark.sql("select * from table_hourly_ridership1 ").show(5)

    group_table = spark.sql(
        "select transit_timestamp,historical,provider_available, sum(ridership) as total_daily_readership from table_hourly_ridership1 group by transit_timestamp,historical,provider_available")
    group_table.createOrReplaceTempView("final_table")
    #spark.sql("select * from final_table order by transit_timestamp").show(10)



    
    
    group_table.coalesce(1).write.format("parquet").mode('overwrite').save("s3://gakas-mta-etl-job/input_files/")
   

main()


