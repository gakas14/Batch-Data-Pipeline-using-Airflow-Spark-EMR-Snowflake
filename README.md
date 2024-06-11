# ETL pipeline using Airflow, Spark, EMR, and Snowflake
### This project will join the hourly_ridership(60M records) and wifi_location (300 records) datasets based on a column and calculate the total daily ridership broken down by three other columns.
hourly_ridership: This dataset provides subway ridership estimates hourly by subway station complex and class of fare payment. Link.

wifi_location: The MTA (Metropolitan Transportation Authority) contracted with Transit Wireless to provide all subway stations with Wi-Fi access and cell service. This dataset is a snapshot of the stations where Wi-Fi was available in part of 2015 and 2016. Link.

###  The project will utilize Airflow to orchestrate and manage the data pipeline as it creates and terminates an EMR transient cluster. Apache Spark will transform data, and the final dataset will be loaded into Snowflake.

#### read more:  https://abdoulkaled.medium.com/building-a-batch-etl-pipeline-using-airflow-spark-emr-and-snowflake-05559bb9799a


![batch ETL pipeline using Airflow, Spark, EMR, and Snowflake](https://github.com/gakas14/Batch-Data-Pipeline-using-Airflow-Spark-EMR-Snowflake/assets/74584964/37356d36-4c76-44a5-84be-20a5432a7385)
