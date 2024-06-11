# ETL pipeline using Airflow, Spark, EMR, and Snowflake
This project will join the hourly_ridership(60M records) and wifi_location (300 records) datasets based on a column and calculate the total daily ridership broken down by three other columns.
The project will utilize Airflow to orchestrate and manage the data pipeline as it creates and terminates an EMR transient cluster. AWS EMR(spark) will be used for data transformation. The final dataset will be loaded into Snowflake.
https://medium.com/@abdoulkaled/building-a-batch-etl-pipeline-using-airflow-spark-emr-and-snowflake-05559bb9799a
