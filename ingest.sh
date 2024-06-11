wget -O - https://data.ny.gov/resource/pwa9-tmie.csv | aws s3 cp - s3://ak-emr-batch-project/input_files/wifi_locations.csv
wget -O - https://data.ny.gov/resource/wujg-7c2s.csv?$limit=6000000 | aws s3 cp - s3://ak-emr-batch-project/input_files/hourly_ridership_full.csv
