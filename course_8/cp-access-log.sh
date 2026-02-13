#! /usr/bin/bash

# Download the data file
wget 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz'

# Unzip the file
gunzip 'web-server-access-log.txt.gz'

# Extract the first four columns in the file and write them to a txt file
cut -d '#' -f 1-4 'web-server-access-log.txt' > 'extracted_data.txt'

# Transform the extracted data to turn it into a csv file
tr "#" "," < extracted_data.txt > transformed_data.csv

# Load the data from the csv file into the database
echo "\c template1;\COPY access_log FROM 'transformed_data.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=postgres -W

# Print out the first 5 rows to check if the data was loaded successfully
echo "SELECT * FROM access_log LIMIT 5;" | psql --username=postgres --host=postgres template1 -W