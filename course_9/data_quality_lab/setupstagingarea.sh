echo "Creating the database"

createdb -h postgres -U postgres -p 5432 -W billingDW

echo "Downloading the data files"
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Setting%20up%20a%20staging%20area/billing-datawarehouse.tgz

echo "Extracting files"
tar -xvzf billing-datawarehouse.tgz

echo "Creating schema"

psql  -h postgres -U postgres -p 5432 billingDW -W < star-schema.sql

echo "Loading data"

psql  -h postgres -U postgres -p 5432 billingDW -W < DimCustomer.sql

psql  -h postgres -U postgres -p 5432 billingDW -W < DimMonth.sql

psql  -h postgres -U postgres -p 5432 billingDW -W < FactBilling.sql

echo "Finished loading data"

echo "Verifying data"

psql  -h postgres -U postgres -p 5432 billingDW -W < verify.sql

echo "Successfully setup the staging area"