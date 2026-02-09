#!/bin/sh

dbname=$(
    mysql -Ne "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '$1';"
)
if [[ -z "${dbname}" ]]; then
  echo "Database \"${1}\" does not exist. No backup will be performed."
  exit
else
  echo "Database \"${dbname}\" found"
fi


if [[ ! -d "${2}" ]]; then
  echo "Directory \"${2}\" not found. It will be created."
  mkdir "${2}"
else
  echo "Directory \"${2}\" exists."
fi

sqlfile="${2}"/"${dbname}"_$(date +%d-%m-%Y_%H-%M-%S).sql

# Create a backup
if mysqldump  "${dbname}" > "${sqlfile}" ; then
   echo 'Sql dump created'
else
   echo 'pg_dump returned non-zero code. No backup was created!' 
   exit
fi