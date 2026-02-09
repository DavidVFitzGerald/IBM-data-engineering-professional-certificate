#!usr/bin/bash

dbname="${1}"
backup_fp="${2}"

if [[ ! -f "${backup_fp}" ]]; then
    echo "Backup filepath does not exist. Database will not be restored."
    exit 1
fi

echo "Backup file exists."

db_exists=$(mysql -Nse "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${dbname}';")

if [[ -z "${db_exists}" ]]; then
    echo "Database \"${dbname}\" does not exist. Creating it now."
    mysql -e "CREATE DATABASE \`${dbname}\`;"
else
    echo "Database \"${dbname}\" already exists."
fi

echo "Restoring database \"${dbname}\"..."
mysql "${dbname}" < "${backup_fp}"
echo "Restore complete."
