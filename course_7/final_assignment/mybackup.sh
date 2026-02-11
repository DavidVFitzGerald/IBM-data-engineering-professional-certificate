#!/usr/bin/bash

backup_filename='all-databases-backup.sql'

mysqldump --all-databases --user=root -p > "${backup_filename}"

date_dir=$(date "+%Y%m%d")
dump_dir="/tmp/mysqldumps/${date_dir}"
mkdir -p "${dump_dir}"

mv "${backup_filename}" "${dump_dir}"



