#!/usr/bin/bash

csv_file="./arrays_table.csv"

col0=($(cut -d "," -f 1 $csv_file))
col1=($(cut -d "," -f 2 $csv_file))
col2=($(cut -d "," -f 3 $csv_file))

col3=("column_3")

N=$(($(wc -l < $csv_file) - 1))
echo "There are $N lines in the file"
for (( i=1; i<=$N; i++ )) ; do
    diff=$((${col2[$i]}-${col1[$i]}))
    col3+=($diff)
done

for item in ${col3[@]}; do
    echo $item
done

> column3.csv
for (( i=0; i<=$N; i++ )) ; do
    echo "${col3[$i]}" >> column3.csv
done

paste -d "," $csv_file column3.csv > report.csv