#!/usr/bin/bash

ACC_REPORT="/home/project/synthetic_historical_fc_accuracy.tsv"
accuracy=($(cut -f 6 "${ACC_REPORT}"))

# Determine the number of lines of data that can be used (in case fewer than 7 days are available)
n_lines=$(wc -l < "${ACC_REPORT}")
if [[ ${n_lines} < 8 ]]; then
  n_lines=$(( ${n_lines} - 1))
  echo "Warning: the file contains only ${n_lines} lines of data."
else
  n_lines=7
fi

init_abs_acc="${accuracy[-${n_lines}]#-}"
min_acc="${init_abs_acc}"
max_acc="${init_abs_acc}"

n_start=$((${n_lines} - 1))
for acc in "${accuracy[@]: -n_start}"; do
  abs_acc="${acc#-}"
  if [[ "${abs_acc}" > "${max_acc}" ]]; then
    max_acc="${abs_acc}"
  fi
  if [[ "${abs_acc}" < "${min_acc}" ]]; then
    min_acc="${abs_acc}"
  fi
done

echo "Min absolute error = ${min_acc}"
echo "Max absolute error = ${max_acc}"
