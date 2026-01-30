#!/usr/bin/bash

REPORT="/home/project/weather_report.tsv"

year=($(cut -d " " -f 1 "${REPORT}"))
month=($(cut -d " " -f 2 "${REPORT}"))
day=($(cut -d " " -f 3 "${REPORT}"))
measured_temp=($(cut -d " " -f 4 "${REPORT}"))
forecasted_temp=($(cut -d " " -f 5 "${REPORT}"))

ACC_FILE="/home/project/historical_fc_accuracy.tsv"
echo -e "year\tmonth\tday\tmeasured_temp\tforecasted_temp\taccuracy\taccuracy_range" > "${ACC_FILE}"

n=$(wc -l < "${REPORT}")
for (( i=2; i<=$n; i++ )); do
  meas_temp="${measured_temp[$i]}"
  n_fc=$(($i - 1))
  fc_temp="${forecasted_temp[$n_fc]}"
  accuracy=$((${fc_temp} - ${meas_temp}))
  
  echo $accuracy
  case "${accuracy#-}" in
    0|1)
      acc_label='excellent'
      ;;
    2)
      acc_label='good'
      ;;
    3)
      acc_label='fair'
      ;;
    *)
      acc_label='poor'
      ;;
  esac

  line="${year[$i]}\t${month[$i]}\t${day[$i]}\t${meas_temp}\t${fc_temp}\t${accuracy}\t${acc_label}"
  echo -e "${line}" >> "${ACC_FILE}"

done