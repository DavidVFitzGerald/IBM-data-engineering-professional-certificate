#!/usr/bin/bash

REPORT="/home/project/weather_report.txt"
HEADER="year,month,day,measured_temp,forecasted_temp"
if [[ ! -f "${REPORT}" ]]; then
    echo "${HEADER}" > "${REPORT}"
fi

region='Africa'
city='Casablanca'

raw_data=$(curl wttr.in/${city}?T)

temps=(
    $(echo "$raw_data" \
  | grep -oE '[+-]?[0-9]+(\([+-]?[0-9]+\))? °C' \
  | sed -E 's/\([+-]?[0-9]+\)//; s/ °C$//')
)

measured_temp=${temps[0]}
forecasted_temp=${temps[6]}

ymd=$(CRON_TZ="${region}/${city}" date "+%Y,%m,%d")
echo "${ymd},${measured_temp},${forecasted_temp}" >> "${REPORT}"
