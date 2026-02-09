#!/bin/sh
 
DATABASE=sakila

mysql -Nse 'show tables' sakila | \
    while read table; do mysql \
    -e "use sakila;SET FOREIGN_KEY_CHECKS=0;truncate table $table;SET FOREIGN_KEY_CHECKS=1;" ;done