#!/usr/bin/bash

echo "Please provide two integers:"
echo -n "Integer 1 = "
read int1
echo -n "Integer 2 = "
read int2

sum_ints=$(($int1+$int2))
echo "The sum of the two integers is equal to $sum_ints"
prod_ints=$(($int1*$int2))
echo "The product of the two integers is equal to $prod_ints"

if [ "$sum_ints" -gt "$prod_ints" ]
then
    echo "The sum is greater than the product."
elif [ "$sum_ints" -eq "$prod_ints" ]
then
    echo "The sum is equal to the product."
else
    echo "The sum is smaller than the product."
fi