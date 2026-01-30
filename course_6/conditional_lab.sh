#!/usr/bin/bash

echo "Are you human? "
echo -n "Enter \"y\" for yes, \"n\" for no: "
read answer

if [ $answer = "y" ]
then
    echo "Your answer is correct"
elif [ $answer = "n" ]
then
    echo "Your answer is wrong"
else
   echo -e "Your response must be either 'y' or 'n'.\nPlease re-run the script to try again."
fi
