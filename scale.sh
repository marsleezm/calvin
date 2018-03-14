#!/bin/bash

folder="newtest/50w-noconf/"
ths="8 16 32 50"
for th in $ths
do
    sed -i -e "s/num_threads .*/num_threads = ${th}/" myconfig.conf 
    #./bin/deployment/db 0 tn 0
    perf stat -d -o perfout ./bin/deployment/db 0 tn 0 > perfout 
    #./bin/deployment/db 0 mn 0
    output=$folder"/"$th
    mkdir $output
    cp myconfig.conf 0output.txt perfout $output
done
