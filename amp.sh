#!/bin/bash

ths="20 50"
update_percents="90"
for p in $update_percents
do
    folder="bench_test/final_scale/${p}"
    sed -i "s/update_percent =.*/update_percent = ${p}/g" myconfig.conf
    for th in $ths
    do
        sed -i -e "s/num_threads .*/num_threads = ${th}/" myconfig.conf 
        output=$folder"/"$th
        mkdir $output
        mkdir $output/memory
        mkdir $output/concurrency
        mkdir $output/lock
        mkdir $output/hotspots

        amplxe-cl -collect memory-access -r $output/memory ./bin/deployment/db 0 tn 0 
        amplxe-cl -collect concurrency -r $output/concurrency ./bin/deployment/db 0 tn 0 
        amplxe-cl -collect locksandwaits -r $output/lock ./bin/deployment/db 0 tn 0 
        amplxe-cl -collect hotspots -r $output/hotspots ./bin/deployment/db 0 tn 0 

        cp myconfig.conf 0output.txt perfout $output
    done
done
