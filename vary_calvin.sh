#!/bin/bash

cores="1 3 4 7 9 19 29 39 49"
#warehouses="50 100 200"
warehouses="5"
sed -i "s/update_percent =.*/update_percent = 90/g" myconfig.conf
output="results/"`date +%Y%m%d_%H%M%S`
mkdir $output
times="1 2 3"
for t in $times
do
    for w in $warehouses
    do
        sed -i "s/num_warehouses =.*/num_warehouses = ${w}/g" myconfig.conf
        for C in $cores
        do
            sed -i "s/num_threads =.*/num_threads = $C/g" myconfig.conf
            ./bin/deployment/db 0 tn 0
            sed -e '/LATENCY/,$d' 0output.txt  > haha
            tail -n +2 haha > haha2
            sed -i '/0, 0/d' haha2
            throughput=`awk -F ',' '{sum+=$1;line+=1}END{print sum/line}' haha2` 
            abort=`awk -F ',' '{abort+=$2;line+=1}END{print abort/line}' haha2` 
            #echo $C, ${sc}, ${w}, $throughput >> nosc_result 
            echo $C, ${sc}, ${w}, ${throughput}, $abort >> $output/calvin_result 
        done
    done
done
