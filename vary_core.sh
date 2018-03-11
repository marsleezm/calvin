#!/bin/bash

cores="10 20 30 40 50"
read_deps="0 1"
total_orders="1"
update_percent="10 50 90"
rp=0
for up in $update_percent
do
for rd in $read_deps
do
    for to in $total_orders
    do
        sed -i "s/total_order =.*/total_order = ${to}/g" myconfig.conf
        sed -i "s/track_read_dep =.*/track_read_dep = ${rd}/g" myconfig.conf
        for C in $cores
        do
            sed -i "s/num_threads =.*/num_threads = $C/g" myconfig.conf
            ./bin/deployment/db 0 tn 0
            sed -e '/LATENCY/,$d' 0output.txt  > haha
            tail -n +2 haha > haha2
            throughput=`awk -F ',' '{sum+=$1;line+=1}END{print sum/line}' haha2` 
            echo $C , $throughput >> bench_test/${up}/result_rd${rd}_to${to}_rp${rp}
        done
    done
done
done
