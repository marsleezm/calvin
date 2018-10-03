#!/bin/bash

cores="1 4 5 8 10 20 30 40 50"
#cores="1"
read_deps="1"
total_orders="1"
update_percent="90"
read_phases="0"
for up in $update_percent
do
for rd in $read_deps
do
    for to in $total_orders
    do
        for rp in $read_phases
        do
            sed -i "s/total_order =.*/total_order = ${to}/g" myconfig.conf
            sed -i "s/track_read_dep =.*/track_read_dep = ${rd}/g" myconfig.conf
            sed -i "s/read_batch =.*/read_batch = ${rp}/g" myconfig.conf
            sed -i "s/update_percent =.*/update_percent = ${up}/g" myconfig.conf
            for C in $cores
            do
                sed -i "s/num_threads =.*/num_threads = $C/g" myconfig.conf
                sed -i "s/num_warehouses =.*/num_warehouses = $C/g" myconfig.conf
                ./bin/deployment/db 0 tn 0
                sed -e '/LATENCY/,$d' 0output.txt  > haha
                tail -n +4 haha > haha2
                throughput=`awk -F ',' '{sum+=$1;line+=1}END{print sum/line}' haha2` 
                abort=`awk -F ',' '{sum+=$2;line+=1}END{print sum/line}' haha2` 
                echo $C , $throughput, $abort >> bench_test/${up}/locon_remove_read_result_rd${rd}_to${to}_rp${rp}
            done
        done
    done
done
done
