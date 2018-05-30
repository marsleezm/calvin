#!/bin/bash

cores="8"
max_scs="400"
warehouses="5"
sed -i "s/update_percent =.*/update_percent = 90/g" myconfig.conf
sed -i "s/read_batch =.*/read_batch = 0/g" myconfig.conf
sed -i "s/deterministic =.*/deterministic = 0/g" myconfig.conf
output="results/"`date +%Y%m%d_%H%M%S`
times="1 2 3"
mkdir $output
for n in $times
do
    for w in $warehouses
    do
        for sc in $max_scs
        do
            sed -i "s/max_sc =.*/max_sc = ${sc}/g" myconfig.conf
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
                    echo $C, ${sc}, ${w}, ${throughput}, $abort >> $output/sc_result 
                    time=`date +%Y%m%d_%H%M%S`
                    cp 0output.txt $output/${time}_0output.txt
            done
        done
    done
done
