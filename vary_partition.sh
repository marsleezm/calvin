#!/bin/bash

nw="200"
output="results/"`date +%Y%m%d_%H%M%S`
times="1 2 3"
mkdir $output
partitions="50 40 25 20"
sed -i "s/update_percent =.*/update_percent = 90/g" myconfig.conf
for n in $times
do
    for p in $partitions 
    do
        rm *output.txt
        head -${p} template.conf > dist-deploy.conf
        cat dist-deploy.conf
        sed -i "s/num_warehouses =.*/num_warehouses = $((nw/p))/g" myconfig.conf
        sed -i "s/sleep =.*/sleep = 20/g" myconfig.conf
        timeout 150 ./bin/deployment/cluster -c dist-deploy.conf -p ./src/deployment/portfile -d bin/deployment/db tn 0

        totalth=0
        for f in `ls *output.txt`
        do
            sed -e '/LATENCY/,$d' $f  > haha
            tail -n +2 haha > haha2
            throughput=`awk -F ',' '{sum+=$1;line+=1}END{print sum/line}' haha2` 
            abort=`awk -F ',' '{abort+=$2;line+=1}END{print abort/line}' haha2` 
            totalth=`echo ${totalth} + ${throughput} | bc -l`
        done
        #echo $C, ${sc}, ${w}, $throughput >> nosc_result 
        echo $p, "0, 0," ${totalth}, $abort >> $output/sc_result 
        time=`date +%Y%m%d_%H%M%S`
        #cp 0output.txt $output/${time}_0output.txt
        mkdir $output/$time
        cp *output.txt $output/$time/
    done
done
