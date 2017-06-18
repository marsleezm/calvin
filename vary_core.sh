#!/bin/bash

max_scs="1 10 50 100 200"
cores="1 2 4 8 12 16 20 24"
for max_sc in $max_scs
do
sed -i "s/max_sc =.*/max_sc = ${max_sc}/g" myconfig.conf
for C in $cores
do
	sed -i "s/num_threads =.*/num_threads = $C/g" myconfig.conf
	sleep 65 && pkill -f deployment &
	./bin/deployment/db 0 mn 0
	sed -e '/LATENCY/,$d' 0output.txt  > haha
	tail -n +2 haha > haha
	throughput=`awk -F ',' '{sum+=$1;line+=1}END{print sum/line}' haha` 
	cat $C ", " $throughput >> result_${max_sc}
done
done
