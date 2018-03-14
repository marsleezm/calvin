#!/bin/bash

folder=$1
for f in `ls -d -tr $folder/*`
do
    lat=`tail -1 $f/0output.txt`
    th=`head -n -2 $f/0output.txt | tail -n +2 | awk -F , '{th+=$1;ab+=$2;cnt+=1}END{print th/cnt ", " ab/cnt}'`
    ins=`cat $f/perfout | grep "insns per cycle" | awk -F ' ' '{print $4}'`
    #head -n -2 $f/0output.txt
    echo ${f##*/}": " $lat", "$th", "$ins
    #exit
done
