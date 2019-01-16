#!/bin/bash

warehouses=$1
memcache=$2
dbip=$3
dbuser=$4
dbpass=$5
min=$6
max=$7
dpw=$8
cpd=$9

bench="/proj/BG/yaz/ycsbcache/oltpbench-hi"

cd $bench
java -Xmx48G -cp $bench/lib/*:oltpbench.jar com.oltpbenchmark.benchmarks.tpcc.procedures.ReadOnly $warehouses $memcache $dbip $dbuser $dbpass $min $max $dpw $cpd true > /tmp/warmup.txt

