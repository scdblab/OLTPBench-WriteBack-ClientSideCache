#/bin/bash

cache=$1
cli=$2
dir=$3
numar=$4
batch=$5
cs=$6
threads=$7
warehouses=$8
arsleep=$9

minw=${10}
maxw=${11}
warmup=${12}
rep=${13}
storesess=${14}
parallel=${15}
persistMode=${16}
manualwarmup=${17}

if [ -z "$storesess" ]; then
	storesess="false"
fi

echo "MinW="$minw
echo "MaxW="$maxw

cd /proj/BG/yaz/ycsbcache/oltpbench-hi

cmd="./oltpbenchmark -b tpcc -c config/tpcc_ngcache.xml --execute=true -s 1 -o ngcache --caches=$cs --ngcache=$cache --cachemode=back --numarworkers=$numar --batch=$batch --threads=$threads --scalefactor=$warehouses --arsleep=$arsleep --im=10000 --minw=$minw --maxw=$maxw --warmup=$warmup --replicas=$rep --storesess=$storesess --buffparallel=$parallel --manualwarmup=$manualwarmup --persistmode=$persistMode --polygraph=false > $dir/output-$cli-$minw.txt"
echo $cmd
eval $cmd
