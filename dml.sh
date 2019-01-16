#!/bin/bash

for i in 1 2 3 4 5
do
	java -jar dmlopt.jar 10.0.0.220 hieun golinux /var/lib/mysql /home/hieun/Desktop/data/tpcc1w sessWriter.txt > loglocal$i.txt
done
