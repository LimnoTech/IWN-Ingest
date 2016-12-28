#!/bin/bash
wget $1 -O download.csv
head -n 1 download.csv > dataincsv.tmp
tail -n 10 download.csv >> dataincsv.tmp
~/anaconda3/bin/python IWN-ingest.py dataincsv.tmp $2
