#!/bin/bash
wget $1 -O download.csv
~/anaconda3/bin/python IWN-ingest.py download.csv $2
