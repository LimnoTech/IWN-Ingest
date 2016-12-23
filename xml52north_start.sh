#!/bin/bash
wget $1 -O download.csv
~/anaconda3/bin/python xml52north.py download.csv $2
