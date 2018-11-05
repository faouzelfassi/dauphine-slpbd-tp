#!/usr/bin/python
import sys
dict_customers = {}
dict_orders = {}
for line in sys.stdin:
    line = line.strip()
    records = line.split(',',1)
    print '{}\t{}'.format(records[0], records[1])
