#!/usr/bin/python
import sys
dict_customers = {}
dict_orders = {}

# In order to ensure scalability, flush dict_customers to disk and add a reducer.
for line in sys.stdin:
    line = line.strip()
    records = line.split('\t')
    cust_data = records[1].split(',')
    if len(cust_data) == 2:
        cid = records[0]
        date, name = cust_data
        if name.startswith("O"):
            dict_customers[cid] = (date, name)
    else:
        cid, total = records
        if cid in dict_orders:
            dict_orders[cid].append(total)
        else:
            dict_orders[cid] = [total]

for cid_c in dict_customers.keys():
    if cid_c in dict_orders:
        for total in dict_orders[cid_c]:
            print '{}\t{}'.format(cid_c, total)
