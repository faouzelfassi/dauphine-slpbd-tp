#!/usr/bin/python
import sys

last_key = None
key = None
current_sum = 0
current_dict = {}

for line in sys.stdin:
    pair = line.split('\t')
    key = pair[0]
    total = int(pair[1])

    if last_key != key:
        if last_key:
            print '%s\t%s\t%s' % (last_key, current_sum, len(current_dict))
        last_key = key
        current_sum = total
        current_dict = {}
        current_dict[total] = 1
    else:
        current_sum += total
        if total not in current_dict:
            current_dict[total] = 1

if last_key == key:
    print '%s\t%s\t%s' % (key, current_sum, len(current_dict))
