#!/usr/bin/python
import sys

current_key = None
key = None
# input comes from STDIN
for line in sys.stdin:
    # parse the input we got from mapper.py
    pair = line.split('\t')
    key = pair[0]

    if current_key != key:
        print '%s\t%s' % (key, '')
        current_key = key
