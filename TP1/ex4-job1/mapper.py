#!/usr/bin/python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    record = line.split(',')
    if record[1][3:5] == '07':
        print '%s\t%s' % (record[2], '1')
