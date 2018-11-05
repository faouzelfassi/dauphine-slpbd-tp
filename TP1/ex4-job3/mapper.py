#!/usr/bin/python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    record = line.split(',')
    print('{}\t{}'.format(record[0], record[1]))
