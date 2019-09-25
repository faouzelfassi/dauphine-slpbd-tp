#!/usr/bin/python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    pair = line.split('\t')
    url = pair[0]
    time = pair[1]
    print '{}\t{}'.format(url, time)


