#!/usr/bin/python
import sys
for line in sys.stdin:
    line = line.strip()
    records = line.split(',', 1)
    print '{}\t{}'.format(records[0], records[1])
