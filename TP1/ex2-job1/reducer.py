#!/usr/bin/python
import sys

current_url = None
current_count = 0
current_sum = 0
url = None

# input comes from STDIN
for line in sys.stdin:
    # parse the input we got from mapper.py
    url, time = line.split('\t')
    # here count can be >1 since the mapper has performed local aggregation
    # convert time (currently a string) to int
    try:
        time = int(time)
    except ValueError:
        # time was not a number, so silently
        # ignore/discard this line
        continue
    if current_url == url:
        current_sum += time
        current_count += 1
    else:
        if current_url:
            # write result to STDOUT
            print '%s\t%s' % (current_url, current_sum/float(current_count))
        current_sum = time
        current_count = 1
        current_url = url

# do not forget to output the last url if needed!
if current_url == url:
    print '%s\t%s' % (current_url, current_sum/float(current_count))
