#!/usr/bin/python
import sys

current_url = None
current_count = 0
current_sum =0
url = None

# input comes from STDIN
for line in sys.stdin:
    # parse the input we got from mapper.py
    triple = line.split('\t')
    url = triple[0]
    time_sum = triple[1]
    count = triple[2]
    # here count can be >1 since  local aggregation has been performed
    # convert time (currently a string) to int
    try:
        time_sum = int(time_sum)
    except ValueError:
        # time was not a number, so silently
        # ignore/discard this line
        continue
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        continue
    if current_url == url:
        current_sum += time_sum
        current_count += count
    else:
        if current_url:
            # write result to STDOUT
            print '%s\t%s' % (current_url, current_sum/float(current_count))
        current_sum = time_sum
        current_count = count
        current_url = url

# do not forget to output the last url if needed!
if current_url == url:
    print '%s\t%s' % (current_url, current_sum/float(current_count))
