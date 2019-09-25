#!/usr/bin/python
import random
import sys


# input comes from STDIN (standard input)
urls = ['aaa.com', 'bbb.com', 'ccc.it', 'ddd.fr', 'eee.ca', 'fff.edu', 'ggg.eu']
for x in range(300) :
    i = random.randint(1,7)
    t = random.randint(1,30)
    print '%s\t%s' % (urls[i-1], t)
