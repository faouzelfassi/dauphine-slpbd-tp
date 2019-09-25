#!/usr/bin/python

# this mapper simulates the combiner, but has the incovenient that the size of the dictionary may exceeds the available RAM
import sys

# input comes from STDIN (standard input)
# initialise the dictionary
dict = {}
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()

    for word in words:
        # clean up the word: pay attention -> the result may be empty
        word = ''.join(ch for ch in word if ch.isalnum())
        if word != '':
            if word in dict:
                dict[word] = dict[word] + 1
            else:
                dict[word] = 1
# observe that the pairs are emitted once the split has been consumed
for x in dict.keys():
    print '%s\t%s' % (x, dict[x])
