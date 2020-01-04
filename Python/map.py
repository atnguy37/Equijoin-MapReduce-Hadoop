#!/usr/bin/env python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split(', ')
    try:
        float(words[2])
        print '%s\trating\t%s' % (words[1],line)
    except:
        print '%s\tmovie\t%s' % (words[0],line)
    