#! /usr/bin/python3

import sys
import re

# prepare files
reviewfile = open("reviews.txt", "w")
reviewfile = open("reviews.txt", "a")

reviewrow = "1,"
reviewcount = 1

for line in sys.stdin:
    if re.match(r'^$', line):
        # An empty line mark the end of the record
        # Write rows to files, eliminating last comma
        reviewfile.write(reviewrow[:-1] + "\n")
        reviewcount += 1
        reviewrow = str(reviewcount) + ","
    else:
        line = line.replace('"','&quot;').replace("\\", "\\\\")

        if line.startswith('review') or line.startswith('product'):
            m = re.match(r'[^/]*/([^:]*): (.*)$', line)
            field, value = m.group(1), m.group(2)
            if field in ('title','profileName', 
                    'summary', 'text'):
                # wrap in double quotes
                value = '"' + value + '"'
            reviewrow += value + ","

reviewfile.close()
