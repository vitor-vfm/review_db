#! /usr/bin/python3

import sys
import re

# prepare files

# the files are first opened in write mode
# to clobber any files with the same name
reviewfile = open("reviews.txt", "w")
ptermsfile = open("pterms.txt", "w")
rtermsfile = open("rterms.txt", "w")

reviewfile = open("reviews.txt", "a")
ptermsfile = open("pterms.txt", "a")
rtermsfile = open("rterms.txt", "a")

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

        # process pterms
        if line.startswith("product/title: "):
            cleanLine = line.replace("product/title: ", "").lower()
            pterms = re.findall(r'\w{3,}', cleanLine)
            for pterm in pterms:
                ptermsfile.write("%s,%d\n" % (pterm, reviewcount))

        # process rterms
        if line.startswith("review/text: ") or line.startswith("review/summary: "):
            cleanLine = re.sub("review/[a-z]+: ", "", line).lower()
            rterms = re.findall(r'\w{3,}', cleanLine)
            for rterm in rterms:
                rtermsfile.write("%s,%d\n" % (rterm, reviewcount))

reviewfile.close()
ptermsfile.close()
rtermsfile.close()
