#! /bin/bash

# Reads a file from stdin and splits the reviews into different files

f=1
while read line; do
    echo $line >> $f
    if [ "$line" == "" ]; then
        f=$((f+1))
    fi
done
