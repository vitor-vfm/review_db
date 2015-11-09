# /bin/bash

# create reviews.txt Hash index
./break.pl <reviews.txt | db_load -T -t hash rw.idx;

# create Btree indices for the other files
fis='pterms.txt rterms.txt scores.txt'
for f in $fis; do
    sort -u <$f | ./break.pl | db_load -T -t btree ${f:0:2}.idx
done
