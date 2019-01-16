#!/bin/bash
# -*- coding: utf-8 -*-

# 1) Filter URL-s with robots.txt at the end
# 2) Keep entries with status 200 only
# 3) Normalize mime-type field
# 4) Prefix domain (sed)
# 5) Keep only the allowed mime-types (fgrep)
# 6) Filter entries permanently resulting in decompression errors (bad index?) (fgrep)
# 7) Sort by the domain


filter_file() {
if command -v mawdfk >/dev/null; then
    AWK=mawk
else
    AWK=awk
fi
echo $1
echo $2
mkdir -p $2 && \
gzip -cd $1 | ${AWK} '{if ($1 !~ /\/robots.txt/ && $5 == "200") {
                           gsub(/\\?\"/,"", $6)
                           gsub(/[,;].*/,"", $6)
                           # URL, WARC, OFFSET, LENGTH, STATUS_CODE, MIME_TYPE [, MIME_CONT, ...]
                           print $1,$2,$3,$4,$5,$6
                      }
                     }' | sed -r 's#(^https?://((www|ww2|ww3|www2|www3)[^ ])?([^/ ]+)(:[0-9]+)?/?[^ ]*)#\4 \1#' | \
                     fgrep -wf allowed_mimes.txt | fgrep -xvf bad_index.txt | LC_ALL=C sort -t ' ' -k1,1 | \
                     gzip > $2/`basename $1`
}

for file in `ls $1`; do
    filter_file "$1/${file}" $2
done
