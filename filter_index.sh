#!/bin/bash

if command -v mawdfk >/dev/null; then
    AWK=mawk
else
    AWK=awk
fi
mkdir -p $2 && \
gzip -cd $1 | ${AWK} '{if ($1 !~ /\/robots.txt/ && $5 == "200") {
                           gsub(/\\?\"/,"", $6)
                           gsub(/[,;].*/,"", $6)
                           # URL, WARC, OFFSET, LENGTH, STATUS_CODE, MIME_TYPE [, MIME_CONT, ...]
                           print $1,$2,$3,$4,$5,$6
                      }
                     }' | sed -r 's#(^https?://((www|ww2|ww3|www2|www3)[^ ])?([^/ ]+)(:[0-9]+)?/?[^ ]*)#\4 \1#' | \
                     fgrep -wf allowed_mimes.txt | LC_ALL=C sort -t ' ' -k1,1 | gzip > $2/`basename $1`
