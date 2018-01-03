# CommonCrawl Downloader

Simple Python command line tools for retrieving a list of urls and specific files in bulk

## Overview

With this tool you can query the CommonCrawl index and download pages to the local machine anonymously.
It optionally uses [JusText](http://corpus.tools/wiki/Justext) to remove boilerplate content and only keep real text.
Using this tool one can create raw text corpora from web (CommonCrawl) easily.

This repository also contains a massively fixed and overhauled version of [cdx-index-client](https://github.com/ikreymer/cdx-index-client/tree/1ae1301ae4fb8416f10bed97d4c7c96ba5ab4dc7).

## Install
    
    # Python 3.x required
    pip3 install -r requirements.txt
    # Optional dependencies (for faster processing)
    [mawk](http://invisible-island.net/mawk/mawk.html) or any AWK implementation
    GNU parallel

## Examples
    # Dowload index for specfic condition
    ./get_indexfiles.sh CONDITION OUTPUT_DIR LOG_FILE MAX_FULL_RETRY
    # e.g. ./get_indexfiles.sh '*.hu' cc_index get_index.log 10
    # Filter index
    ./filter_index.sh cc_index cc_index_filtered
    # Download pages for index to pages dir
    ./download_pages.py -b 'Hungarian' -o out -i 'cc_index_filtered/*.gz' --preprocessed
    # or (with optionally GNU parallel)
    zgrep "." cc_index_filtered/*.gz | sed 's/:/ /' | ./download_pages.py -b 'Hungarian' -o out -s  --preprocessed
    # or grouped by files
    zcat "$1" | awk -v prefix="$1" '{print prefix,$0}' | ./download_pages.py -b 'Hungarian' -o out -s  --preprocessed

## Licence

GNU LGPL 3.0 or any later version
