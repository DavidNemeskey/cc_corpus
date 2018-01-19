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

If you use this program please cite the following paper:

Indig Balázs. _Közös crawlnak is egy korpusz a vége -- Korpuszépítés a CommonCrawl .hu domainjából_ XIV. Magyar Számítógépes Nyelvészeti Konferencia (MSZNY 2018). 125--134. Szeged. 2018.

    @inproceedings{indig_2018a,
        title = {K{\"o}z{\"o}s crawlnak is egy korpusz a v{\'e}ge -- Korpusz{\'e}p{\'i}t{\'e}s a CommonCrawl .hu domainj{\'a}b{\'o}l},
        booktitle = {XIV. Magyar Sz{\'a}m{\'i}t{\'o}g{\'e}pes Nyelv{\'e}szeti Konferencia (MSZNY 2018)},
        year = {2018},
        pages = {125{\textendash}134},
        publisher={Szegedi Tudom{\'a}nyegyetem Informatikai Tansz{\'e}kcsoport},
        organization = {Szegedi Tudom{\'a}nyegyetem Informatikai Int{\'e}zet},
        address = {Szeged},
        author = {Indig, Bal{\'a}zs},
        editor = {Vincze, Veronika}
    }