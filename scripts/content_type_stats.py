#!/usr/bin/env python3
# -*- coding: utf-8 -*

"""
Collects content-type data from the corpus.
Content-type information appears in multiple sections of the metadata,
and they are not consistent. We collect:
* the mime-type from the <doc> tag
* the warc identified content type from the request header
* the content type from the response header
* the extension of the attachment file, if any.
Writes the output as a tsv.gz.
"""


from argparse import ArgumentParser
import logging
from multiprocessing import Pool
from pathlib import Path
import re

from cc_corpus.corpus import parse_file
from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory that contains the corpus')
    parser.add_argument('--output-file', '-o', type=Path, required=True,
                        help='the file where results are written. It should '
                             'be a .tsv.gz file')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1).')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    return args


def process_file(input_file: Path) -> str:
    """
    Processes a file, collecting mime-type data from the headers of the docs
    contained in that file.
    It returns a string which is a tsv: each document is a single line and the
    values are separated by tabs within the line.
    """
    results = ""
    matcher_ct = re.compile(r'Content-Type: ([\w/]+)', re.I)
    matcher_wi = re.compile(r'WARC-IDentified-Payload-Type: ([\w/]+)', re.I)
    matcher_cd = re.compile(r'(Content-Disposition: )([^\n"]*)"([^\n^"]*)"', re.I)
    for doc in parse_file(input_file):
        type_from_doctag = type_from_warc_id = type_from_response = '-'
        attachment_ext = '-'
        # Get the type from the metadata of the doc tag:
        if doc.attrs['mime-type']:
            type_from_doctag = doc.attrs['mime-type']
        # Get the type from the warc request header:
        if (match_warcid := matcher_wi.search(doc.meta['request'])):
            type_from_warc_id = match_warcid.group(1)
        # Get the type from the response header:
        if (match_resp := matcher_ct.search(doc.meta['response'])):
            type_from_response = match_resp.group(1)
        # Get the info about the attached file, if any:
        match_cd = matcher_cd.search(doc.meta['response'])
        if match_cd:
            attached_file = match_cd.group(3)
            if '.' in attached_file:
                attachment_ext = attached_file.split('.')[-1]
            else:
                attachment_ext = 'no_extension'
        # Process the results for this doc:
        line = '\t'.join((str(input_file), type_from_doctag,
                          type_from_warc_id, type_from_response,
                          attachment_ext,))
        results += line + '\n'
    return results


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    args.output_file.parent.mkdir(parents=True, exist_ok=True)
    with openall(args.output_file, "wt", encoding="utf-8") as f:
        with Pool(args.processes) as p:
            for stats in p.imap_unordered(process_file, args.input_dir.iterdir()):
                print(stats, file=f, end='')
        p.close()
        p.join()


if __name__ == '__main__':
    main()
