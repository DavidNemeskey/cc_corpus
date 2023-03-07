#!/usr/bin/env python3
# -*- coding: utf-8 -*

"""
Collects content-type data from the corpus.
Operates with gz files.
Writes the output as a tsv.gz.
"""


from argparse import ArgumentParser
from functools import partial
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
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the directory where results are written')
    parser.add_argument('--from-dir', '-f',
                        help='the earlies batch to work on.')
    parser.add_argument('--upto-dir', '-u',
                        help='the latest batch to work on.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1).')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    if not args.output_dir.is_dir():
        parser.error('The directory for the output must exist.')
    return args


def fetch_directories(root_dir: Path, from_dir: Path, upto_dir: Path
                      ) -> [Path]:
    """
    Collects the batches within the rootdir, that are not earlier than
    from_dir and not later than upto_dir.
    """
    list_of_dirs = [dir for dir in sorted(root_dir.iterdir())]
    if upto_dir:
        try:
            upto_i = [dir.name for dir in list_of_dirs].index(upto_dir)
        except ValueError:
            raise ValueError('The upto-dir must be a valid directory')
        list_of_dirs = list_of_dirs[: upto_i + 1]
    if from_dir:
        try:
            from_i = [dir.name for dir in list_of_dirs].index(from_dir)
        except ValueError:
            raise ValueError('The from-dir is not a valid batch')
        list_of_dirs = list_of_dirs[from_i:]
    return list_of_dirs


def process_batch(input_dir, output_dir):
    """
    Processes a batch, collecting mime-type data from its files.
    Writes the results into a single tsv.gz file.
    """
    stat_file = (output_dir / input_dir.name).with_suffix('.tsv.gz')
    matcher_ct = re.compile(r'Content-Type: ([\w/]+)', re.I)
    matcher_wi = re.compile(r'WARC-IDentified-Payload-Type: ([\w/]+)', re.I)
    matcher_cd = re.compile(r'(Content-Disposition: )([^\n^"]*)"([^\n^"]*)"', re.I)
    with openall(stat_file, "wt", encoding="utf-8") as f:
        for file in input_dir.iterdir():
            file_name = input_dir.name + '/' + file.name
            for doc in parse_file(file):
                # Get the type from the metadata of the doc tag:
                if doc.attrs['mime-type']:
                    type_from_doctag = doc.attrs['mime-type']
                else:
                    type_from_doctag = '-'
                # Get the type from the warc request header:
                match_warcid = matcher_wi.search(doc.meta['request'])
                if match_warcid:
                    type_from_warc_id = match_warcid.groups(1)[0]
                else:
                    type_from_warc_id = '-'
                # Get the type from the response header:
                match_resp = matcher_ct.search(doc.meta['response'])
                if match_resp:
                    type_from_response = match_resp.groups(1)[0]
                else:
                    type_from_response = '-'
                # Get the info about the attached file, if any:
                match_cd = matcher_cd.search(doc.meta['response'])
                if match_cd:
                    attached_file = match_cd.group(3)
                    if '.' in attached_file:
                        attachment_ext = attached_file.split('.')[-1]
                    else:
                        attachment_ext = 'no_extension'
                else:
                    attachment_ext = '-'
                # Process the results for this doc:
                line = file_name + '\t'
                line += type_from_doctag + '\t'
                line += type_from_warc_id + '\t'
                line += type_from_response + '\t'
                line += attachment_ext + '\t'
                print(line, file=f)


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    task_dirs = fetch_directories(args.input_dir, args.from_dir, args.upto_dir)
    logging.debug(f'The directories to process are: {task_dirs}')

    f = partial(process_batch, output_dir=args.output_dir)
    with Pool(args.processes) as p:
        for _ in p.imap_unordered(f, task_dirs):
            pass

    # process_batch(task_dirs[0], args.output_dir)


if __name__ == '__main__':
    main()
