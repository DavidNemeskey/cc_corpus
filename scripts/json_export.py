#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import json
import logging
from pathlib import Path

from cc_corpus.utils import collect_inputs, openall
from cc_corpus.corpus import parse_file

def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the output directory.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    if not args.input_dir.is_dir():
        parser.error('The directory for the batches must exist.')
    return args


def write_json(document, output_file):
    logging.info(f'Writing document to file: {output_file}')
    restructured_document = {}
    restructured_document['id'] = document.attrs.pop('url')
    restructured_document['meta'] = document.attrs
    # The content() function joins the paragraphs with a \n
    # This is exactly what we wanted, isn't it?
    restructured_document['text'] = document.content()
    logging.info(json_document)
    print(json_document, file=output_file)

def main():
    print("===Json export starting===")
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    # todo handle multiple directories in one go.
    input_files = collect_inputs([args.input_dir])
    logging.info('Scheduled {} files for renumbering.'.format(len(input_files)))
    logging.info(f'the files are: {input_files}')
    for os_input_file in input_files:
        # The utils.collected_inputs() is still using os.path, not pathlib.
        input_file = Path(os_input_file)
        # todo is there a parent=True setting for openall, so it creates the
        # parent directories as well?
        with openall(args.output_dir / input_file.name, 'wt') as f:
            for document in parse_file(input_file):
                write_json(document, f)
                break

if __name__ == '__main__':
    main()

