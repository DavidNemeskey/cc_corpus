#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exports files to JSONL format.
Operates with gz files
If given a directory that contains subdirectories, it will process those.
"""

from argparse import ArgumentParser
import json
import logging
from pathlib import Path
import typing

from cc_corpus.utils import collect_inputs, openall
from cc_corpus.corpus import Document, parse_file


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


def write_json(document: Document, output_file: typing.TextIO):
    """
    Writes the document given into the output file, using JSONL format.
    The url of the document will be its 'id' field.
    The rest of the  metadata contained in the original <doc> tag will be the
    'meta' field.
    The metadata contained in the request and response fields are discarded.
    The paragraphs of the document, appended by \n will be the 'text' field.
    """
    restructured_document = {}
    restructured_document['id'] = document.attrs.pop('url')
    restructured_document['meta'] = document.attrs
    # If we need to structure the text differently, then we will have to work
    # with the document.paragraph attribute instead of the content() function.
    restructured_document['text'] = document.content()
    json_document = json.dumps(restructured_document, ensure_ascii=False)
    print(json_document, file=output_file)


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    # Because utils.py#collected_inputs() cannot be given dirs which contain
    # subdirs, we have to work around this:
    input_dirs = [x for x in args.input_dir.iterdir() if x.is_dir()]
    if not input_dirs:
        input_dirs = [args.input_dir]
    input_files = collect_inputs(input_dirs)
    logging.info(f'We have {len(input_files)} files to convert to JSON.')
    logging.debug(f'the files are: {input_files}')

    # The utils.collected_inputs() is still using os.path, not pathlib:
    for os_input_file in input_files:
        logging.debug(f'The current file to process: {os_input_file}')
        input_file = Path(os_input_file)
        os_output_file = os_input_file.replace(str(args.input_dir),
                                               str(args.output_dir))
        output_dir = Path(os_output_file).parents[0]
        output_dir.mkdir(parents=True, exist_ok=True)
        with openall(os_output_file, 'wt') as f:
            for document in parse_file(input_file):
                write_json(document, f)


if __name__ == '__main__':
    main()
