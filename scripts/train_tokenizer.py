#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Trains a tokenizer on the corpus.
"""

from argparse import ArgumentParser
import logging
from pathlib import Path
from transformers import AutoTokenizer

from cc_corpus.utils import collect_inputs
from cc_corpus.corpus import parse_file


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='The input directory where the corpus is.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='The output directory for the tokenizer.')
    parser.add_argument('--base-tokenizer', '-bt', type=str, required=True,
                        help='The hugging face moniker of the tokenizer'
                             'which will serve as the basis for the new one.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    if not args.input_dir.is_dir():
        parser.error('The directory for the batches must exist.')
    return args


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    input_files = collect_inputs([args.input_dir])
    logging.info(f'Load {len(input_files)} files as corpus.')

    # TODO turn this into a function that returns a generator.
    # One yield should return all the documents from a single file.
    training_corpus = []
    for input_file in input_files:
        for document in parse_file(input_file):
            training_corpus.append(document.content())
    logging.info(f'Collected {len(training_corpus)} documents as corpus.')

    old_tokenizer = AutoTokenizer.from_pretrained(args.base_tokenizer)

    example = "Szia uram, tokenizer érdekelne?"
    tokens = old_tokenizer.tokenize(example)
    print(f'Tokenizing the following text: {example}:\n')
    print(tokens)
    print(len(tokens))

    tokenizer = old_tokenizer.train_new_from_iterator(training_corpus, 52000)
    tokens = tokenizer.tokenize(example)
    print(f'Tokenizing the following text: {example}:\n')
    print(tokens)
    print(len(tokens))

    tokenizer.save_pretrained(args.output_dir)

# TODO
# The following dependencies were added:
# pip install transformers
# pip install chardet


if __name__ == '__main__':
    main()
