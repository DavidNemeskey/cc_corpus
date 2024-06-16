#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Trains a tokenizer on the corpus.

It uses a tokenizer from hugging face as its base.
It uses the configurations of the base, but not its token vocab.
For example: mistralai/Mistral-7B-Instruct-v0.2
"""

from argparse import ArgumentParser
import logging
import os
from pathlib import Path
from transformers import AutoTokenizer

from cc_corpus.utils import collect_inputs
from cc_corpus.corpus import parse_file


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='The input directory where the corpus is.')
    parser.add_argument('--output-dir', '-o', type=Path,
                        help='The output directory for the tokenizer.')
    parser.add_argument('--mode', '-m', type=str, required=True,
                        help='Sets what task to do: count, train or '
                             'generate_sp_corpus')
    parser.add_argument('--base-tokenizer', '-bt', type=str, required=True,
                        help='The hugging face moniker or the path to the'
                             'base tokenizer.')
    parser.add_argument('--vocab-size', '-vc', type=int, default=52000,
                        help='The vocabulary size of the new tokenizer.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    if not args.input_dir.is_dir():
        parser.error('The directory for the input (the corpus) must exist.')
    if args.mode in ['train', 'generate_sp_corpus'] and not args.output_dir:
        parser.error('For this mode you must specify an output dir.')
    return args


def get_training_corpus(dir):
    # This is a generator that yields one file's worth of documents per
    # iteration. We usually chunk our final corpus to have 5000 documents
    # per file.
    input_files = collect_inputs([dir])
    # logging.info(f'Load {len(input_files)} files as corpus.')
    for input_file in input_files:
        yield [document.content() for document in parse_file(input_file)]


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    os.nice(20)

    training_corpus = get_training_corpus(args.input_dir)
    base_tokenizer = AutoTokenizer.from_pretrained(args.base_tokenizer)

    example = "Szia uram, tokenizer érdekelne?"
    tokens = base_tokenizer.tokenize(example)
    print(f'Tokenizing the following text: {example}:\n')
    print(tokens)
    print(len(tokens))
    print("=======")

    if args.mode == 'count':
        logging.info('Started counting the token count of the corpus.')
        token_count = 0
        for text_batch in training_corpus:
            for text in text_batch:
                tokens = base_tokenizer.tokenize(text)
                token_count += len(tokens)
        print(token_count)
        logging.info(f'Counted {token_count} tokens in the corpus.')
    elif args.mode == 'train':
        logging.info('Training a new tokenizer.')
        new_tokenizer = base_tokenizer.train_new_from_iterator(
            training_corpus,
            args.vocab_size
        )
        tokens = new_tokenizer.tokenize(example)
        print(f'Tokenizing the following text: {example}:\n')
        print(tokens)
        print(len(tokens))

        new_tokens = set(new_tokenizer.vocab).difference(base_tokenizer.vocab)
        print(f'We got {len(new_tokens)} new tokens.')

        new_tokenizer.save_pretrained(args.output_dir)
    elif args.mode == 'generate_sp_corpus':
        args.output_dir.mkdir(parents=True, exist_ok=True)
        with open(args.output_dir / 'corpus.txt', 'wt') as out_f:
            for text_batch in training_corpus:
                for text in text_batch:
                    print(text, file=out_f)
    else:
        logging.info(f'The mode {args.mode} is undefined.')


# TODO
# The following dependencies were added:
# pip install transformers
# pip install chardet


if __name__ == '__main__':
    main()
