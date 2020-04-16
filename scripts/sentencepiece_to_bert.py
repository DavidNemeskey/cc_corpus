#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Converts a sentencepiece-style vocabulary file to BERT-style.
The script reads from stdin and writes to stdout.
"""

from argparse import ArgumentParser
from collections import namedtuple
import sys


# Sentencepiece tags to remove from the vocabulary...
spm_tags_to_remove = {'<unk>', '<s>', '</s>'}

# ... and format-dependent tags to add
bert_tags_to_add = ['[PAD]', '[UNK]', '[CLS]', '[SEP]', '[MASK]']
# Re-adding <unk> here because otherwise it would get a ##
lm_tags_to_add = ['<newdoc>', '<unk>']


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--tag-set', '-t', choices=['bert', 'lm'],
                        default='bert',
                        help='the output tag set. Decides what tags '
                             '(non-wordpiece) tokens to add or remove.')
    parser.add_argument('--unused-tokens', '-u', type=int,
                        help='the number of [unusedXX] tokens. '
                             'The default is 1000 for bert.')
    args = parser.parse_args()
    if not args.unused_tokens:
        args.unused_tokens = 1000 if args.tag_set == 'bert' else 0
    return args


def main():
    args = parse_arguments()
    tags_to_add = bert_tags_to_add if args.tag_set == 'bert' else lm_tags_to_add

    for tag in tags_to_add:
        print(tag)
    for unused in range(1, args.unused_tokens + 1):
        print(f'[unused{unused}]')
    for line in sys.stdin:
        token = line.strip().split('\t')[0]
        if token in spm_tags_to_remove:
            continue
        if token.startswith('▁'):
            if len(token) > 1:
                print(token[1:])
        else:
            print(f'##{token}')


if __name__ == '__main__':
    main()
