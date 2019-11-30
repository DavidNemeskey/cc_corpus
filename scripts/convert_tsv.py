#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Converts a corpus in the tsv format to other formats (such as BERT's input
format).
"""

from argparse import ArgumentParser
from functools import partial
from itertools import islice
import logging
from multiprocessing import Pool
import os
import os.path as op

from multiprocessing_logging import install_mp_handler

from cc_corpus.tsv import parse_file
from cc_corpus.utils import collect_inputs, openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories of tsv files.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--from-text', '-t', action='store_true',
                        help='by default, the script extracts the sentence '
                             'tokens from the first column of the tsv. If '
                             'this option is specified, the original, '
                             'untokenized sentences will be copied over from '
                             'the tsv comments.')
    parser.add_argument('--lower', '-l', action='store_true',
                        help='lowercase the text.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()

    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def lower(text, lower_case: bool = False):
    """
    Lowercases _text_ if _lower_case_ is ``True``; otherwise, keeps
    it unchanged.
    """
    return text.lower() if lower_case else text


def process_file(input_file: str, output_dir: str, from_text: bool = False,
                 lower_case: bool = False):
    """
    Converts _input_file_ from tsv to the BERT input format.

    :param input_file: the input file.
    :param output_dir: the output directory; the output file will be created
                       here, with the same name as _input_file_ (except any
                       `tsv` in its name is replaced with `txt`).
    :param from_text: if `True`, the output will contain the original
                      (untokenized) sentences in the `# text` comment lines.
                      If `False` (the default), the output is extracted from
                      the tsv.
    :param lower_case: lowercase the text?
    """
    transform = partial(lower, lower_case=lower_case)
    output_file = op.join(output_dir, op.basename(input_file).replace('tsv', 'txt'))
    logging.debug(f'Converting {input_file} to {output_file}...')
    with openall(output_file, 'wt') as outf:
        for document in islice(parse_file(input_file), 1, None):
            for paragraph in document:
                for sentence in paragraph:
                    if from_text:
                        if sentence.comment.startswith('# text = '):
                            print(transform(sentence.comment[9:]), file=outf)
                    else:
                        print(' '.join(token.split('\t', 1)[0]
                                       for token in sentence.content),
                              file=outf)
            print(file=outf)
    logging.debug(f'Converted {input_file} to {output_file}.')


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not op.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    input_files = sorted(collect_inputs(args.inputs))
    logging.info('Scheduled {} files for conversion.'.format(len(input_files)))

    with Pool(args.processes) as pool:
        f = partial(process_file,
                    output_dir=args.output_dir, from_text=args.from_text,
                    lower_case=args.lower)
        res = pool.map_async(f, input_files)
        res.get()
        pool.close()
        pool.join()

    logging.info('Done.')


if __name__ == '__main__':
    main()
