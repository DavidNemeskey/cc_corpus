#!/usr/bin/env python3
# -*- coding: utf-8, vim: expandtab:ts=4 -*-

"""
Analyzes the corpus with `emtsv <https://github.com/nytud/emtsv>`. It uses
the emtsv REST endpoint provided by an emtsv server running either
locally or remotely. The easiest way to set up one is to use the Docker image.

The script can handle both files in the "corpus format" and tsv files; the
latter are expected to have headers and at the first column be called "form".

.. note::

    A file is sent to emtsv in a single request, which means that the returned
    parse might take up a lot of memory.
"""

from argparse import ArgumentParser, ArgumentTypeError
from functools import partial
from io import StringIO
import logging
from multiprocessing import Pool
import os
import os.path as op
import re
import sys
from urllib.parse import urlparse, urlunparse

from multiprocessing_logging import install_mp_handler
import requests

from cc_corpus.corpus import parse_file
from cc_corpus.utils import collect_inputs, openall


def parse_arguments():
    def parse_extension(s):
        try:
            return s.split('#', 1)
        except:
            raise ArgumentTypeError('Invalid extension format; must be old#new.')

    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories of files to analyze.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--emtsv-url', '-e', required=True,
                        help='the URL to the emtsv REST endpoint.')
    parser.add_argument('--tasks', '-t', default='morph,pos',
                        help='the analyzer tasks to execute. The default is '
                             'morph,pos. Note that the initial tok task is '
                             'always included implicitly for text files, but '
                             'not for tsv (see -f).')
    parser.add_argument('--extension', '-x', type=parse_extension, default=None,
                        help='the extension of the tsv files. The default is '
                             'to keep the original filename. The format should '
                             'be old#new; then, old will be replaced by new.')
    parser.add_argument('--max-sentence-length', '-s', type=int, default=500,
                        help='limit the length of sentences (in tokens) to '
                             'pass to emtsv. The default is 500.')
    parser.add_argument('--file-format', '-f', choices=['text', 'tsv'],
                        default='text',
                        help='the file format. The default is text.')
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


# Regex to extract a sentence from quntoken's output
senp = re.compile(r'<s>(.+?)</s>', re.S)
# Regex to enumerate the XML tags from the sentence in quntoken's output
tagp = re.compile(r'<(ws?|c)>(.+?)</\1>', re.S)
def analyze_file_stats(input_file: str, output_file: str):
    import cProfile
    cProfile.runctx('analyze_file(input_file, output_file)',
                    globals(), locals(), output_file + '.stats')


def analyze_tsv_file(input_file: str, output_file: str,
                     emtsv_url: str, max_sentence_length: int = sys.maxsize):
    logging.info('Analyzing tsv {}...'.format(input_file))

    lemma_col = None
    try:
        with openall(input_file) as inf, openall(output_file, 'wt') as outf:
            r = requests.post(emtsv_url, files={'file': inf})
            header, _, data = r.text.partition('\n')
            if header:
                print(header, file=outf)
                # Sometimes the lemma column is empty. In such cases, we
                # double the form as the lemma. In order to do that, we need
                # to find which column is the lemma...
                try:
                    lemma_col = header.rstrip().split('\t').index('lemma')
                except ValueError:
                    pass

            last_empty = False
            # We only allow a single empty line between sentences, or at the end
            for line in data.split('\n'):
                # Handle comments here, so that they don't cause problems
                if line.startswith('# '):
                    print(line.split('\t')[0], file=outf)
                # The other part of the no-lemma handling code
                elif lemma_col:
                    fields = line.rstrip('\r').split('\t')
                    if len(fields) > 1 and not fields[lemma_col]:
                        fields[lemma_col] = fields[0]  # form
                        print('\t'.join(fields), file=outf)
                        logging.info(f'printed line >>{line}<<, {last_empty}')
                    elif line or not last_empty:
                        # Marginally faster without the join
                        print(line, file=outf)
                # When we don't know, which one is the lemma (we should though)
                else:
                    if line or not last_empty:
                        print(line, file=outf)
                last_empty = (len(line) == 0)

        logging.info('Finished {}.'.format(input_file))
    except:
        logging.exception('Error in file {}!'.format(input_file))


def analyze_file(input_file: str, output_file: str,
                 emtsv_url: str, max_sentence_length: int = sys.maxsize):
    """
    Analyzes *input_file* with quntoken + emtsv and writes the results to
    *output_file*.

    :param max_sentence_length: sentences longer than this number will not be
                                sent to emtsv.
    """
    logging.info('Analyzing {}...'.format(input_file))
    from __init__ import build_pipeline

    # Install xtsv warning & error logging filter, so that we know where the
    # problem happens
    xtsv_filter = XtsvFilter()
    logging.getLogger().handlers[0].addFilter(xtsv_filter)
    # So that we know that everything is filtered
    assert len(logging.getLogger().handlers) == 1

    from emtokenpy.quntoken.quntoken import QunToken
    qt = QunToken('xml', 'token', False)

    header_written = False
    lemma_col = None
    try:
        with openall(output_file, 'wt') as outf:
            for doc in parse_file(input_file):
                doc_written = False
                for p_no, p in enumerate(doc.paragraphs, start=1):
                    p_written = False
                    try:
                        p_tokenized = qt.tokenize(p)
                    except ValueError:
                        logging.exception(f'quntoken error in file {input_file}'
                                          f', document {doc.attrs["url"]}, '
                                          f'paragraph {p_no}; skipping...')
                        # Skip paragraph if we cannot even tokenize it
                        continue
                    for sent_len, sent_tsv, sent_text in get_sentences(p_tokenized):
                        if sent_len > max_sentence_length:
                            logging.warning(f'Too long sentence in file '
                                            f'{input_file}, document '
                                            f'{doc.attrs["url"]}; skipping: '
                                            f'"{sent_text}"')
                            continue

                        xtsv_filter.set(input_file, doc.attrs['url'], sent_text)
                        last_prog = build_pipeline(
                            StringIO(sent_tsv), used_tools, inited_tools, {}, True)
                        try:
                            for rline in last_prog:
                                if not header_written:
                                    header_written = True
                                    outf.write(rline)
                                    # The lemma column might be empty; see
                                    # https://github.com/dlt-rilmta/emtsv/issues/7
                                    # This, along with code below, is a workaround
                                    # until that issue is fixed
                                    try:
                                        lemma_col = rline.rstrip('\n').split('\t').index('lemma')
                                    except ValueError:
                                        pass
                                if not doc_written:
                                    doc_written = True
                                    print('# newdoc id = {}'.format(doc.attrs['url']),
                                          file=outf)
                                if not p_written:
                                    # Relative paragraph id, because urls are long
                                    p_written = True
                                    print('# newpar id = p{}'.format(p_no), file=outf)
                                break
                            print('# text = {}'.format(sent_text), file=outf)
                            for rline in last_prog:
                                # The other part of the no-lemma handling code
                                if lemma_col:
                                    fields = rline.rstrip('\n').split('\t')
                                    if len(fields) > 1 and not fields[lemma_col]:
                                        fields[lemma_col] = fields[0]  # form
                                        print('\t'.join(fields), file=outf)
                                    else:
                                        # Marginally faster without the join
                                        outf.write(rline)
                                else:
                                    outf.write(rline)
                        except:
                            logging.exception(f'Error in file {input_file}, '
                                              f'document {doc.attrs["url"]}, '
                                              f'with sentence: "{sent_text}"')
        logging.info('Finished {}.'.format(input_file))
    except:
        logging.exception('Error in file {}!'.format(input_file))


def output_file_name(input_file, extension=None):
    base_name = op.basename(input_file)
    if extension:
        old, new = extension
        return re.sub(f'(.+){old}(.*?)', rf'\1{new}\2', base_name)
    else:
        return base_name


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
    logging.info('Found a total of {} input files.'.format(len(input_files)))

    output_files = [op.join(args.output_dir, output_file_name(f, args.extension))
                    for f in input_files]

    # Constructs the full emtsv url
    scheme, netloc, *_ = urlparse(args.emtsv_url)
    tasks = '/'.join(task for task in args.tasks.split(',') if task != 'tok')
    emtsv_url = urlunparse((scheme, netloc, tasks, '', '', ''))

    with Pool(args.processes) as pool:
        f = partial(
            analyze_file if args.file_format == 'text' else analyze_tsv_file,
            emtsv_url=emtsv_url, max_sentence_length=args.max_sentence_length
        )
        pool.starmap(f, zip(input_files, output_files))
        logging.debug('Joining processes...')
        pool.close()
        pool.join()
        logging.debug('Joined processes.')

    logging.info('Done.')


if __name__ == '__main__':
    main()
