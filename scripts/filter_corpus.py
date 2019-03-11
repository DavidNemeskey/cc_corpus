#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Filters documents in a corpus. Currently two filters are supported:
    - a language filter that discards documents not in one of the accepted
      languages
    - length filter that discards too short documents
"""

from argparse import ArgumentParser
from collections import Counter
from functools import partial
import logging
from multiprocessing import Pool
import os
import re

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser('Filters documents in a corpus. Currently two '
                            'filters are supported:'
                            '- a language filter that discards documents not '
                            'in one of the accepted languages'
                            '- length filter that discards too short documents')
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the corpus directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--language', '-l', action='append', default=[],
                        dest='languages',
                        help='activates language filtering and marks a '
                             'language to keep. Should be specified once '
                             'per language.')
    parser.add_argument('--language-unit', '-u', choices=['doc', 'p'],
                        default='p',
                        help='the unit of language detection: document or '
                             'paragraph.')
    parser.add_argument('--min-len', '-m', type=str,
                        help='the minimum number of characters / words in a '
                             'document. Activates length filtering. Values '
                             'are accepted in the format of e.g. 500c and 100w.')
    parser.add_argument('--keep-urls', '-k', action='append', default=[],
                        help='keeps only the URLs in the specified url file(s).')
    parser.add_argument('--drop-urls', '-d', action='append', default=[],
                        help='drop all URLs in the specified url file(s).')
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
    if args.min_len and not re.match(r'^\d+[w|c]$', args.min_len):
        parser.error('Invalid value for the minimum length parameter.')
    if not args.languages and not args.min_len:
        parser.error('At least one filter must be specified.')
    if args.languages:
        try:
            import cld2  # noqa
        except:
            parser.error('cld2 library not available.')
    return args


def each_doc(doc_iter, stats):
    """
    This function is just there so that we can count the number of documents
    initially.
    """
    doc_no = 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        yield doc
    stats['initial'] = doc_no


def check_language(text, languages):
    """Checks if text is written in any of the specified languages."""
    import cld2
    try:
        _, _, lang = cld2.detect(text)
        return lang[0].language_code in languages
    except Exception as cld_ex:
        # cld2 cannot handle some UTF-8 characters that Python can. See
        # https://github.com/mikemccand/chromium-compact-language-detector/issues/22
        # There is a workaround, but I'd rather just call langid in this case
        import langid
        lang, _ = langid.classify(text)
        return lang in languages


def filter_languages_doc(doc_iter, languages, stats):
    """Filters languages on the document level."""
    doc_no, kept = 0, 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        content = doc.content()
        try:
            if check_language(content, languages):
                kept += 1
                yield doc
        except Exception:
            logging.exception('Error identifying document {}\'s language'.format(
                repr(doc)))
    if doc_no:
        logging.info('Filtered {} documents based on language, kept {}.'.format(
            doc_no, kept))
    stats['language'] = kept


def filter_languages_p(doc_iter, languages, stats):
    """Filters languages on the paragraph level."""
    doc_no, kept, all_p, kept_p = 0, 0, 0, 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        all_p += len(doc.paragraphs)
        try:
            doc.paragraphs = [p for p in doc.paragraphs
                              if check_language(p, languages)]
            if doc.paragraphs:
                kept_p += len(doc.paragraphs)
                kept += 1
                yield doc
        except:
            logging.exception('Error identifying document {}\'s language'.format(
                repr(doc)))
    if doc_no:
        logging.info('Filtered {} documents / {} paragraphs based on language, '
                     'kept {} / {}.'.format(doc_no, all_p, kept, kept_p))
    stats['language'] = kept
    stats['language_p'] = kept_p


def filter_length(doc_iter, min_len_str, stats):
    min_len = int(min_len_str[:-1])
    arg = {min_len_str[-1]: True}

    doc_no, kept = 0, 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        if doc.wc(**arg) >= min_len:
            kept += 1
            yield doc
    if doc_no:
        logging.info('Filtered {} documents based on length, kept {}.'.format(
            doc_no, kept))
    stats['length'] = kept


def process_file(filename, input_dir, output_dir, languages,
                 language_unit, min_len_str, keep_urls, drop_urls):
    input_file = os.path.join(input_dir, filename)
    output_file = os.path.join(output_dir, filename)
    logging.info('Processing file {}...'.format(filename))

    stats = Counter()
    it = parse_file(input_file, True, True, True)
    it = each_doc(it, stats)
    if languages:
        if language_unit == 'doc':
            it = filter_languages_doc(it, languages, stats)
        else:
            it = filter_languages_p(it, languages, stats)
    if min_len_str:
        it = filter_length(it, min_len_str, stats)
    if keep_urls:
        pass
    if drop_urls:
        pass
    try:
        with openall(output_file, 'wt') as outf:
            for doc in it:
                print(doc, file=outf)
    except:
        logging.exception('Got an error.')
    logging.info('Finished processing file {}...'.format(filename))
    return stats


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    files = os.listdir(args.input_dir)
    logging.info('Scheduled {} files for filtering.'.format(len(files)))
    p = Pool(args.processes)
    f = partial(process_file, input_dir=args.input_dir,
                output_dir=args.output_dir, languages=set(args.languages),
                language_unit=args.language_unit, min_len_str=args.min_len)
    # Note: + / sum() do not keep keys with 0 values here, hence update()
    stats = Counter()
    for sub_stats in p.map(f, files):
        stats.update(sub_stats)
    logging.info('Statistics: {}'.format(stats))
    p.close()
    p.join()
    logging.info('Done.')


if __name__ == '__main__':
    main()
