#!/usr/bin/env python3
# -*- coding: utf-8, vim: expandtab:ts=4 -*-

"""
Analyzes the corpus with `emtsv <https://github.com/dlt-rilmta/emtsv>`. For now,
uses the default REST client built into
`xtsv <https://github.com/dlt-rilmta/xtsv>`, starting as many servers as there
are processes to achieve concurrency. Note that this might require a huge
amount of memory; however, the basic stuff (tokenization, morphological analysis
and disambiguation) should not cause any problems.
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
from typing import Any, Dict, Generator, List, Tuple

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.utils import collect_inputs, ispunct, openall


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
    parser.add_argument('--emtsv-dir', '-e', required=True,
                        help='the emtsv installation directory. Why it isn\'t '
                             'a proper Python package is beyond me.')
    parser.add_argument('--tasks', '-t', default='morph,pos',
                        help='the analyzer tasks to execute. The default is '
                             'morph,pos. Note that the initial tok task is '
                             'always included implicitly.')
    parser.add_argument('--extension', '-x', type=parse_extension, default=None,
                        help='the extension of the tsv files. The default is '
                             'to keep the original filename. The format should '
                             'be old#new; then, old will be replaced by new.')
    parser.add_argument('--max-sentence-length', '-s', type=int, default=500,
                        help='limit the length of sentences (in tokens) to '
                             'pass to emtsv. The default is 500.')
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


# The initiated emtsv tools (modules). An :module:`xtsv` detail.
# Initialized in :func:`start_emtsv`.
inited_tools = None  # type: Dict[str, Any]
# A list of the module used (same as --tasks, split)
used_tools = None  # type: List[str]
# Regex to extract a sentence from quntoken's output
senp = re.compile(r'<s>(.+?)</s>', re.S)
# Regex to enumerate the XML tags from the sentence in quntoken's output
tagp = re.compile(r'<(ws?|c)>(.+?)</\1>', re.S)


def start_emtsv(emtsv_dir: str, tasks: str):
    """
    Starts the emtsv pipeline with the specified parameters and sets up the
    environment.

    :param emtsv_dir: the directory of emtsv repo. Yes, I know: this is
                      ridiculous. If you want to provide an API, do it in a
                      Python package!
    :param tasks: the tasks to run. Module names separated by commas.
    """
    global inited_tools, used_tools
    # For the emtsv import. Pathetic...
    sys.path.insert(1, emtsv_dir)
    # from __init__ import init_everything, jnius_config, tools, presets
    from __init__ import init_everything, tools, presets

    # jnius_config.classpath_show_warning = False  # Suppress warning.
    if len(tasks) > 0:
        used_tools = tasks.split(',')
        if len(used_tools) == 1 and used_tools[0] in presets:
            # Resolve presets to module names to init only the needed modules...
            used_tools = presets[used_tools[0]]

        inited_tools = init_everything({k: v for k, v in tools.items() if k in set(used_tools)})
    else:
        inited_tools = init_everything(tools)

    logging.getLogger('xtsv').setLevel(logging.WARNING)


def analyze_file_stats(input_file: str, output_file: str):
    import cProfile
    cProfile.runctx('analyze_file(input_file, output_file)',
                    globals(), locals(), output_file + '.stats')


def get_sentences(xml_tokens: str) -> Generator[Tuple[int, str, str], None, None]:
    """
    Parses the XML output of quntoken and yields the sentences one-by-one.
    More specifically, the following tuple is yielded:

    1. the length of the sentence;
    2. the sentence in tsv format, to be forwarded to emtsv;
    3. the sentence in text format, to be included in the output file as-is.

    .. note::

        Since quntoken returns all punctuation marks as separate tokens,
        constructs such as ``!!!!!!!!`` would cause problems in PurePos and
        beyond. Therefore, this function allows at most 3 punctuation tokens
        next to each other; the surplus is dropped.

    :param xml_tokens: the XML output of quntoken.
    """
    for sen in senp.finditer(xml_tokens):
        tsv_tokens, text_tokens = ['form'], []
        num_puncts = 0
        for m in tagp.finditer(sen.group(1)):
            if m.group(1) == 'ws':
                # To get rid of newlines, etc. in the text version
                text_tokens.append(' ')
            else:
                text_tokens.append(m.group(2))
                if ispunct(m.group(2)):
                    if num_puncts == 3:
                        continue
                    num_puncts += 1
                else:
                    num_puncts = 0
                tsv_tokens.append(m.group(2))
        yield len(tsv_tokens), '\n'.join(tsv_tokens) + '\n\n', ''.join(text_tokens)


class XtsvFilter(logging.Filter):
    """
    Consumes all log messages issued by xtsv and reissues them with information
    that allows the localization of the problem.
    """
    def __init__(self):
        super().__init__(name='xtsv')
        self.file = self.url = self.sent = None

    def filter(self, record: logging.LogRecord):
        """Consumes xtsv messages and lets all others pass through."""
        if super().filter(record):
            logging.log(record.levelno,
                        f'xtsv {record.levelname.lower()} in file {self.file}, '
                        f'document {self.url}, with sentence: "{self.sent}": '
                        f'{record.msg}.')
            return False
        else:
            return True

    def set(self, file: str, url: str, sent: str):
        """Sets the parameters necessary to localize the error."""
        self.file = file
        self.url = url
        self.sent = sent


def analyze_file(input_file: str, output_file: str,
                 max_sentence_length: int = sys.maxsize):
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

    with Pool(args.processes, initializer=start_emtsv,
              initargs=[args.emtsv_dir, args.tasks]) as pool:
        f = partial(analyze_file, max_sentence_length=args.max_sentence_length)
        pool.starmap(f, zip(input_files, output_files))
        logging.debug('Joining processes...')
        pool.close()
        pool.join()
        logging.debug('Joined processes.')

    logging.info('Done.')


if __name__ == '__main__':
    main()
