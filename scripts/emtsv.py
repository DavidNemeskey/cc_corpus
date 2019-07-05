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
from itertools import cycle
import logging
from multiprocessing import Pool
import os
import os.path as op
import subprocess
import sys
import time

from multiprocessing_logging import install_mp_handler
import requests

from cc_corpus.corpus import parse_file
from cc_corpus.utils import collect_inputs


def parse_arguments():
    def parse_slice(s, def_begin, def_step):
        try:
            begin, _, step = s.partition(':')
            begin = int(begin) if begin else def_begin
            step = int(step) if step else def_step
            return slice(begin, None, step)
        except:
            raise ArgumentTypeError('Invalid slice format; must be begin:step; '
                                    'both values are optional.')

    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories of files to analyze.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--emtsv-dir', '-e', required=True,
                        help='the emtsv installation directory. Why it isn\'t '
                             'a proper Python package is beyond me.')
    parser.add_argument('--tasks', '-t', default='tok,morph,pos',
                        help='the analyzer tasks to execute. The default is '
                             'tok,morph,pos.')
    parser.add_argument('--ports', '-p', default=slice(5000, None, 10),
                        type=partial(parse_slice, def_begin=5000, def_end=10),
                        help='a one- or two-element slice that describes the '
                             'port range used by the REST servers. E.g. '
                             '8888:8 means every 8th port up from 8888. The '
                             'defaults are 5000 and 10.')
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


def analyze_file(input_file, output_file, uri):
    """Analyzes *input_file* with the emtsv REST server running on *uri*."""
    with open(input_file) as inf, open(output_file, 'wt') as outf:
        r = requests.post(uri, files={'file': inf})
        outf.write(r.text)


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

    # Create the Flask processes
    ports = list(range(sys.maxsize)[args.ports][:args.processes])
    uris = [
        'http://127.0.0.1:{}/{}'.format(port, '/'.join(args.tasks.split(',')))
        for port in ports]
    output_files = [op.join(args.output_dir, op.basename(f))
                    for f in input_files]

    cmd = 'python {} -e {} -c -p {{}} {}'.format(
        op.join(op.dirname(__file__), 'emtsv_rest.py'), args.emtsv_dir, args.tasks)
    rests = [subprocess.Popen(cmd.format(port).split(),
                              stderr=subprocess.DEVNULL,
                              stdout=subprocess.DEVNULL) for port in ports]

    time.sleep(10)

    try:
        with Pool(args.processes) as pool:
            pool.starmap(analyze_file, zip(input_files, output_files, cycle(uris)))
            pool.close()
            pool.join()

    finally:
        # Stop the Flask processes
        for rest in rests:
            rest.terminate()

    logging.info('Done.')


if __name__ == '__main__':
    main()
