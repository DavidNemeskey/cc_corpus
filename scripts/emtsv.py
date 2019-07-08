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
import logging
from multiprocessing import Pool
from multiprocessing.managers import BaseManager
import os
import os.path as op
import sys
import time
from typing import List

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
    parser.add_argument('--seconds', '-s', type=int, default=10,
                        help='the number of seconds to wait for the Flask '
                             'processes to initialize.')
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


def analyze_file(input_file, output_file, dist):
    """Analyzes *input_file* with the emtsv REST server running on *uri*."""
    logging.info('Analyzing {}...'.format(input_file))
    uri = dist.acquire()
    logging.debug('Acquired URI {}.'.format(uri))
    try:
        with open(input_file) as inf, open(output_file, 'wt') as outf:
            r = requests.post(uri, files={'file': inf})
            outf.write(r.text)
        logging.info('Finished {} at URI {}...'.format(input_file, uri))
    finally:
        dist.release(uri)
        logging.debug('Released URI {}.'.format(uri))


class URIManager(BaseManager):
    """
    A :class:`multiprocessing.managers.BaseManager` subclass for registering
    :class:`URIDistributor`.
    """
    pass


class URIDistributor:
    """
    Distributes emtsv URIs to processes asking for them. A URI is issued only
    to a single process at any time; the process has to release it so that
    others can receive it.
    """
    def __init__(self, uris: List[str]):
        """
        :param uris: the list of URIs to distribute.
        """
        self.uris = [uri for uri in uris]

    def acquire(self) -> str:
        """
        Assigns a URI to a client. The URI is removed from the list of
        available URIs until it is :meth:`release`d.
        """
        if self.uris:
            return self.uris.pop()
        else:
            raise ValueError('No more URIs to distribute.')

    def release(self, uri):
        """
        The client calls this function to release *uri*. Note that there are
        no safeguards in place to make sure *uri* is valid, or that it is one
        of the original URIs. So use this class only with friendly processes.

        :param uri: the URI to release (i.e. put back to the pool).
        """
        self.uris.append(uri)


def start_emtsv(port, emtsv_dir, tasks):
    """Starts the emtsv pipeline with the specified parameters."""
    # This is ridiculous. If you want to provide an API,
    # do it in a Python package!
    sys.path.insert(1, emtsv_dir)
    from __init__ import init_everything, pipeline_rest_api, jnius_config
    from __init__ import tools, presets

    jnius_config.classpath_show_warning = False  # Suppress warning.
    conll_comments = True
    if len(tasks) > 0:
        used_tools = tasks.split(',')
        if len(used_tools) == 1 and used_tools[0] in presets:
            # Resolve presets to module names to init only the needed modules...
            used_tools = presets[used_tools[0]]

        inited_tools = init_everything({k: v for k, v in tools.items() if k in set(used_tools)})
    else:
        inited_tools = init_everything(tools)

    with open('/dev/null', 'wt') as null:
        sys.stdout = sys.stderr = null
        app = pipeline_rest_api(name='e-magyar-tsv', available_tools=inited_tools,
                                presets=presets, conll_comments=conll_comments)
        logging.getLogger('xtsv').setLevel(logging.WARNING)
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
        # use_reloader=False is very important, otherwise Flask starts this script
        # as many times as there are servers... See
        # https://stackoverflow.com/questions/33927616/multiprocess-within-flask-app-spinning-up-2-processes
        app.run(port=port, debug=True, use_reloader=False)


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

    logging.info('Starting REST servers...')
    rpool = Pool(args.processes)
    rf = partial(start_emtsv, emtsv_dir=args.emtsv_dir, tasks=args.tasks)
    rpool.map_async(rf, ports)
    rpool.close()

    logging.debug('Sleping {} seconds...'.format(args.seconds))
    time.sleep(args.seconds)
    logging.debug('Slept.')

    URIManager.register('URIDistributor', URIDistributor)
    try:
        with Pool(args.processes) as pool, URIManager() as manager:
            dist = manager.URIDistributor(uris)
            f = partial(analyze_file, dist=dist)
            pool.starmap(f, zip(input_files, output_files))
            logging.debug('Joining processes...')
            pool.close()
            pool.join()
            logging.debug('Joined processes.')
    finally:
        logging.debug('Terminating REST servers...')
        rpool.terminate()
        rpool.join()
        logging.debug('Terminated REST servers...')

    logging.info('Done.')


if __name__ == '__main__':
    main()
