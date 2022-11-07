#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Deduplicates a batch of index directories from earliest to the latest.

Keeps the list of URLs already processed (deduplication_urls.gz), as well as the
catalog of which index directories have already been deduplicated
(deduplication.done) in the input directory.
"""

from argparse import ArgumentParser
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import mkstemp

from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('input_dir', type=Path,
                        help='the index directory. Each subdirectory in it '
                             'is considered to be an index for a Common Crawl '
                             'monthly dump.')
    parser.add_argument('output_dir', type=Path,
                        help='the output directory. Each subdirectory in the '
                             'input directory is mirrored here, its contents '
                             'deduplicated against all preceeding dumps.')
    parser.add_argument('--log-file', '-l', type=Path,
                        help='the log file to output logs from all processes.')
    parser.add_argument('--hash', action='store_true',
                        help='use hashes to store the URLs to skip. Uses less '
                             'memory, but there is a chance for hash collision '
                             '(though not high: all the Hungarian URLs of '
                             '2017-2018 failed to produce one).')
    parser.add_argument('--keep', '-k', choices=['latest', 'biggest'],
                        default='biggest',
                        help='which occurrence to keep. Default: biggest.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    return args


def read_config(config_file: Path) -> list[str]:
    """
    Reads the configuration file. The "configuration" file contains the list
    of directories already processed.
    """
    if config_file.is_file():
        with openall(config_file) as inf:
            return [
                Path(line).resolve() for line in map(str.strip, inf) if line
            ]
    else:
        return []


def write_config(config: list[str], config_file: Path):
    with openall(config_file, 'wt') as outf:
        for index_dir in config:
            print(index_dir, file=outf)


def new_urls(urls_file: str, output_dir: Path) -> int:
    filename = None
    try:
        handle, filename = mkstemp('.gz')
        os.close(handle)
        command = (
            f'sort '
            f'<(cd {output_dir}; ls | parallel \'zcat {{}} | cut -d" " -f2\') '
            f'<(zcat {urls_file}) '
            f'| gzip > {filename}'
        )
        try:
            subprocess.run(command, shell=True, executable='/bin/bash')
        except subprocess.CalledProcessError as cpe:
            raise ValueError(cpe.returncode)
        try:
            shutil.copyfile(filename, urls_file)
        except OSError as oe:
            logging.error(f'Error copying updated URL list to {urls_file}: {e}.'
                          'The target file might have been corrupted. The '
                          'updated list is preserved at {filename}.')
            filename = None
            raise ValueError(oe.errno)
        return 0
    except ValueError as ve:
        return ve.args[0]
    finally:
        if filename is not None:
            os.unlink(filename)


def main():
    args = parse_arguments()

    if args.log_file:
        logging.basicConfig(
            level=getattr(logging, args.log_level.upper()),
            format='%(asctime)s - %(process)s - %(levelname)s - %(message)s',
            filename=args.log_file
        )
    else:
        logging.basicConfig(
            level=getattr(logging, args.log_level.upper()),
            format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
        )

    config_file = args.input_dir / 'deduplication.done'
    url_file = args.input_dir / 'deduplication_urls.gz'

    if not args.output_dir.is_dir():
        args.output_dir.mkdir(parents=True)

    input_indices = [
        d.resolve() for d in args.input_dir.iterdir() if d.is_dir()
    ]
    indices_done = read_config(config_file)
    indices_to_go = sorted(idx for idx in input_indices
                           if idx not in set(indices_done))
    logging.info(f'Found {len(input_indices)} index directories, out of which '
                 f'{len(indices_to_go)} are new.')

    for index_dir in indices_to_go:
        output_dir = args.output_dir / index_dir.name
        url_str = f' -s {url_file}' if url_file.is_file() else ''
        log_str = f' 2>> {args.log_file}' if args.log_file else ''
        retval = os.system(
            f'deduplicate_index_urls.py -i {index_dir} -o {output_dir}'
            f'{url_str}{" --hash" if args.hash else ""} -k {args.keep} '
            f'-L {args.log_level}'
            f'{log_str}'
        )
        if (ec := os.waitstatus_to_exitcode(retval)) != 0:
            logging.error(f'Nonzero return value for {index_dir}.')
            sys.exit(ec)
        if (ec := new_urls(url_file, output_dir)) != 0:
            sys.exit(ec)
        indices_done.append(index_dir)
        write_config(indices_done, config_file)


if __name__ == '__main__':
    main()
