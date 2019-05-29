#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Distributes files in a directory to several directories so that we can
distribute the processing as well to separate hosts.
"""

from argparse import ArgumentParser
import os
import os.path as op
import shutil

from cc_corpus.utils import host_weight


def parse_arguments():
    parser = ArgumentParser('Distributes files in a directory to several '
                            'directories so that we can distribute the '
                            'processing as well to separate hosts.')
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the input directory.')
    parser.add_argument('--output-dir', '-o',
                        help='the output directory, to which the newly '
                             'created directories are put. If not specified, '
                             'they are put next to the input directory.')
    parser.add_argument('--host', '-H', action='append', type=host_weight,
                        dest='hosts', help='a host:weight pair.')
    args = parser.parse_args()
    if not args.output_dir:
        args.output_dir = os.path.dirname(args.input_dir)
    return args


def split_list(lst, ratios):
    """
    Splits a list into parts according to the ratios specified. Both ratios and
    the return value are dictionaries, with the keys being the names of the
    partitions.
    """
    index = 0
    partitions = {}
    for host, ratio in ratios.items():
        new_index = index + int(len(lst) * ratio)
        partitions[host] = lst[index:new_index]
        index = new_index
    # The rest (floating point / other errors)
    partitions[host] += lst[new_index:]
    return partitions


def link_files(partitions, input_dir, base_output_dir):
    """
    Creates symlinks to the files in input_dir into host-suffixed directories
    in the base_output_dir.
    """
    input_prefix = op.basename(input_dir)
    for host, files in partitions.items():
        output_dir = op.abspath(op.join(
            base_output_dir, '{}_{}'.format(input_prefix, host)))
        if op.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)

        for f in files:
            os.symlink(op.abspath(op.join(input_dir, f)), op.join(output_dir, f))


def main():
    args = parse_arguments()
    input_files = os.listdir(args.input_dir)
    ratios = dict(args.hosts)
    ratios = {host: weight / sum(ratios.values())
              for host, weight in ratios.items()}
    partitions = split_list(input_files, ratios)
    link_files(partitions, args.input_dir, args.output_dir)


if __name__ == '__main__':
    main()
