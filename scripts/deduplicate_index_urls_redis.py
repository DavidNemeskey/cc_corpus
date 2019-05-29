#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Deduplicates the urls in the index using redis."""

from argparse import ArgumentParser
from collections import defaultdict
import concurrent.futures as cf
from contextlib import contextmanager
from functools import partial
import gzip
from itertools import zip_longest
import logging
import os
import os.path as op
import re
import subprocess
import time

from multiprocessing_logging import install_mp_handler
import redis

from cc_corpus.utils import grouper


file_name_p = re.compile('(\d{4}-\d{2}-\d+).gz$')

latest_script = """
local old_warc = redis.call('hget', KEYS[1], 'warc')
if not old_warc or old_warc < ARGV[1] then
  redis.call('hmset', KEYS[1], 'warc', ARGV[1], 'offset', ARGV[2], 'length', ARGV[3], 'index', ARGV[4])
end
"""

biggest_script = """
local old_length = redis.call('hget', KEYS[1], 'length')
if not old_length or tonumber(old_length) < tonumber(ARGV[3]) then
  redis.call('hmset', KEYS[1], 'warc', ARGV[1], 'offset', ARGV[2], 'length', ARGV[3], 'index', ARGV[4])
end
"""

sort_script = """
local num_urls = 0
for _, url in ipairs(redis.call('KEYS', 'u:*')) do
  local flat_map = redis.call('HGETALL', url)
  for i = 1, #flat_map, 2 do
    if flat_map[i] == 'index' then
      redis.call('RPUSH', flat_map[i + 1], url)
    end
  end
  num_urls = num_urls + 1
end
return num_urls
"""

# We return a list from HGETALL instead of a dict, because
# redis / lua / redis-py cannot send back dictionaries

# local function hgetvalues(hash_key)
#     local flat_map = redis.call('HGETALL', hash_key)
#     local keys = {}
#     local values = {}
#     for i = 1, #flat_map, 2 do
#         keys[#keys + 1] = flat_map[i]
#         values[#values + 1] = flat_map[i + 1]
#     end
#     return keys, values
# end

index_script = """
local ret = {}
for _, url in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
  ret[#ret + 1] = url:sub(3)
  ret[#ret + 1] = redis.call('HGETALL', hash_key)
end
return ret
"""


class Record():
    def __init__(self, warc, offset, length, index):
        self.warc = warc
        self.offset = int(offset)
        self.length = int(length)
        self.index = index

    def __repr__(self):
        return '({}, {}, {} in {})'.format(
            self.warc, self.offset, self.length, self.index)


@contextmanager
def mem_only_redis(port):
    """Creates a memory-only redis instance as a context manager."""
    # See https://stackoverflow.com/a/41238678/2765164
    proc = subprocess.Popen(
        'redis-server --save "" --appendonly no --port {}'.format(port),
        shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        yield
    finally:
        # Just terminate() doesn't work, so we need to shut redis down.
        r = redis.Redis(port=port)
        r.shutdown(nosave=True)
        proc.terminate()


def parse_arguments():
    parser = ArgumentParser('Deduplicates the urls in the index.')
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the index directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--keep', '-k', choices=['latest', 'biggest'],
                        default='biggest',
                        help='which occurrence to keep. Default: biggest.')
    parser.add_argument('--processes', '-p', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--redis-port', '-r', type=int, default=6666,
                        help='the port the redis process listens on.')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    if args.redis_port < 1024 or args.redis_port > 65535:
        parser.error('Redis port number not valid.')
    return args


def uniq_record(url, record, uniqs, keep):
    """
    Uniq's a record. Returns whether the record is uniq (not in uniqs), or is the
    representative of its URL (i.e. it is the latest / biggest).
    """
    if url in uniqs:
        other_record = uniqs[url]
        if keep == 'latest':
            if record.warc < other_record.warc:
                return False
        else:
            if record.length <= other_record.length:
                return False

    uniqs[url] = record
    return True


def file_to_redis(file_name, keep, port):
    logging.info('Collecting URLs from {}...'.format(file_name))
    try:
        # Collect data
        with gzip.open(file_name, 'rt') as inf:
            uniqs = {}
            file_id = file_name_p.search(file_name).group(1)
            for line_no, line in enumerate(map(str.strip, inf), start=1):
                try:
                    # After filtering, the line is prepended with the "domain"
                    # I skip that and extract it myself
                    url, warc, offset, length = line.split()[:7][-6:-2]
                    record = Record(warc, offset, length, file_id)
                    uniq_record(url, record, uniqs, keep)
                except:
                    logging.exception(
                        'Exception in file {}:{}'.format(file_name, line_no))
                    break
            logging.info('Deduplicated {} URLs in {} to {}.'.format(
                line_no, file_name, len(uniqs)))

        # Save urls into redis
        start_time = time.time()
        r = redis.Redis(port=port, decode_responses=True)
        script = r.register_script(
            biggest_script if keep == 'biggest' else latest_script)
        pipe = r.pipeline()
        for url, record in uniqs.items():
            script(keys=['u:{}'.format(url)],
                   args=[record.warc, record.offset, record.length, record.index],
                   client=pipe)
        pipe.execute()
        logging.info('Redis time: {}'.format(time.time() - start_time))
    except:
        logging.exception(
            'Exception in file {}'.format(file_name))
        return {}


def filter_file(input_file, output_file, index, port):
    try:
        logging.info('Filtering file {}...'.format(input_file))
        r = redis.Redis(port=port, decode_responses=True)
        redis_return = r.register_script(index_script)(keys=[index], client=r)
        # logging.debug('Got from redis: {}'.format(redis_return))
        uniqs = {url: Record(**dict(grouper(flat_dict, 2))) for url, flat_dict in
                 grouper(redis_return, 2)}
        if uniqs:
            with gzip.open(input_file, 'rt') as inf, gzip.open(output_file, 'wt') as outf:
                lines_printed = 0
                for line_no, line in enumerate(map(str.strip, inf), start=1):
                    try:
                        url, warc, offset, length = line.split()[:7][-6:-2]
                        record = uniqs.get(url)
                        if (
                            record and record.warc == warc and
                            record.offset == int(offset) and
                            record.length == int(length)
                        ):
                            print(line, file=outf)
                            lines_printed += 1
                    except:
                        logging.exception(
                            'Exception in file {}:{}'.format(input_file, line_no))
                logging.info('Kept {} URLs out of {} in {}.'.format(
                    lines_printed, line_no, input_file))
        else:
            logging.info('No unique records in {}.'.format(input_file))
    except:
        logging.exception('Exception!')


def main():
    args = parse_arguments()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    with mem_only_redis(args.redis_port):
        # Collect the representative records for all URLs
        files = os.listdir(args.input_dir)
        input_files = [op.join(args.input_dir, f) for f in files]
        with cf.ProcessPoolExecutor(max_workers=args.processes) as executor:
            fn = partial(file_to_redis, keep=args.keep, port=args.redis_port)
            for _ in executor.map(fn, input_files):
                pass
        logging.info('Getting keys...')

        # Sort them by file (so that the whole dict need not be sent to every process)
        r = redis.Redis(port=args.redis_port, decode_responses=True)
        num_urls = r.register_script(sort_script)(client=r)
        logging.info('Final tally: {} URLs.'.format(num_urls))
        logging.info('Redis memory usage: {}'.format(
            r.info('memory')['used_memory_human']))

        # And filter the files with these per-file dictionaries
        if not os.path.isdir(args.output_dir):
            os.mkdir(args.output_dir)
        tasks = zip(input_files,
                    [op.join(args.output_dir, f) for f in files],
                    [file_name_p.search(f).group(1) for f in files])
        with cf.ProcessPoolExecutor(max_workers=args.processes) as executor:
            fn = partial(filter_file, port=args.redis_port)
            cf.wait([executor.submit(fn, *task) for task in tasks])


if __name__ == '__main__':
    main()
