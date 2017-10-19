#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import gzip
import glob
import os.path

domain_re = re.compile('^https?://((www|ww2|ww3|www2|www3)[.])?([^/]+)(:[0-9]+)?/.*')
replace_re = re.compile('[?].*')


def get_domain_for_page(fh=sys.stdout):
    robots_count = 0
    for filename in glob.glob(os.path.join(sys.argv[1], '*.gz')):
        with gzip.open(filename) as inpfh:
            for num, line in enumerate(inpfh):
                line = line.strip()
                url, warc_file, offset_str, length_str = line.split(' ')
                if url.endswith('/robots.txt'):
                    robots_count += 1
                m = domain_re.match(url)
                if m:
                    domain = replace_re.sub('', m.group(3))
                else:
                    domain = 'NONE'
                fh.write(' '.join((domain, url, filename, warc_file, offset_str, length_str)))
    return robots_count


nr_of_robots = get_domain_for_page()
sys.stderr.write('robots.txt: {0}\n'.format(nr_of_robots))
