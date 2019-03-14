#!/usr/bin/env python
# vim: set fileencoding=utf-8 :

# I used the following resources to compile the packaging boilerplate:
# https://python-packaging.readthedocs.io/en/latest/
# https://packaging.python.org/distributing/#requirements-for-packaging-and-distributing

from setuptools import find_packages, setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='commoncrawl-downloader',
      version='1.1.10',
      description='A Python package for retrieving a list of urls and '
                  'specific files in bulk from Common Crawl, as well as '
                  'for processing the downloaded files.',
      long_description=readme(),
      url='https://github.com/DavidNemeskey/commoncrawl-downloader',
      author='Dávid Márk Nemeskey, Balázs Indig (original version)',
      license='LGPL',
      classifiers=[
          # How mature is this project? Common values are
          #   3 - Alpha
          #   4 - Beta
          #   5 - Production/Stable
          'Development Status :: 5 - Stable',

          # Indicate who your project is intended for
          'Intended Audience :: Science/Research',
          'Topic :: Scientific/Engineering :: Information Analysis',
          # This one is not in the list...
          'Topic :: Scientific/Engineering :: Natural Language Processing',

          # Environment
          'Operating System :: POSIX :: Linux',
          'Environment :: Console',
          'Natural Language :: English',

          # Pick your license as you wish (should match "license" above)
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',

          # Specify the Python versions you support here. In particular, ensure
          # that you indicate whether you support Python 2, Python 3 or both.
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7'
      ],
      keywords='corpus common crawl hungarian',
      packages=find_packages(exclude=['scripts']),
      # Install the scripts
      scripts=[
          'scripts/deduplicate_index_urls_redis.py',
          'scripts/filter_corpus.py',
          'scripts/filter_known_urls.py',
          'scripts/find_finished_downloads.py',
          'scripts/index_statistics.py',
          'scripts/remove_boilerplate.py',
          'scripts/wc.py',
          'scripts/extract_pages_from_warc.py',
          'scripts/extract_docs_from_minhash.py',
          'scripts/distribute_files.py',
          'scripts/minhash.py',
          'scripts/lsh.py',
          'scripts/dedup_filter.py',
      ],
      install_requires=[
          'boto3',
          'botocore',
          # Minhash + LSH
          'datasketch',
          # Boilerplate removal
          'justext',
          'lxml',
          'multiprocessing-logging',
          # Just for URL deduplication
          'redis',
          'requests',
          # Will maybe remove this later
          'simplejson',
          # Easier TLD extraction
          'tldextract',
          # Language identification
          'cld2-cffi', 'langid',
          # WARC 3 library
          'warc',
      ],
      dependency_links=[
          # WARC 3 library
          'git+git://github.com/erroneousboat/warc3.git#egg=warc'
      ],
      # zip_safe=False,
      use_2to3=False)
