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
      version='1.15.0',
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
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
      ],
      keywords='corpus common crawl hungarian',
      packages=find_packages(exclude=['scripts']),
      # Install the scripts
      scripts=[
          'scripts/api_wrapper.py',
          'scripts/autonomous_cross_deduplicator.py',
          'scripts/autonomous_cross_deduplicator2.py',
          'scripts/batch_deduplicate_index_urls.py',
          'scripts/content_type_stats.py',
          'scripts/get_indexfiles.py',
          'scripts/filter_index.py',
          'scripts/deduplicate_index_urls.py',
          'scripts/deduplicate_index_urls_redis.py',
          'scripts/download_pages.py',
          'scripts/filter_corpus.py',
          'scripts/filter_warc.py',
          'scripts/filter_known_urls.py',
          'scripts/find_finished_downloads.py',
          'scripts/index_statistics.py',
          'scripts/json_export.py',
          'scripts/remove_boilerplate.py',
          'scripts/wc.py',
          'scripts/extract_pages_from_warc.py',
          'scripts/extract_docs_from_minhash.py',
          'scripts/distribute_files.py',
          'scripts/minhash.py',
          'scripts/renumber_minhash.py',
          'scripts/lsh.py',
          'scripts/dedup_filter.py',
          'scripts/extract_attributes.py',
          'scripts/renumber_corpus_files.py',
          'scripts/frequent_paragraphs.py',
          'scripts/merge_files.py',
          'scripts/rewrite_files.py',
          'scripts/remove_same_p.py',
          'scripts/sort_files.py',
          'scripts/emtsv.py',
          'scripts/parse_log.py',
          'scripts/pipe_that_line.py',
          'scripts/shuffle_tsv.py',
          'scripts/convert_tsv.py',
          'scripts/sentencepiece_to_bert.py',
          'scripts/top_n_conll.py',
          'scripts/reparse_documents.py',
          'scripts/fix_corpus.py',
          'scripts/extract_htmls.py',
          'scripts/find_new_dumps.py',
      ],
      install_requires=[
          # For parsing atom/RSS feeds
          'atoma',
          'beautifulsoup4',
          'boto3',
          'botocore',
          # Minhash + LSH
          'datasketch',
          # idzip for fast seek in compressed files
          # Uncommented until my PR is merged
          # 'idzip',
          # Manager webapp framework:
          'fastapi>=0.101.1',
          # Boilerplate removal
          'justext',
          'lxml',
          'more_itertools',
          'multiprocessing-logging>=0.3.4',
          # For parsing .bib files
          'pybtex',
          # MIME detection
          'python-magic',
          # To parse the manager webapp's config.yaml:
          'pyyaml>=6.0',
          # Just for URL deduplication
          'redis',
          'requests',
          # For handling multipart requests
          'requests-toolbelt',
          # Will maybe remove this later
          'simplejson',
          # Manager webapp database:
          'sqlalchemy>=2.0.20',
          # Boilerplate removal
          'trafilatura==1.2.2',  # 1.2.2
          # Easier TLD extraction
          'tldextract',
          # Language identification -- cld2 master doesn't compile ATM
          'cld2-cffi==0.1.4', 'langid',
          # A web server for the manager webapp:
          'uvicorn>=0.23.2',
          # WARC 3 library
          # 'warc @ https://github.com/erroneousboat/warc3/archive/master.zip',
          'warc-knot>=0.2.5',
          # Type hints for Python < 3.5
          'typing',
          # A progress bar
          'tqdm',
      ],
      # zip_safe=False,
      use_2to3=False)
