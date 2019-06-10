# CommonCrawl Downloader

A Python package for retrieving a list of urls and specific files in bulk from Common Crawl, as well as for processing the downloaded files.

This is a modification of the original code in [this repo](https://github.com/ppke-nlpg/commoncrawl-downloader).

## Overview

With this tool you can query the CommonCrawl index and download pages to the local machine anonymously.
It optionally uses [JusText](http://corpus.tools/wiki/Justext) to remove boilerplate content and only keep real text.
Using this tool one can create raw text corpora from web (CommonCrawl) easily.

This repository also contains a massively fixed and overhauled version of [cdx-index-client](https://github.com/ikreymer/cdx-index-client/tree/1ae1301ae4fb8416f10bed97d4c7c96ba5ab4dc7).

## Install
    
    # Python 3.x required
    pip3 install -r requirements.txt
    # Optional dependencies (for faster processing)
    [mawk](http://invisible-island.net/mawk/mawk.html) or any AWK implementation
    GNU parallel

## Examples
    # Dowload index for specfic condition
    ./get_indexfiles.sh CONDITION OUTPUT_DIR LOG_FILE MAX_FULL_RETRY
    # e.g. ./get_indexfiles.sh '*.hu' cc_index get_index.log 10
    # Filter index
    ./filter_index.sh cc_index cc_index_filtered
    # Download pages for index to pages dir
    ./download_pages.py -b 'Hungarian' -o out -i 'cc_index_filtered/*.gz' --preprocessed
    # or (with optionally GNU parallel)
    zgrep "." cc_index_filtered/*.gz | sed 's/:/ /' | ./download_pages.py -b 'Hungarian' -o out -s  --preprocessed
    # or grouped by files
    zcat "$1" | awk -v prefix="$1" '{print prefix,$0}' | ./download_pages.py -b 'Hungarian' -o out -s  --preprocessed

## The new part

### From start to end

This section describes how to download and process the Common Crawl data to
arrive at a corpus.

#### Download the index

The script `get_indexfiles.py` can be used to download the index for a
specific collection. The example below downloads the index for January 2019:
```
get_indexfiles.py -q *.hu -o 2019/cc_index -l 2019_01.log -m 5 -c CC-MAIN-2019-04
```

Note that `get_indexfiles.py` is a replacement for the original
`get_indexfiles.sh`.

#### Filter the index

Next, filter the index with the script `filter_index.py`:
```
filter_index.py 2019/cc_index/ 2019/cc_index_filtered/ -a commoncrawl-downloader2/allowed_mimes.txt -P 12
```

This script is a replacement for and an improvement on the original
`filter_index.sh`. Being Python, it might be a little slower than the original
though, hence the `-P` option for multiprocessing.

#### Index statistics

At any point while working with indices, you can query the index statistics like
```
index_statistics.py -i 2019/cc_index/ -o 2019/stats/index -P 12
```

This script writes 5 files:

- `urls.tsv` lists the individual urls and their count
- `domains.tsv`, `statuses.tsv` and `mimes.tsv` list the domains / status codes
  / mime types in the index, along with their count and percentage
- `stats.tsv` contains general statistics, such as the total number of documents

#### Index deduplication

Once the index is filtered, it should be deduplicated, because the same URLs
are re-downloaded time and again (as many times as 240 in 4 month). The first
step is to collect all URLs we have already downloaded in a previous batch.
For example, if we already have the (deduplicated) URLs of the previous year,
we should do something like this first:
```
zcat 2018/cc_index_dedup/*.gz | cut -d' ' -f2 | sort | gzip > 2018_index_urls.gz
```

Alternatively, the URLs can be extracted from an already existing corpus:
```
extract_attributes.py -i 2018/cc_corpus/ -a url -o 2018_urls.tsv.gz -P 12 2> /dev/null
```

Generally, it is better to collect the list from the index, because the corpus
might already have been filtered (based on language, length, etc.), which means
a lot of URLs that we have downloaded and discarded don't appear in it, so we'll
just re-download them again.

Once the list of URLs to skip is complete, the index can be deduplicated like
```
deduplicate_index_urls.py -i 2019/cc_index_filtered/ -o 2019/cc_index_dedup/ -s 2018_index_urls.gz -k biggest
```

Note: unfortunately, loading the old URLs takes a very long time... also, the
script runs in a single process as Python's shared memory performance is
abysmal.

#### Download pages

Pages in the (filtered, deduplicated) index can be downloaded by the command
```
download_pages.py -o 2019/cc_downloaded -e warc.gz -i '2019/cc_index_dedup/*.gz'
```

This step takes a while, so it make sense to [distribute the work among
a cluster of machines](#tech). However, it is a bit more involved than
distributing other scripts, as this one has been inherited from the old
repository. The differences are:

1. The script requires a `glob` (possibly wildcard) expression that expands to
   a list of input files, and not an input directory. So the place of the host
   name in the value to the `-i` argument must be specified manually (with
   `{}`, see below)
1. The number of processes must be set to 0. This eliminates the `-P` argument
   from the Python command line. We need to do this because first, `-P` means
   something else in `download_pages.py`, and second, the script uses threads
   anyway.

```
ansible-playbook -i hosts distribute_files.yml -e
    '{"input_dir": "/mnt/data/lang/Hungarian/cc_corpus/2019/cc_index_dedup",
      "output_dir": "/mnt/data/lang/Hungarian/cc_corpus/2019"}'

ansible-playbook -i hosts python.yml -e
    '{"python_script": "download_pages.py",
      "log_file": "2019_download.log",
      "working_dir": "/mnt/data/lang/Hungarian/cc_corpus/",
      "arguments": "-o $output_dir -e warc.gz -i $input_glob",
      "per_host_args": {"input_glob": "\"2019/cc_index_dedup_{}/*.gz\"",
                        "output_dir": "2019/cc_downloaded/"}, "processes": 0}'
```

#### Remove boilerplate

Boilerplate code is removed with `justext`, which also splits the data into
paragraphs. The script to run is `remove_boilerplate.py`. Since we have already
split up the data between hosts, we can distribute boilerplate removal as well.
```
ansible-playbook -i hosts python.yml -e
    '{"python_script": "remove_boilerplate.py",
      "log_file": "2019_remove_boilerplate.log",
      "working_dir": "/mnt/data/lang/Hungarian/cc_corpus/",
      "arguments": "-i $index -w $warc -o $output",
      "per_host_args": {"index": "2019/cc_index_dedup/",
                        "warc": "2019/cc_downloaded/",
                        "output": "2019/cc_corpus/"}}'
```

### Type checking

Some of the code I have annotated with
[type annotations](https://docs.python.org/3/library/typing.html). To validate
the annotations, you can install [mypy](http://mypy-lang.org/) via `pip` and
run it on a file or directory. The following command also shows the recommended
switches:
```
mypy --python-version 3.5 --no-strict-optional --ignore-missing-imports scripts/frequent_paragraphs.py
```

### <a name="tech"></a>Technicalities

Most of the tasks can be executed on a single server, albeit a little patience
is in order. Others (mostly anything related to minhashing, especially
paragraphs), however, are computationally intensive, and benefit from
distributed execution. While there are several possible architecture options
(MapReduce or Spark comes to mind), we opted for a much simpler design, based
on Ansible.

The next section explain how to run a task in a distributed fashion. In a
nutshell, the exact same script that would be run on a single machine is run
parallelly on multiple servers, each attending to a slice of the full data.

Below, the word _Controller_ shall refer to the machine Ansible playbooks are
executed from; _Slave_ shall refer to the machines doing running the actual
task (Python script).

#### Prerequisites

In order for our code to work, we need the servers in the cluster to be able
to see the same data disk. More specifically, the Slaves are not required to
see each other's data, but Controller must be able to dispatch the data to
the Slaves' disks. The easiest setup is a network drive that is shared to all
machines.

All machines must have Python 3.5 or newer installed. This is in addition to the
following lists:

Controller:
- Ansible
- a hosts file that lists the slave machines to be used

Slaves:
- git
- tmux
- virtualenv

If these requirements are met, the Ansible playbook `configuration.yml` can be
used to {un}install the code -- this is a prerequisite of the next steps:
```
ansible-playbook -i hosts configuration.yml --tags {install,cleanup}
```

#### Data distribution

Most task take one (or more) input directories, process the files in them
one-by-one, and write the result to an output directory. The script
`distribute_files.py` apportions the input files into per-Slave directories,
e.g.
```
distribute_files.py -i 2018/cc_corpus_hu/ -o working_dir/ -H host1:1 -H host2
-H host3:0.5
```
The number after the host names are the _host weights_: the number of files
assigned to each host will be proportional to these numbers.

**Warning**: the script only sees files at this point, so if the number of
documents is distributed unevenly, a machine might still end up with a
disproportionately large or small amount of data. Use `renumber_corpus_files.py`
to distribute documents evenly in the input files.

#### Task execution

The tasks are run via Ansible. Each Python script can be executed distributedly
(is there such a word?) by running the playbook `python.yml`. Most of the work
is done by the `python` role.

What it does is:
- it runs the script specified in the `python_script` argument on all Slaves in
a tmux session
- calls it with the `-P processes` argument where `processes` is a host variable
(so it can be set on a group or host basis)
- passes the rest of the `arguments` to the script as-is
- _except_ arguments whose value starts with a dollar sign (`$`). These are
interpreted as keys in the `per_host_args` dictionary. When running the script,
the values in the dictionary are copied into the argument line, _extended with
the host name_
- the script will be executed in the `working_dir` of the user's choosing.

The script arguments can be specified in the `-e` (`--extra-args`) argument of
`ansible-playbook`, in the following way. Note that per-host arguments are
supplied in a dictionary (without the `-` or `--` in the keys).
Note that the value to `-e` must be in JSON (YAML) format.

```
ansible-playbook -i hosts python.yml -e
'{"python_script": "minhash.py", "log_file": "minhash_2018.log",
  "working_dir": "/mnt/data/lang/Hungarian/cc_corpus",
  "arguments": "--input $input -o $output --unit doc -p 256 -n 5 -L info",
  "per_host_args": {"input": "2018/cc_corpus_hu",
                    "o": "2018/cc_corpus_hu_minhashes"}}'
```

#### Output collection

If all Slaves operate on shared storage, the output dictionary can be a
common parameter, and then all files will be written to it. If not, it must be
a per-host parameter, and the contents of the per-host output directories must
be merged by hand.

## Licence

GNU LGPL 3.0 or any later version.

If you use this program please cite the following paper:

Indig Balázs. _Közös crawlnak is egy korpusz a vége -- Korpuszépítés a CommonCrawl .hu domainjából_ XIV. Magyar Számítógépes Nyelvészeti Konferencia (MSZNY 2018). 125--134. Szeged. 2018.

    @inproceedings{indig_2018a,
        title = {K{\"o}z{\"o}s crawlnak is egy korpusz a v{\'e}ge -- Korpusz{\'e}p{\'i}t{\'e}s a CommonCrawl .hu domainj{\'a}b{\'o}l},
        booktitle = {XIV. Magyar Sz{\'a}m{\'i}t{\'o}g{\'e}pes Nyelv{\'e}szeti Konferencia (MSZNY 2018)},
        year = {2018},
        pages = {125{\textendash}134},
        publisher={Szegedi Tudom{\'a}nyegyetem Informatikai Tansz{\'e}kcsoport},
        organization = {Szegedi Tudom{\'a}nyegyetem Informatikai Int{\'e}zet},
        address = {Szeged},
        author = {Indig, Bal{\'a}zs},
        editor = {Vincze, Veronika}
    }
