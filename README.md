# CommonCrawl Downloader

A Python package for retrieving a list of urls and specific files in bulk from Common Crawl, as well as for processing the downloaded files.

This is a modification of the original code in [this repo](https://github.com/ppke-nlpg/commoncrawl-downloader).

## Overview

With this tool you can query the CommonCrawl index and download pages to the local machine anonymously.
It optionally uses [JusText](http://corpus.tools/wiki/Justext) to remove boilerplate content and only keep real text.
Using this tool one can create raw text corpora from web (CommonCrawl) easily.

This repository also contains a massively fixed and overhauled version of [cdx-index-client](https://github.com/ikreymer/cdx-index-client/tree/1ae1301ae4fb8416f10bed97d4c7c96ba5ab4dc7).

## Installation

The package can be installed (as usual) with

```
export CFLAGS="-Wno-narrowing"
pip install -e .
```

The reason for the export is that one of the language detection packages,
`cld2-cffi`, has not been updated for a long time, and the C language related
problems (narrowing conversions) that were once only warnings are now considered
errors by modern compilers. The export tells them to disregard these errors.

## From start to end

This section describes how to download and process the Common Crawl data to
arrive at a corpus.

### Download the index

The script `get_indexfiles.py` can be used to download the index for a
specific collection. The example below downloads the index for all pages
in the `.hu` domain in January 2019:
```
get_indexfiles.py -q "*.hu" -o 2019/cc_index -l 2019_01.log -m 5 -c CC-MAIN-2019-04
```

Note that `get_indexfiles.py` is a replacement for the original
`get_indexfiles.sh`.

### Filter the index

Next, filter the index with the script `filter_index.py`:
```
filter_index.py 2019/cc_index/ 2019/cc_index_filtered/ -a commoncrawl-downloader2/allowed_mimes.txt -P 12
```

This script is a replacement for and an improvement on the original
`filter_index.sh`. Being Python, it might be a little slower than the original
though, hence the `-P` option for multiprocessing.

### Index statistics

At any point while working with indices, you can query the index statistics like
```
index_statistics.py -i 2019/cc_index/ -o 2019/stats/index -P 12
```

This script writes 5 files:

- `urls.tsv` lists the individual urls and their count
- `domains.tsv`, `statuses.tsv` and `mimes.tsv` list the domains / status codes
  / mime types in the index, along with their count and percentage
- `stats.tsv` contains general statistics, such as the total number of documents

### Index deduplication

Once the index is filtered, it should be deduplicated, because the same URLs
are re-downloaded time and again (as many times as 240 in 4 month). The first
step is to collect all URLs we have already downloaded in a previous batch.
For example, if we already have the (deduplicated) URLs of the previous year,
we should do something like this first:
```
zcat 2018/cc_index_dedup/*.gz | cut -d' ' -f2 | sort | gzip > 2018_index_urls.gz
```
or, parallelly,
```
ls *.gz | parallel -j12 -k "zcat {} | cut -d' ' -f2 " | sort | gzip > ../2018_index_urls.gz
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

### Download pages

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

### Remove boilerplate

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

### <a name="filtering">Filtering</a>

After boilerplate removal, the corpus is in its final format. However, to
increase its quality, we also filter out certain pages: those not in Hungarian
and those shorter than 1500 characters.
```
ansible-playbook -i hosts python.yml -e
    '{"python_script": "filter_corpus.py",
      "log_file": "2019_filter_corpus.log",
      "working_dir": "/mnt/data/lang/Hungarian/cc_corpus/",
      "arguments": "-i $input -o $output -l hu",
      "per_host_args": {"input": "2019/cc_corpus/",
                        "output": "2019/cc_corpus_hu/"}}'

ansible-playbook -i hosts python.yml -e
    '{"python_script": "filter_corpus.py",
      "log_file": "2019_filter_length.log",
      "working_dir": "/mnt/data/lang/Hungarian/cc_corpus/",
      "arguments": "-i $input -o $output -m 1500c",
      "per_host_args": {"input": "2019/cc_corpus_hu/",
                        "output": "2019/cc_corpus_hu_1500c/"}}'
```

### Document deduplication

Although we have filtered the index for duplicate _URLs_, the corpus might still
contain duplicate _documents_. The reason is twofold: first, we cannot filter
variations of the same URL, because there is no way to know which parameters
are important to the content (e.g. _doc_id_ probably is, while _token_ most
likely isn't); and second, the same news item / poem / etc. might have been
published on different domains. So we need document-level deduplication.

Unfortunately, deduplication is not a one-step process. First, the documents
have to be <em>minhash</em>ed:
```
ansible-playbook -i hosts python.yml -e
    '{"python_script": "minhash.py",
      "log_file": "2019_minhash.log",
      "working_dir": "/mnt/data/lang/Hungarian/cc_corpus/",
      "arguments": "-i $input -o $output -b 3000000 -u doc -p 256 -n 5 -Z 1",
      "per_host_args": {"input": "2019/cc_corpus_hu_1500c/",
                        "output": "2019/minhashes/"}}'
```

Since the command above output the files into separate directories, they need
to be copied into one:
```
renumber_minhash.py -i 2019/minhashes_host1 -i 2019/minhashes_host2
                    -o 2019/minhashes/ -k -Z 1
```

The script `lsh.py` then can be used to deduplicate documents based on their
minhashes. It has two modi operandi:

- <em>self</em>-deduplication: removes all documents that occur in one directory
  of minhash files
- <em>cross</em>-deduplication: removes all documents from a directory of
  minhash files that are already contained in another (e.g. last year's)

Usually one runs both like so:
```
lsh.py -i 2019/minhashes/ -o 2019/minhashes_self/ -t 0.95 -p 256 -P 12 self
lsh.py -i 2019/minhashes_self/ -o 2019/minhashes_full/ -t 0.95 -p 256
       -P 12 cross -c 2018/minhashes_full
```

Here, the directory `2018/minhashes_full` contains the minhashes for documents
in the 2018 corpus. We deduplicate with them to make sure we get rid of all
documents we already have.

Once the minhashes in `2019/minhashes_full/` are done, we could create a
"minhashes of documents thus far" directory that includes all documents from
2018 and 2019 by running
```
renumber_minhash.py -i 2018/minhashes_full -i 2019/minhashes_full
                    -o all_time_minhashes -b 3000000 -Z 2
```

Finally, we filter corpus to contain only the unique documents:
```
dedup_filter.py -o 2019/cc_corpus_hu_1500c_dedup/ -m 2019/minhashes/minhashes_full --ignore-missing-files
```

### <a name="frequent">Delete frequent paragraphs</a>

Some sites contain "boilerplate" paragraphs that occur in many of their documents.
We get rid of such paragraphs so that they do not skew the language model
probabilities, word statistics, etc.

It is a four-step process, this time each of which is implemented by the same
script: `frequent_paragraphs.py`.

The first step creates an index of the documents in the corpus, sorted by
domain:
```
frequent_paragraphs.py --index 2019/index.gz -P 12 index_docs
                       -i 2019/cc_corpus_hu_1500c_dedup/
```

Then, since we are going to compute the minhashes for each _paragraph_ in the
corpus, we definitely need to do it in a distributed fashion. The next step is
then the distribution of the index:
```
frequent_paragraphs.py --index 2019/index.gz -P 12 distribute -H host1 -H host2:1.5 ...
```

And then, we collect the frequent paragraphs. The script implements the
"frequent item search in streams" algorithm in Mining Massive Datasets. Run it
as follows:
```
ansible-playbook -i hosts python.yml -e
    '{"python_script": "frequent_paragraphs.py",
      "log_file": "2019_fp_collect.log",
      "working_dir": "/mnt/data/lang/Hungarian/cc_corpus/",
      "arguments": "-L debug --index $index collect -t 0.95 -c 0.9999
                    -o $output --docs-per-batch 1000
		    --decay-filter \"score < 0.5 and count == 1\"
		    --wrap-filter \"count >= 1\" -b frequent_ps/2018_all",
      "per_host_args": {"index": "2019/index.gz", "output": "2019/frequent_ps"}}'
```

The script above doesn't exactly run as advertised: first, it collects all
paragraphs, not just the frequent ones. The reason for this is that the "stream"
doesn't start in 2019 -- we already have data from earlier years. Hence, we
bootstrap (`-b`) with the paragraph data from earlier years, and we also keep
all paragraphs so that we preserve the whole "stream" for future runs.

In order to identify the frequent paragraphs, we first merge the chunks created
by the distributed hosts:
```
merge_files.py -t pdata -o 2019/all_ps 2019/frequent_ps_host1 2019/frequent_ps_host2 ...
```

`merge_files.py` can also be used for filtering. So now that we have the file that
contains all paragraphs, we create another that lists only the frequent ones.
```
merge_files.py -t pdata -o 2019/frequent_ps 2019/all_ps -f "pdata.count >= 3 or pdata.count / float(docs) >= 0.5"
```

And we use this file to filter the frequent paragraphs from the corpus like so:
```
ansible-playbook -i hosts2 python.yml -e
    '{"python_script": "frequent_paragraphs.py",
      "log_file": "2019_fp_filter.log",
      "working_dir": "/mnt/data/lang/Hungarian/cc_corpus/",
      "arguments": "-L debug --index $index filter -t 0.95 -o $output -d 2000
                    -z 4 --frequents 2019/frequent_ps
		    --old-frequents 2018/frequent_ps",
      "per_host_args": {"index": "2019/index.gz",
                        "output": "2019/cc_corpus_hu_1500c_nofreq/"}}'
```

Finally, since the distributed script above creates as many output directories
as there are hosts (and we lost the original file structure as the documents
have been sorted by domain), we need to merge these sub-corpora into one:
```
renumber_corpus_files.py -o 2019/cc_corpus_hu_1500c_nofreq -k -Z 4 -L debug 2019/cc_corpus_hu_1500c_nofreq_*
```

(final)

### Delete duplicate paragraphs

The last form of content duplication is when a document contains a paragraph
several times. Sometimes this is valid repetition, but most of the time, it
is an artifact of (bad?) HTML page design (e.g. including the same content once
for static and once for dynamic presentation) and BeautifulSoup's inability to
cope with it.

The following script removes all duplicate paragraphs that occur in the same
document. Note that the script is fast enough, so there is no need for
distributed execution.
```
remove_same_p.py -P 12 -i 2019/cc_corpus_hu_1500c_nofreq/ -L debug remove -o 2019/cc_corpus_hu_1500c_nodup/
```

### Filtering -- again

With frequent paragraph removed, the lengths of some documents might have fallen
below the threshold (1500 characters in our case). It is therefore recommended
to run the [filtering step](#filtering) anew.

### Final steps

Let's assume that the re-filtered corpus is in the directory
`cc_corpus_hu_1500c_filtered_again`. All processing steps are effectively done;
however, in order to get the corpus ready for release, we need to run two
clean-up steps.

The first is re-sorting the files. With all the distributed processing, the
order of the files in the directory no longer reflects the sorting we imposed
on it [earlier](#frequent). The following script re-sorts the files:
```
sort_files.py -i cc_corpus_hu_1500c_filtered_again/ -t corpus
```

The second step is to make sure each file (save the last one) has the same
number of documents. Even though we started with a corpus like that, the various
filtering steps have made the document distribution uneven. The next command
"renumbers" the files, creating the final form of the corpus, with 2,500
documents per file:
```
renumber_corpus_files.py -Z 4 -d 2500 -o 2019/cc_corpus_hu_1500c_filtered_again
                         2019/cc_corpus_hu_1500c_final
```

**And we are done. Whew!**

## Text processing

The repository contains scripts to run the
[emtsv](https://github.com/dlt-rilmta/emtsv) text-processing pipeline on the
corpus.

### Installing the pipeline

The master branch of emtsv (and its dependencies) has a few problems that
needs to be fixed before it can be used efficiently. Until the fixes are merged,
the [`quntoken_v1` branch](https://github.com/DavidNemeskey/emtsv/tree/quntoken_v1)
in my fork should be used.

It fixes two issues:

- the 2.x branch of [quntoken](https://github.com/dlt-rilmta/quntoken/) is very
  slow; [my `v1` branch](https://github.com/DavidNemeskey/quntoken/tree/v1)
  fixes that by doing away with the per-function executables and providing a
  Python API based on a shared library;
- [xtsv](https://github.com/dlt-rilmta/xtsv) does not handle CoNLL-U comments
  safely; the emtsv version above uses
  [my own fork](https://github.com/DavidNemeskey/xtsv/tree/hash_mark_fix),
  which does.

The "installation" is very simple: clone the repo somewhere and execute the
steps listed in the readme.

### Running the pipeline

The `emtsv.py` script runs the pipeline. Some parts (tokenization, morphologic
analysis) are fast; the rest (morphologic disambiguation, syntactic parsing,
etc.) are slow and require lots of memory. Perhaps more importantly than any
previous step, this task should be run distributedly:
```
ansible-playbook -i hosts python.yml -e
    '{"python_script": "emtsv.py",
      "log_file": "2019_emtsv.log",
      "working_dir": "/mnt/data/lang/Hungarian/cc_corpus/",
      "arguments": "-i $input -o $output -e /home/ndavid/emtsv",
      "per_host_args": {"input": "2019/cc_corpus_hu_1500c_final/",
                        "output": "2019/cc_corpus_hu_1500c_emtsv/"}}'
```

The default tasks run are tokenization, morphological analysis and disambiguation.

### Shuffling the tsv files

With the last step complete, we have a corpus that is tokenized, morphologically
analyzed and disambiguated. The files contain the documents in the order of
their URLs, helping users find their way easily in the corpus. This is also
facilitated by CoNLL comment headers before each unit, such as document and
paragraph IDs (URL for the former), and the raw sentence texts.

However, the documents being sorted means that similar documents (those from
the same site, blog, etc.) cluster together, which can be problematic when
using the corpus as an input to a machine learning task, such as language
processing: since long batches of minibatches will have similar input, the
learning process will be biased. To circumvent this, we provide a script to
shuffle the documents in the tsv files:
```
shuffle_tsv.py 2019/cc_corpus_hu_1500c_emtsv/ -o 2019/cc_corpus_hu_1500c_shuffled/
               -d 2500 -P 8
```

Note that for best effect, this script should be run on a single machine 
(although the partial shuffling that results from distributed execution should
work fine as well).

## Type checking

Some of the code I have annotated with
[type annotations](https://docs.python.org/3/library/typing.html). To validate
the annotations, you can install [mypy](http://mypy-lang.org/) via `pip` and
run it on a file or directory. The following command also shows the recommended
switches:
```
mypy --python-version 3.5 --no-strict-optional --ignore-missing-imports scripts/frequent_paragraphs.py
```

## <a name="tech"></a>Technicalities

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

### Prerequisites

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
- _virtualenv_ (optional)

If these requirements are met, the Ansible playbook `configuration.yml` can be
used to {un}install the code -- this is a prerequisite of the next steps:
```
ansible-playbook -i hosts configuration.yml --tags {install,cleanup}
```

In order to use `emtsv.py`, [emtsv](https://github.com/dlt-rilmta/emtsv) must
be installed on the cluster. This can be done with the following commands:
```
ansible-galaxy install -r requirements.yml
ansible-playbook -i hosts configuration.yml --tags install_emtsv
```

### Python version

It is possible to set up the virtualenv to use a specific Python version
(i.e. one from Anaconda). In order to do that, specify the `path` variable on
the command line when calling the install tasks:
```
ansible-playbook -i hosts configuration.yml --tags install
-e '{"path": "/home/user/anaconda3/bin"}'
```

Note that this is **the recommended way** of configuring the virtual environments.
If `path` is specified, the script makes use of the
[venv](https://docs.python.org/3/library/venv.html) module, which _links_ the
virtual environment's Python version to the one in `path`, making (minor)
version upgrades to the Python version seamless.
[virtualenv](https://virtualenv.pypa.io/en/latest/), on the other hand, _copies_
the Python executable, which thus never gets updated when the main Python
version does, and this setup results in an inconsistent environment.
Unfortunately, when `path` is not specified, we need to revert to `virtualenv`
because of
[a bug in ansible's `pip` module](https://github.com/ansible/ansible/issues/52275).

### Data distribution

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

### Task execution

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

### Output collection

If all Slaves operate on shared storage, the output dictionary can be a
common parameter, and then all files will be written to it. If not, it must be
a per-host parameter, and the contents of the per-host output directories must
be merged by hand.

## Licence

GNU LGPL 3.0 or any later version.

If you use this program please cite the following paper:

Nemeskey, D.M.: Natural Language Processing Methods for Language Modeling. Ph.D. thesis, Eötvös Loránd University (2020)

    @PhDThesis{ Nemeskey:2020,                                                 
      author = {Nemeskey, Dávid Márk},
      title  = {Natural Language Processing Methods for Language Modeling},
      year   = {2020},
      school = {E\"otv\"os Lor\'and University}
    }
