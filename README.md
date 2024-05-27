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

## Docker Usage

The package includes a docker buildfile. 

```
docker run cc_corpus:latest
```

By default, the docker run command will launch the manager webapp. But you can
append any command to it, and then it will do that instead of launching the 
webapp. For example, you can launch a bash, or a specific script.

If you want to run the bash or any specific scripts, you should mount the
input and output dirs and anything else needed with the `-v` flags.

If you want to run a step which uses AWS S3 download (for example step 4), then
you should also mount the dir which has your aws certificates to 
`/home/cc/.aws` and (supposing that the permissions on your certificate files
are 600) run the image with `-u root`.

```
docker run -v $HOME/.aws:/home/cc/.aws:ro -u root cc_corpus:latest
```

To run the manager webapp from docker:
* you will need to map port 8000 to a port of your choice,
* mount a dir where you want the corpus to /data/corpus,
* mount a dir where you collect the url lists for deduplication (see Index 
Deduplication below) to /data/url_repo,
* mount a dir where you collect the minhashes for deduplication (see Document
Deduplication below) to /data/minhash_repo.
* If you want the state of your manager webapp to be persistent, you will also
need to mount a dir to /home/cc/cc_corpus/db (the sqlite DB will be stored there)

```
docker run -p 8000:8000 -v <work dir>:/data/corpus 
-v <url repository>:/data/url_repo 
-v <minhash repository>:/data/minhash_repo  
-v $HOME/.aws:/home/cc/.aws:ro  
-v <dir for the sqlite db>:/home/cc/cc_corpus/db 
-u root cc_corpus:latest
```

## Manager webapp

You can use this package either using command line commands or by the manager webapp interface.
The webapp uses a sqlite DB.

Once you have set up its configuration (see below), you can launch 
the manager webapp server with uvicorn:
```
uvicorn app.main:app
```

In the manager you can create individual steps or entire pipelines. Steps come with
default parameters which you can change. The same is true for pipelines. Once the proper
parameteres have been given to a pipeline, you can "spawn" the steps that constitute it.
The pipeline will set the parameters of those steps based on its own parameters.

### Configuring the webapp

To use the webapp you must set up a configuration file for it.

First, create it from the template:

```
cp app/config_example.yaml app/config.yaml
```

Then open it with an editor and following the instructions fill out the required fields.

### Autorunner

If you set a pipeline's status to "autorun" it flags it for being available to the autorun
feature. You can start that feature by the autorun button, and it will try to go through
all the steps of all the "autorun" flagged pipelines.

### Standalone Autorunner

The pipe_that_line.py script takes its command line parameters to create a pipeline,
spawn its corresponding steps and start the autorunner feature.

It requires the webapp to be configured properly (see above).

## Manually running the scripts

This section describes how to download and process the Common Crawl data to
arrive at a corpus. 

Note: there are several optional command line parameters beyond the ones
covered in this overview. Use the -h flag to get information about them.

Most scripts support multiprocessing. You can activate it with the -P x
command line argument, where x is the number of processes.

### Download the index

The script `get_indexfiles.py` can be used to download the index for a
specific collection. The example below downloads the index for all pages
in the `elte.hu` domain in the January (week 04) 2019 common crawl batch:
```
get_indexfiles.py -p elte.hu -c CC-MAIN-2019-04 -o 01_index/2019-04
```

### Filter the index

Next, filter the index with the script `filter_index.py`:
```
filter_index.py -i 01_index/2019-04 -o 02_index_filtered/2019-04 -a data/allowed_mimes.txt
```

This filters out invalid documents based on the metadata in the index.

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
are re-downloaded time and again (as many times as 240 in 4 month). 

If you have already downloaded previous batches, or have any other collection
of URLs, collect all the url lists into a single directory and pass it on as
the -s parameter.

If you plan to download further batches it is higly recommended to add the
url list of the current batch to that directory. You can do that by using the
--export-urls-file argument and pass along a filename for the url list.

```
deduplicate_index_urls.py -i 02_index_filtered/2019-04 -o 03_index_dedup/2019-04 
-s urls_already_collected/ -euf hu_hu_2019-04.gz -k biggest
```

If you have not exported the url list, you can extract them at any later point:
```
extract_attributes.py -i 2018/cc_corpus/ -a url -o 2018_urls.tsv.gz -P 12 2> /dev/null
```

Generally, it is better to collect the url list from the index, and not the
corpus. Because the corpus might already have been filtered (based on language, 
length, etc.), which means a lot of URLs that we have downloaded and discarded 
would no longer be included in it. So if we based our url list on the corpus we 
would re-download unnecessary documents again.

Note: unfortunately, loading the previously collected URLs takes a very long time... 
also, the script runs in a single process as Python's shared memory performance is
abysmal.

### Download pages

Common crawl offers two ways to download their corpus: via http and via Amazon's
S3 buckets. The first version is getting less and less support and since 2023 it
has been throttled down. So we use the S3 version.

This requires a valid AWS account to be configured using ENV variables with the
default names: AWS_ACCESS_KEY, AWS_SECRET_ACCES_KEY, etc.

Thanks to Amazon's support for the Common Crawl project, downloading is free.

Pages in the (filtered, deduplicated) index can be downloaded by the command
```
download_pages.py -i 03_index_dedup/2019-04 -o 04_downloaded/2019-04
--index_output_dir 04a_index_sorted/2019-04 --error_file 04b_download_errors
```

It is highly recommended to set the number of processes (-P) as high as
possible in your environment. The reason for this is that S3 does not allow 
the download of multiple slices from a single file during a single request.
This means that the number of download requests we make will be comparable
to the total number of documents we download.

### Remove boilerplate

We have to turn webscraped pages into plain text representations. We also want
to remove "boilerplate", that is text strings which are not parts of the 
document itself. For example the texts of the menus of a webpage, the legal
disclaimers, privacy and cookie policy explanations, etc.

```
remove_boilerplate.py -i 04_downloaded/2019-04 -o 05_boilerplate_removed/2019-04 
--index-dir 04a_index_sorted/2019-04 --boilerplate-tool justext
```

Note: by default the language of the filter is set to Hungarian. This determines
the stopword list used by the algorithm to detect SEO texts and such. If you work
with another language, use the -l argument with the correct language, otherwise 
your entire corpus might get removed!

The language parameter must be passed according to the naming conventions of 
Justext, so "hungarian" and not "hu".

### <a name="filtering">Filtering</a>

After boilerplate removal, the corpus is in its final format. However, to
increase its quality, we should filter out certain documents. For example those
that are not in the language we want, or those which are too short.

```
filter_corpus.py -i 06_filtered/2019-04/ -o 06_filtered_500c/2019-04/ 
-l hu -u doc -m 500c
```

### Document deduplication

Although we have filtered the index for duplicate _URLs_, the corpus might still
contain duplicate _documents_. The reason is twofold: first, we cannot filter
variations of the same URL, because there is no way to know which parameters
are important to the content (e.g. _doc_id_ probably is, while _token_ most
likely isn't); and second, the same news item / poem / etc. might have been
published on different domains. So we need document-level deduplication.

Unfortunately, deduplication is not a one-step process. 

## Generate minhashes

First, the documents
have to be <em>minhash</em>ed:
```
minhash.py -i 06_filtered_1500c/2019_04 -o 07a_minhash/2019_04 -u doc
```

There is also an option to do the deduplication based on paragraphs instead of
documents.

## Deduplicate a minhash

We now deduplicate the minhashes of the current batch:

```
lsh.py -i 07a_minhash/2019_04/ -o 07b_minhash_self/2019_04 self
```

## Deduplicate a minhash against earlier ones

If you work with a single batch, you can skip this step.

If you have earlier batches, it is important to deduplicate the current batch
against those as well. There are several scripts for that depending on the
workflow you need.

Take care that the "earlier" batch(es) must be already deduplicated both with 
themselves (see previous substep) and compared to the other earlier batches.
Otherwise, you can run into fatal errors further down the process.

Deduplicating a single new batch versus a single earlier batch:

```
lsh.py -i 07b_minhash_self/2019_04 -o 07c_minhash_full/2019_04 
-c 07c_minhash_full/2018_06 cross
```

Deduplicating a single new batch versus every earlier batch:

```
lsh.py -i 07b_minhash_self/2019_04/ -o 07c_minhash_full/2019_04 cumulative -c 07c_minhash_full/
```
This will look for the earlier batches in the directory supplied with the -c 
argument. It will parse the last part of the path numerically to determine
which batches are earlier then the current one and use only those for the
deduplication.

Under the hood this script chains together pairwise (one new vs one earlier) 
deduplications, so it rebuilds the minhash tree several times. This can make it
rather slow, but has a (relatively) low memory usage. If you don't worry about
memory usage and want a faster method, you can use the following one even if
you have only a single new batch.

Deduplicating one or more new batches versus earlier batches and each other:

```
autonomous_cross_deduplicator2.py -i 07b_minhash_self/ -o 07c_minhash_dedup/ 
-d ../com/07c_dedup -d ../ro/07c_dedup -d ../sk/07c_dedup
```
Take care that the input and the output does not point to a single batch, but to an
entire directory containing data from (possibly) multiple batches. 

This will automatically deduplicate batches against each other using the 
following two presumptions:
* Directory names follow the common crawl batch conventions and thus express
temporal relations.
* A batch that has already been deduplicated against every earlier batch has
a file named 'DONE' in its (output) folder. 
 
The script will also create these 'DONE' files as it finishes processing a batch, 
so you don't have to worry about  this if you are using only this method to 
deduplicate batches against each other. 

You can also supply further minhash directories for this script with the 
-d argument. You must pass a directory of directories with -d, and you can use
it multiple times. This is used when you have other documents in your corpus
which you want to deduplicate against. For example when we download hungarian
language files from another domain or TLD as a separate project.

This script loads the minhashes of every 'earlier' batch into memory, and then
expands it with every new batch it processes. Therefore it requires a lot of
memory, but runs faster.

## Deduplicate the documents using the deduplicated minhashes

So far we have only deduplicated the minhashes, not the corpus itself. To do so
we have to run:

```
dedup_filter.py -m 07c_minhash_dedup/2019_04 -o 07d_deduplicated/2019_04
```

### <a name="frequent">Delete frequent paragraphs</a>

This feature is currently under refactoring!

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

## Delete duplicate paragraphs

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

## Filtering -- again

With frequent paragraph removed, the lengths of some documents might have fallen
below the threshold (500 characters in our case). It is therefore recommended
to run the [filtering step](#filtering) anew.

### Final steps

At this point all steps are effectively done. However, there is some tidying up
to do.

## Re-sort files (under refactoring)

*This step, along with deduplication based on paragraphs, is currently under
refactoring.*

The first is re-sorting the files. With all the distributed processing, the
order of the files in the directory no longer reflects the sorting we imposed
on it [earlier](#frequent). The following script re-sorts the files:
```
sort_files.py -i cc_corpus_hu_1500c_filtered_again/ -t corpus
```

## Re-chunking files

The final step is to make sure each file (save the last one) has the same
number of documents. Even though we started with a corpus like that, the various
filtering steps have made the document distribution uneven. The next command
"renumbers" the files, creating the final form of the corpus, with 2,500
documents per file:
```
renumber_corpus_files.py -i 07d_dedup/2019_04/ -o 10_final/2019_04/ -d 5000
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
