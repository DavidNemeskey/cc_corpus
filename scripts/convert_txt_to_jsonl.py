#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exports files to JSONL format.
Operates with gz files.
If given a directory that contains subdirectories, it will process those.
"""

from argparse import ArgumentParser
from contextlib import closing
from functools import partial
import logging
import math
from multiprocessing import Pool
from pathlib import Path
import regex

from cc_corpus.utils import collect_inputs, num_digits, openall, otqdm
from cc_corpus.corpus import BatchWriter, Document

typewriter_match = regex.compile(
    r"(?:\s|^)"
    r"((?:[[:alpha:]]\s[[:alpha:]](?:\s[[:alpha:]])+(?:\s[[:punct:]])*)|"
    r"(?:[[:alpha:]]\s\s[[:alpha:]](?:\s\s[[:alpha:]])+))"
    r"(?:\s|$|[[:punct:]])"
)


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the output directory.')
    parser.add_argument('--documents', '-d', type=int, default=5000,
                        help='the maximum number of documents in the resulting '
                        'jsonl files (default: 5000).')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1).')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    if not args.input_dir.is_dir():
        parser.error('The directory for the batches must exist.')
    return args


def convert_txt_file_to_jsonl(input_file: Path, output_file: Path):
    """
    Writes a file containing documents in our format into the output as JSONL.
    """
    logging.debug(f'The current file to process: {input_file}')
    with (openall(input_file, 'rt', encoding='utf-8') as inf,
          openall('_typewriter_errors.log', 'a') as error_file):
        document = Document(id=Path(input_file).name)
        ps = [line.rstrip() for line in inf]
        if ps[0].startswith('\ufeff') or ps[0].startswith('\ufffe'):
            ps[0] = ps[0][1:]
        document.paragraphs = [p for p in ps if p]

        matches = []
        for i, p in enumerate(document.paragraphs):
            p_matches = regex.findall(typewriter_match, p)
            logging.info(f'Par "{p[:30]}" -> {len(p_matches)}')
            if p_matches:
                matches += p_matches
                for match in p_matches:
                    logging.info(f'Found {match}')
                    p = p.replace(match, match.replace(' ', ''))
                document.paragraphs[i] = p
        if matches:
            error = f'\nDocument: {document.id}, problematic strings:'
            error += ' | '.join(matches)
            error += '\n\nSuggested fix: '
            error += ' | '.join(match.replace(' ', '') for match in matches)
            print(error, file=error_file)
        logging.debug(f'Converted {input_file} {document=}.')
    return document


def export_txt_file_to_jsonl(input_file: Path, input_root_dir: Path,
                             output_root_dir: Path):
    """
    Exports a file containing documents in our format into the output as JSONL.
    Output files are put under the output_root_dir, matching the subdir and
    file name of the input file.
    """
    # The current extension is .txt.gz we need .jsonl.gz:
    rel_path = Path(input_file).relative_to(input_root_dir).with_suffix('.jsonl.gz')
    output_file = output_root_dir / rel_path
    output_dir = Path(output_file).parents[0]
    output_dir.mkdir(parents=True, exist_ok=True)
    return convert_txt_file_to_jsonl(input_file, output_file)


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    # Because utils.py#collected_inputs() cannot be given dirs which contain
    # subdirs, we have to work around this:
    input_dirs = [x for x in args.input_dir.iterdir() if x.is_dir()]
    if not input_dirs:
        input_dirs = [args.input_dir]
    input_files = sorted(collect_inputs(input_dirs))
    # The utils.collected_inputs() is still using os.path, not pathlib.
    logging.info(f'We have {len(input_files)} files to convert to JSON.')
    logging.debug(f'The files are: {input_files}.')

    zeros = num_digits(math.ceil(len(input_files) / args.documents))

    f = partial(export_txt_file_to_jsonl, input_root_dir=args.input_dir,
                output_root_dir=args.output_dir)
    with Pool(args.processes) as p:
        with closing(BatchWriter(args.documents, args.output_dir, zeros)) as bw:
            for doc in otqdm(p.imap(f, input_files),
                             'Exporting corpus...', total=len(input_files)):
                bw.write(doc, True)
        p.close()
        p.join()
    logging.info('Finished exporting files to JSONL.')


if __name__ == '__main__':
    main()
