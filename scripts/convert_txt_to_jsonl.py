#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exports files to JSONL format.
Operates with gz files.
If given a directory that contains subdirectories, it will process those.
"""

from argparse import ArgumentParser
from functools import partial
from itertools import chain
import logging
from multiprocessing import Pool
from pathlib import Path
import regex

from cc_corpus.utils import collect_inputs, consume, openall, otqdm
from cc_corpus.corpus import Document

typewriter_match = regex.compile(r"(?:\s|^)([[:alpha:]]\s[[:alpha:]](?:\s[[:alpha:]])+)(?:\s|$|[[:punct:]])")

def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the output directory.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
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
    with openall(input_file, 'rt', encoding='utf-8') as inf\
            , openall(output_file, 'wt', encoding='utf-8') as outf\
            , openall('_typewriter_errors.log', 'a') as error_file:
        document = Document(id=Path(input_file).name)
        document.paragraphs = [line.rstrip() for line in inf]

        matches = []
        for i, p in enumerate(document.paragraphs):
            match = regex.findall(typewriter_match, p)
            if match:
                matches += match
                for m in match:
                    document.paragraphs[i] = p.replace(m, m.replace(' ', ''))
        if matches:
            error = f'\nDocument: {document.id}, problematic strings:'
            error += ' | '.join(matches)
            error += '\n\nSuggested fix: '
            error += ' | '.join(match.replace(' ', '') for match in matches)
            print(error, file=error_file)
        print(document.to_json(), file=outf)
        logging.debug(f'Completed exporting to {output_file} as JSON')


def export_txt_file_to_jsonl(input_file: Path, input_root_dir: Path,
                             output_root_dir: Path):
    """
    Exports a file containing documents in our format into the output as JSONL.
    Output files are put under the output_root_dir, matching the subdir and
    file name of the input file.
    """
    logging.debug(f'The current file to process: {input_file}')
    # The current extension is .txt.gz we need .jsonl.gz:
    rel_path = Path(input_file).relative_to(input_root_dir).with_suffix('.jsonl.gz')
    output_file = output_root_dir / rel_path
    output_dir = Path(output_file).parents[0]
    output_dir.mkdir(parents=True, exist_ok=True)
    convert_txt_file_to_jsonl(input_file, output_file)


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
    input_files = collect_inputs(input_dirs)
    # The utils.collected_inputs() is still using os.path, not pathlib.
    logging.info(f'We have {len(input_files)} files to convert to JSON.')
    logging.debug(f'The files are: {input_files}.')

    f = partial(export_txt_file_to_jsonl, input_root_dir=args.input_dir,
                output_root_dir=args.output_dir)
    with Pool(args.processes) as p:
        consume(otqdm(p.imap_unordered(f, input_files),
                      'Exporting corpus...', total=len(input_files)))
        p.close()
        p.join()
    logging.info('Finished exporting files to JSONL.')


if __name__ == '__main__':
    main()
