#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Converts a corpus in the tsv format to other formats (such as BERT's input
format).
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Pool
import os
import os.path as op
import re
from typing import Dict
import unicodedata

from multiprocessing_logging import install_mp_handler

from cc_corpus.tsv import clean_xpostag, parse_file, Sentence
from cc_corpus.utils import collect_inputs, openall
from cc_corpus.wordpiece import WordpieceTokenizer


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories of tsv files.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--lower', '-l', action='store_true',
                        help='lowercase the text.')
    parser.add_argument('--normalize', '-n',
                        choices=['nfc', 'nfd', 'nfkc', 'nfkd'],
                        help='normalize the text to a unicode normalization '
                             'form.')
    parser.add_argument('--token', '-t', default='form',
                        help='value of the output tokens. Possible values '
                             'include a field name (e.g. "form", which uses '
                             'the surface form of the word; "glf", the '
                             '"gluten-free" form (i.e. the lemma and '
                             'inflectional suffixes separated); and "text". '
                             'which takes the original, untokenized sentences.')
    parser.add_argument('--wordpiece-vocab', '-v',
                        help='a wordpiece (BPE, etc.) vocabulary file in the '
                             'BERT vocab.txt format. If specified, the tokens '
                             'are further tokenized with it. Note that this is '
                             'not required if the output is for BERT (and '
                             'related) models, as they perform the '
                             'tokenization themselves. However, regular '
                             'word-based LMs might benefit from this step. '
                             'Note that this option requires the transformers '
                             'library.')
    parser.add_argument('--output-format', '-f', choices=['bert', 'lm'],
                        default='bert',
                        help='possible output formats. "bert" prints sentences '
                             'on separate lines; "lm" paragraphs. Both put an '
                             'empty line between documents, but "lm" also '
                             'starts each document with a <newdoc> token.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()

    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


class TokenExtractor:
    """Extracts tokens from a sentence."""
    def __init__(self, lower: bool = False, norm_form: str = None):
        """
        :param lower: whether the text should be lower cased.
        :param norm_form: the unicode normalization form.
        """
        self.lower = lower
        self.norm_form = partial(
            unicodedata.normalize, norm_form.upper()) if norm_form else None

    def normalize(self, text):
        """Normalizes / lowercases _text_."""
        if self.lower:
            text = text.lower()
        if self.norm_form:
            text = self.norm_form(text)
        return text

    def tokenize(self, sentence: Sentence):
        """Extracts output tokens from a sentence."""
        raise NotImplementedError(
            f'tokenize() not implemented in {self.__class__.__name__}')


class FieldExtractor(TokenExtractor):
    """Extracts a field with the specified name."""
    def __init__(self, field: str, fields: Dict[str, int],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.idx = fields[field]
        except KeyError:
            raise ValueError(f'Field {field} does not exist in this file.')

    def tokenize(self, sentence: Sentence):
        return [self.normalize(token.split('\t', self.idx + 1)[self.idx])
                for token in sentence]


class GLFExtractor(TokenExtractor):
    """
    Extracts the gluten-free (GLF) tokens: the lemma, some derivational
    suffixes and inflectional suffixes.
    """
    def __init__(self, fields: Dict[str, int], *args, **kwargs):
        """:param fields: the field name -> id mapping."""
        super().__init__(*args, **kwargs)
        self.tagp = re.compile(r'\[[^]]+\]')
        self.punct_tags = {'[Hyph:Dash]', '[Punct]', '[Hyph:Slash]'}
        try:
            self.lemma_idx = fields['lemma']
            self.xpostag_idx = fields['xpostag']
        except KeyError:
            raise ValueError('Both the lemma and xpostag columns are required.')

    def tokenize(self, sentence: Sentence):
        ret = []
        for token in sentence:
            fields = token.split('\t')
            lemma = fields[self.lemma_idx]
            xpostag = clean_xpostag(fields[self.xpostag_idx])
            tags = [tag.group() for tag in self.tagp.finditer(xpostag)
                    if tag.group() != '[Nom]']
            last_slash = -1
            for i, tag in enumerate(tags):
                if tag[1] != '/' and tag not in self.punct_tags:
                    break
                last_slash = i
            # There should be at least one POS category-related tag
            if last_slash == -1:
                raise ValueError(f'No / tag for word {lemma}/{xpostag} in '
                                 f'{sentence}')
            # Remove the . from 1. or XIII.
            if '[_Ord/Adj]' in tags[last_slash:] and lemma.endswith('.'):
                lemma = lemma[:-1]
            tags[last_slash] = self.normalize(lemma)
            ret.extend(tags)
        return ret


class TextExtractor(TokenExtractor):
    """Extracts tokens from the # text comment."""
    def tokenize(self, sentence: Sentence):
        if sentence.comment.startswith('# text = '):
            return self.normalize(sentence.comment[9:]).split()


def process_file(input_file: str, output_dir: str, token_type: str,
                 output_format: str, lower_case: bool = False,
                 norm_form: str = None, vocab_file: str = False):
    """
    Converts _input_file_ from tsv to the BERT input format.

    :param input_file: the input file.
    :param output_dir: the output directory; the output file will be created
                       here, with the same name as _input_file_ (except any
                       `tsv` in its name is replaced with `txt`).
    :param token_type: the token type; see the argument description, above.
    :param output_format: see the argument description, above.
    :param lower_case: lowercase the text?
    :param norm_form: the unicode normalization form. If ``None``, no
                      normalization is performed.
    :param vocab: a wordpiece vocabulary file.
    """
    output_file = op.join(output_dir, op.basename(input_file).replace('tsv', 'txt'))
    logging.debug(f'Converting {input_file} to {output_file}...')
    if vocab_file:
        wordpiece = WordpieceTokenizer(
            vocab_file=vocab_file,
            unk_token=('[UNK]' if output_format == 'bert' else '<unk>')
        )
    else:
        wordpiece = None

    with openall(output_file, 'wt') as outf:
        input_it = parse_file(input_file)
        fields = {field: i for i, field in enumerate(next(input_it))}
        lm_format = (output_format == 'lm')
        eol = '' if lm_format else '\n'

        if token_type == 'text':
            token_extractor = TextExtractor(lower_case, norm_form)
        elif token_type == 'glf':
            token_extractor = GLFExtractor(fields, lower_case, norm_form)
        else:
            token_extractor = FieldExtractor(token_type, fields,
                                             lower_case, norm_form)

        for document in input_it:
            if lm_format:
                print('\n<newdoc>\n', file=outf)
            for paragraph in document:
                for sid, sentence in enumerate(paragraph):
                    try:
                        tokens = token_extractor.tokenize(sentence)
                    except:
                        logging.exception(f'Error in sentence {sentence}')
                    if wordpiece:
                        tokens = wordpiece.tokenize(' '.join(tokens))
                    if lm_format and sid:
                        print(' ', end='', file=outf)
                    print(' '.join(tokens), end=eol, file=outf)
                if lm_format:
                    print(file=outf)
            if not lm_format:
                print(file=outf)
    logging.debug(f'Converted {input_file} to {output_file}.')


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not op.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    input_files = sorted(collect_inputs(args.inputs))
    logging.info('Scheduled {} files for conversion.'.format(len(input_files)))

    with Pool(args.processes) as pool:
        f = partial(process_file,
                    output_dir=args.output_dir,
                    token_type=args.token.lower(),
                    output_format=args.output_format.lower(),
                    lower_case=args.lower,
                    norm_form=args.normalize,
                    vocab_file=args.wordpiece_vocab)
        res = pool.map_async(f, input_files)
        res.get()
        pool.close()
        pool.join()

    logging.info('Done.')


if __name__ == '__main__':
    main()
