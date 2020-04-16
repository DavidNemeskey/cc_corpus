#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Code from the BERT repo (https://github.com/google-reserach/bert).
Copied here so that the user doesn't need to install the whole package for
wordpiece tokenization.
"""

import collections

class WordpieceTokenizer(object):
    """Runs WordPiece tokenization."""

    def __init__(self, vocab=None, vocab_file=None,
                 unk_token='<unk>', max_input_chars_per_word=100):
        if vocab and vocab_file:
            raise ValueError('Only one of vocab and vocab_file can be specified')
        if not (vocab or vocab_file):
            raise ValueError('One of vocab or vocab_file must be specified.')
        self.vocab = vocab if vocab else self.load_vocab(vocab_file)
        self.unk_token = unk_token
        self.max_input_chars_per_word = max_input_chars_per_word

    def load_vocab(self, vocab_file):
        """Loads a vocabulary file into a dictionary."""
        vocab = collections.OrderedDict()
        with open(vocab_file, 'rt', encoding='utf-8') as reader:
            for index, token in enumerate(map(str.strip, reader)):
                vocab[token] = index
        return vocab

    def tokenize(self, text):
        """Tokenizes a piece of text into its word pieces.

        This uses a greedy longest-match-first algorithm to perform tokenization
        using the given vocabulary.

        For example:
          input = "unaffable"
          output = ["un", "##aff", "##able"]

        Args:
          text: A single token or whitespace separated tokens. This should have
            already been passed through `BasicTokenizer`.

        Returns:
          A list of wordpiece tokens.
        """

        output_tokens = []
        for token in text.strip().split():
            chars = list(token)
            if len(chars) > self.max_input_chars_per_word:
                output_tokens.append(self.unk_token)
                continue

            is_bad = False
            start = 0
            sub_tokens = []
            while start < len(chars):
                end = len(chars)
                cur_substr = None
                while start < end:
                    substr = "".join(chars[start:end])
                    if start > 0:
                        substr = "##" + substr
                    if substr in self.vocab:
                        cur_substr = substr
                        break
                    end -= 1
                if cur_substr is None:
                    is_bad = True
                    break
                sub_tokens.append(cur_substr)
                start = end

            if is_bad:
                output_tokens.append(self.unk_token)
            else:
                output_tokens.extend(sub_tokens)
        return output_tokens
