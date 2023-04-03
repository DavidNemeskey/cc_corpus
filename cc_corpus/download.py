#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Network-related functionality."""

import logging
import time

import requests
from requests_toolbelt.multipart import decoder


class DownloadError(Exception):
    """Exception raised on if a download fails."""
    pass


def download_ranges(url: str,
                    offsets_and_lengths: list[tuple[int, int]],
                    retry_left: int) -> list[bytes]:
    """
    Downloads a list of ranges from a URL.

    :param url: the URL the byte ranges are downloaded from.
    :param offsets_and_lengths: the byte ranges to download, represented as
                                offset-lengths pairs.
    :param retry_left: the number of retries left.
    """
    logging.debug(f'Downloading {len(offsets_and_lengths)=} ranges from {url}.')
    range_str = ', '.join(f'{offset}-{offset + length}'
                          for offset, length in offsets_and_lengths)
    byte_range = f'bytes={range_str}'
    orig_retry_left = retry_left
    while retry_left > 0:
        retry_left -= 1
        retry_str = f'{retry_left} retr{"y" if retry_left == 1 else "ies"}'
        try:
            r = requests.get(
                url, headers={'Range': byte_range}, stream=True, timeout=60
            )
        except Exception as e:
            logging.exception(f'Exception {e} with URL {url}; {retry_str} left.')
            continue

        if r.status_code == 206:
            if len(offsets_and_lengths) > 1:
                try:
                    multipart_data = decoder.MultipartDecoder.from_response(r)
                    return [p.content for p in multipart_data.parts]
                except Exception as e:  # noqa
                    logging.error(f'Error while reading multipart data with '
                                  f'URL {url}: {e}; {retry_str} left.')
            else:
                return [r.content]
        elif r.status_code == 200:
            logging.error(f'Had to download {url} as {byte_range} '
                          'was not available.')
            time.sleep(orig_retry_left - retry_left)
            continue
        elif r.status_code == 404:
            logging.error(f'URL {url} not found (404).')
            return [None for _ in offsets_and_lengths]
        else:
            logging.error(f'Misc HTTP error for URL {url}: '
                          f'{r.status_code} - {r.text}; sleeping '
                          f'{orig_retry_left - retry_left}...')
            time.sleep(orig_retry_left - retry_left)
            continue
    else:
        raise DownloadError(f'Could not download ranges from URL {url}.')


def download_warc_ranges(
    warc_file_name: str,
    offsets_and_lengths: list[tuple[int, int]],
    retry_left: int
) -> list[bytes]:
    """
    Downloads byte ranges from a WARC file. A thin wrapper over
    :func:`download_ranges`.
    """
    return download_ranges(
        f'https://ds5q9oxwqwsfj.cloudfront.net/{warc_file_name}',
        offsets_and_lengths, retry_left
    )


def download_index_range(
    index_file_url: str,
    offset: int,
    length: int,
    retry_left: int
) -> bytes:
    """
    Downloads a single byte range from an index file.
    """
    return download_ranges(index_file_url, [(offset, length)], retry_left)[0]
