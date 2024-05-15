#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Network-related functionality."""

import logging
import time
from typing import Any, Union

import requests
from requests_toolbelt.multipart import decoder


WARC_BASE_URI = 'https://ds5q9oxwqwsfj.cloudfront.net/'
BUCKET_NAME = 'commoncrawl'


class DownloadError(Exception):
    """Exception raised on if a download fails."""
    pass


def download_ranges(url_or_key: str,
                    offsets_and_lengths: list[tuple[int, int]],
                    retry_left: int,
                    delay_period: int = 1,
                    s3_download: bool = False,
                    session: Any = None) -> Union[bytes, list[bytes]]:
    """
    Downloads a list of ranges from a URL.

    :param url_or_key: the URL or S3 object key of the source.
    :param offsets_and_lengths: the byte ranges to download, represented as
                                offset-lengths pairs.
    :param retry_left: the number of retries left.
    :param delay_period: the base time unit for delay in seconds.
    :param s3_download: whether to download via S3 or http.
    :param session: S3 session to use.
    """
    if s3_download and (len(offsets_and_lengths) > 1):
        logging.error(f"S3 does not support multiple ranges in a single"
                      f"get request.")
        raise ValueError("More than one range requested for S3 download.")
    logging.debug(
        f'Downloading {len(offsets_and_lengths)=} ranges from {url_or_key}.')

    if s3_download and not session:
        logging.error("S3 Session object is required for S3 download")
        raise ValueError("S3 Session object is required for S3 download")

    range_str = ', '.join(f'{offset}-{offset + length}'
                          for offset, length in offsets_and_lengths)
    byte_range = f'bytes={range_str}'

    orig_retry_left = retry_left
    while retry_left > 0:
        retry_left -= 1
        retry_str = f'{retry_left} retr{"y" if retry_left == 1 else "ies"}'
        try:
            if s3_download:
                r = session.get_object(
                    Bucket=BUCKET_NAME,
                    Key=url_or_key,
                    Range=byte_range,
                )
            else:
                r = requests.get(url_or_key,
                                 headers={'Range': byte_range},
                                 stream=True,
                                 timeout=60)
        except Exception as e:
            logging.exception(
                f'Exception {e} with URL {url_or_key}; {retry_str} left.')
            continue

        if s3_download:
            return r['Body'].read()
        else:
            if r.status_code == 206:
                if len(offsets_and_lengths) > 1:
                    try:
                        multipart_data = decoder.MultipartDecoder.from_response(r)
                        return [p.content for p in multipart_data.parts]
                    except Exception as e:  # noqa
                        logging.error(f'Error while reading multipart data with '
                                      f'URL {url_or_key}: {e}; {retry_str} left.')
                else:
                    return [r.content]
            elif r.status_code == 200:
                logging.error(f'Had to download {url_or_key} as {byte_range} '
                              'was not available.')
                time.sleep((orig_retry_left - retry_left) * delay_period)
                continue
            elif r.status_code == 404:
                logging.error(f'URL {url_or_key} not found (404).')
                return [None for _ in offsets_and_lengths]
            else:
                logging.error(f'Misc HTTP error for URL {url_or_key}: '
                              f'{r.status_code} - {r.text}; sleeping '
                              f'{orig_retry_left - retry_left}...')
                time.sleep((orig_retry_left - retry_left) * delay_period)
                continue
    else:
        raise DownloadError(f'Could not download ranges from {url_or_key}.')


def download_warc_ranges(
        warc_file_name: str,
        offsets_and_lengths: list[tuple[int, int]],
        retry_left: int,
        delay: int = 10,
        session: Any = None,
) -> list[bytes]:
    """
    Downloads byte ranges from a WARC file. A thin wrapper over
    :func:`download_ranges`.
    """
    return download_ranges(
        warc_file_name,
        offsets_and_lengths, retry_left,
        delay_period=delay,
        s3_download=True,
        session=session,
    )


def download_index_range(
        index_file_url: str,
        offset: int,
        length: int,
        retry_left: int,
        delay: int = 10,
        session: Any = None,
) -> bytes:
    """
    Downloads a single byte range from an index file.
    """
    return download_ranges(
        index_file_url,
        [(offset, length)],
        retry_left,
        delay_period=delay,
        s3_download=True,
        session=session
    )[0]
