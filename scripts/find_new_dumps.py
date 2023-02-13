#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Finds and collects new (not yet downloaded) indices from common_crawla. The
new indices are then printed to the screen, or are sent to a REST endpoint.
"""

import bs4
import json
import requests
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional
import warnings


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--index_dir', '-d', type=Path, help='Directory where previous info is stored.')
    parser.add_argument('--url', '-u', type=str, help='REST API url')
    args = parser.parse_args()
    return args


# Mapping months to the corresponding two-digit numbers.
months_dict = {
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12',
    'Winter': '12',
    'Summer': '06',
    'Spring': '03',
    'Fall': '09',
}


def get_info_from_line(li: bs4.element.Tag) -> Optional[tuple]:
    """Parses the 'CC-MAIN' list item for the link."""
    if 'CC-MAIN' in li.text:
        month_year_str = li.find('a').text
        month_str, year_str = month_year_str.split(' ')
        if month_str not in months_dict:
            month_str = month_str.split('/')[0]
        month_num_str = months_dict[month_str]
        date_str = year_str + '_' + month_num_str
        index_name = 'CC-MAIN-' + li.text.split('CC-MAIN-')[1].split('/')[0].split(' ')[0]
        return date_str, index_name
    else:
        return None


def get_info_dict(index_dir: Optional[Path]) -> dict[str, str]:
    """
    Retrieves the monthly links to the 'CC-MAIN' index from the _Get started_
    page of the Common Crawl website. Only the indices not downloaded yet are
    returned.

    :param index_dir: a directory that contains the already downloaded monthly
                      indices. If specified, the indices corresponding to
                      its subdirectories (those that conform to the naming
                      scheme described below) will be excluded from the
                      returned dictionary.
    :return: a directory of the (new) monthly indices, as a mapping from
             the readily understandable ``yyyy_mm`` date format to their names
             in the Common Crawl.
    """
    if index_dir is not None:
        ind_in_dir = {path.name for path in index_dir.iterdir()}
    else:
        ind_in_dir = set()
    webpage = requests.get("https://commoncrawl.org/the-data/get-started/")
    soup = bs4.BeautifulSoup(webpage.text)

    out_dict = {}
    for div in soup.find_all('div', 'entry-content'):
        if 'ul' in div.text:
            for ul in soup.find_all('ul'):
                if 'CC-MAIN' in ul.text:
                    for idx, li in enumerate(ul.findChildren('li')):
                        info = get_info_from_line(li)
                        if info is not None:
                            date_str, index_name = info
                            if date_str not in ind_in_dir:
                                out_dict[date_str] = index_name
                    break

    return out_dict


def print_dict_to_console(info_dict: dict[str, str]):
    """
    Prints the dictionary returned by :func:`get_info_dict` to console. Used
    when no REST API URL is specified on the command line.
    """
    for key, value in info_dict.items():
        print(key, value, sep='\t')


def send_dict_to_url(url: str, info_dict: dict[str, str]):
    """
    Sends the index dictionary returned by :func:`get_info_dict` to a REST
    endpoint.
    """
    r = requests.post(url, data=json.dumps(info_dict))


def main():
    # Get rid of BeautifulSoup warnings
    warnings.filterwarnings("ignore")

    args = parse_arguments()
    info_dict = get_info_dict(args.index_dir)
    if args.url:
        send_dict_to_url(args.url, info_dict)
    else:
        print_dict_to_console(info_dict)


if __name__ == '__main__':
    main()
