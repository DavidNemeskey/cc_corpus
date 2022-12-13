"""
Find and collect new indexes from common_crawl
"""

import bs4
import json
import requests
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input_dir', '-d', type=Path, help='Directory where previous info is stored.')
    parser.add_argument('--url', '-u', type=str, help='REST API url')
    args = parser.parse_args()
    return args


"""
Pairing months by number.
"""
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
    """
    Specify 'CC-MAIN' structure
    """
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


def get_info_dict(input_dir: Optional[Path]) -> dict:  # Optinal[path]
    """
    Retrieve 'CC-MAIN' from the website
    """
    if input_dir is not None:
        ind_in_dir = {path.name for path in input_dir.iterdir()}
    else:
        ind_in_dir = set()
    webpage = requests.get("https://commoncrawl.org/the-data/get-started/")
    soup = bs4.BeautifulSoup(webpage.text, features="html5lib")
    print(soup)

    out_dict = {}
    for div in soup.find_all('div'):
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


def help_func():
    webpage = requests.get('https://commoncrawl.org/the-data/get-started/')
    soup = bs4.BeautifulSoup(webpage.text, features='html5lib')
    for ul in soup.find_all('ul'):
        if 'CC-MAIN' in ul.text:
            for idx, li in enumerate(ul.findChildren('li')):
                info = get_info_from_line(li)
                if info is not None:
                    date_str, index_name = info
                    print(date_str, index_name)
            break


def send_dict_to_url(url: str, info_dict: dict) -> None:
    r = requests.post(url, data=json.dumps(info_dict))


def main():
    args = parse_arguments()
    info_dict = get_info_dict(args.input_dir)
    if args.url:
        send_dict_to_url(args.url, info_dict)
    else:
        help_func()


if __name__ == '__main__':
    main()
