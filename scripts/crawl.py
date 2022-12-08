import bs4
import json
import requests
from pathlib import Path
from typing import Union
from argparse import ArgumentParser


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input_dir', '-d', default='/home/elte-dh-clymene/dh/proba', type=Path,
                        help='Directory where previous info is stored.')
    parser.add_argument('--url', '-u', type=str, default="https://horvlu.web.elte.hu/",
                        help='REST API url')
    args = parser.parse_args()
    return args


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


def get_info_from_line(li: bs4.element.Tag) -> Union[tuple, None]:
    if "CC-MAIN" in li.text:
        month_year_str = li.find("a").text
        month_str, year_str = month_year_str.split(" ")
        if month_str not in months_dict:
            month_str = month_str.split("/")[0]
        month_num_str = months_dict[month_str]
        date_str = year_str + "_" + month_num_str
        index_name = "CC-MAIN-" + li.text.split("CC-MAIN-")[1].split("/")[0].split(" ")[0]
        return date_str, index_name
    else:
        return None


def get_info_dict(input_dir: Path) -> dict:  # Optinal[path]
    ind_in_dir = {path.name for path in input_dir.iterdir()}
    webpage = requests.get("https://commoncrawl.org/the-data/get-started/")
    soup = bs4.BeautifulSoup(webpage.text, features="html5lib")

    out_dict = {}
    for ul in soup.find_all("ul"):
        if "CC-MAIN" in ul.text:
            for idx, li in enumerate(ul.findChildren('li')):
                info = get_info_from_line(li)
                if info is not None:
                    date_str, index_name = info
                    if date_str not in ind_in_dir:
                        out_dict[date_str] = index_name
            break

    print(out_dict)
    return out_dict


def send_dict_to_url(url: str, info_dict: dict) -> None:
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, data=json.dumps(info_dict), headers=headers)
    # r = requests.post('https://echo.zuplo.io/', json=info_dict)


def main():
    args = parse_arguments()
    info_dict = get_info_dict(args.input_dir)
    send_dict_to_url(args.url, info_dict)


if __name__ == '__main__':
    main()
