import requests
from bs4 import BeautifulSoup
from re import compile
from urllib.parse import urljoin
from datetime import datetime
from csv import DictReader
import os


def countries(download_flags=False):
    response = requests.get('http://football-data.co.uk/data.php')

    soup = BeautifulSoup(response.text, 'html5lib')

    flags = soup.find_all(
        'img',
        src=compile(r'/flags/[a-z]+\.gif'),
        align='right',
        a=True,
    )

    for flag in flags:
        country = {
            'name': flag.parent.parent.text.strip(),
            'url': flag.parent['href'],
        }

        if download_flags:
            country['image'] = download_image(flag['src'])

        else:
            country['image'] = flag['src'].replace('http://livescore.football-data.co.uk/', '')

        yield country


def leagues(url):
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html5lib')

    links = soup.find_all(
        'a',
        href=compile(r'\.csv'),
    )

    league_dict = {}
    for link in links:
        if link.text == 'CSV':
            continue

        filename = link['href'].split('/').pop()
        league_code = filename.replace('.csv', '')

        league_dict.setdefault(league_code, link.text)

    for code, name in league_dict.items():
        yield {'code': code, 'name': name}


def seasons(url):
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html5lib')

    files = soup.find_all(
        'a',
        href=compile(r'\.csv'),
    )

    for file in files:
        if file.find_previous_sibling('i'):
            yield {
                'league': file.text,
                'season': file.find_previous_sibling('i').text,
                'url': urljoin(url, file['href']),
            }

        elif file.text == 'CSV':
            yield {
                'league': file.text,
                'season': 'All Seasons',
                'url': urljoin(url, file['href']),
            }

        else:
            continue


def file(url):
    with requests.get(url) as response:
        timestamp = datetime.utcnow()
        print('{timestamp} - Loading file {url}'.format(timestamp=timestamp, url=url))

        reader = DictReader(
            response.iter_lines(decode_unicode=True)
        )

    for row in reader:
        yield row, timestamp


def country_code(name):
    current_dir = os.path.dirname(__file__)
    filepath = os.path.join(current_dir, 'data/fifa_country_codes.csv')

    with open(filepath) as f:
        reader = DictReader(f)

        for row in reader:
            if row['name'] == name:
                return row['code']
        else:
            raise KeyError


def download_image(url):
    response = requests.get(url)
    filename = response.url.replace('http://livescore.football-data.co.uk/', '')

    current_dir = os.path.dirname(__file__)
    filepath = os.path.join(current_dir, filename)

    with open(filepath, 'wb') as f:
        f.write(response.content)

    return filename


def fetch_last_modified(url):
    response = requests.head(url)

    return response.headers['last-modified']
