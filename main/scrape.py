import requests
from bs4 import BeautifulSoup
from re import compile
from urllib.parse import urljoin


def available_countries(download_flags=False):
    response = requests.get('http://football-data.co.uk/data.php')

    soup = BeautifulSoup(
        response.text,
        'html5lib',
    )

    flags = soup.find_all(
        'img',
        src=compile(r'/flags/[a-z]+\.gif'),
        align='right',
        a=True,
    )

    countries = []
    for flag in flags:
        country = {
            'name': flag.parent.parent.text.strip(),
            'url': flag.parent['href'],
        }

        if download_flags:
            country['image'] = download_image(flag['src'])

        else:
            country['image'] = flag['src'].replace('http://livescore.football-data.co.uk/', '')

        countries.append(country)

    return countries


def available_leagues(url):
    response = requests.get(url)

    soup = BeautifulSoup(
        response.text,
        'html5lib',
    )

    links = soup.find_all(
        'a',
        href=compile(r'\.csv'),
    )

    leagues = {}
    for link in links:
        if link.text == 'CSV':
            continue

        filename = link['href'].split('/').pop()
        league_code = filename.replace('.csv', '')

        leagues.setdefault(league_code, link.text)

    return [{'name': name, 'code': code} for code, name in leagues.items()]


def available_seasons(url):
    response = requests.get(url)

    soup = BeautifulSoup(
        response.text,
        'html5lib',
    )

    seasons = soup.find_all(
        'i',
        text=compile('Season'),
    )

    return [season.text for season in seasons]


def available_files(url):
    response = requests.get(url)

    soup = BeautifulSoup(
        response.text,
        'html5lib',
    )

    links = soup.find_all(
        'a',
        href=compile(r'\.csv'),
    )

    files = []
    for link in links:
        if link.find_previous_sibling('i'):
            file = {
                'league': link.text,
                'season': link.find_previous_sibling('i').text,
                'url': urljoin(url, link['href']),
            }

        elif link.text == 'CSV':
            file = {
                'league': link.text,
                'season': 'All Seasons',
                'url': urljoin(url, link['href']),
            }

        else:
            continue

        files.append(file)

    return files


def download_image(url):
    response = requests.get(url)
    filename = response.url.replace('http://livescore.football-data.co.uk/', '')

    with open(filename, 'wb') as f:
        f.write(response.content)

    return filename


def fetch_last_modified(url):
    response = requests.head(url)

    return response.headers['last-modified']
