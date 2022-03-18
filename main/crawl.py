import requests
from re import compile

SITE = 'http://football-data.co.uk/'


def find_countries():
    pattern = r'HREF="([a-z]+m\.php)"'
    regex = compile(pattern)
    response = requests.get(SITE + 'data.php')

    for page in regex.findall(response.text):
        yield page


def find_files(page):
    pattern = r'HREF="(?P<file>mmz4281/(?P<season>\d{4})/(?P<league>(?P<country>[A-Z]+)(?P<tier>\w))\.csv)"'
    regex = compile(pattern)
    
    print(SITE + page)
    response = requests.get(SITE + page)

    for match in regex.finditer(response.text):
        print('+ ' + SITE + match.group('file'))
        yield SITE + match.group('file'), match.group('season'), match.group('league'), match.group('country'), match.group('tier')
            

def find_extra_countries():
    pattern = r'href="([a-z]+\.php)"'
    regex = compile(pattern)
    response = requests.get(SITE + 'all_new_data.php')

    for page in regex.findall(response.text):
        yield page


def find_extra_files(page):
    pattern = r'<A HREF="(?P<file>new/(?P<league>(?P<country>[A-Z]+))\.csv)">CSV</A>'
    regex = compile(pattern)
    
    print(SITE + page)
    response = requests.get(SITE + page)

    for match in regex.finditer(response.text):
        print('+ ' + SITE + match.group('file'))
        yield SITE + match.group('file'), match.group('league'), match.group('country')


def fetch_last_modified(url):
    response = requests.head(url)
    
    return response.headers['last-modified']
