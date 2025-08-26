
import requests
import datetime
import re


def parse_date(date):
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
        try:
            return datetime.datetime.strptime(date, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')


def get_hostname(endpoint):
    """
    Extract hostname from the endpoint.
    Returns empty string if failed to extract.

    :return: hostname value
    """

    p = r'^(.*?://)?(?P<host>[\w.-]+).*'
    r = re.search(p, endpoint)

    return r.group('host') if r else ''

def resolve_sites():
    cric_url = "http://wlcg-cric.cern.ch/api/core/service/query/?json&type=SE&groupby=rcsite"
    r = requests.get(url=cric_url).json()
    site_protocols = {}
    for site, info in r.items():
        for se in info:
            for name, prot in se.get('protocols', {}).items():
                print(prot['endpoint'])
                site_protocols.setdefault(get_hostname(prot['endpoint']), site)

    return site_protocols
print(resolve_sites())