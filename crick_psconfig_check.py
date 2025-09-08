""" maps ips to sites """

import requests
import socket
import ipaddress
import psconfig.api
from pymemcache.client import base

# suppress InsecureRequestWarning: Unverified HTTPS request is being made.
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

PerfSonars = {}

print('loading pwa nodes')
psc = psconfig.api.PSConfig('https://psconfig.aglt2.org/pub/config',
                            hostcert=None, hostkey=None, verify=False)
prod_hosts = psc.get_all_hosts()
print(f'found {len(prod_hosts)} production hosts in PWA:')
print(prod_hosts)

def get_subnets_mapping(data):
    subnets_mapping = {}
    for _, site in data.items():
        if 'netroutes' not in site.keys():
            continue
        for netroute, nets in site["netroutes"].items():
            subnets = nets["networks"].get("ipv4", []) + nets["networks"].get("ipv6", [])
            for subnet in subnets:
                try:
                    cidr_subnet = ipaddress.ip_network(subnet)
                except ValueError:
                    print(f'Subnet {subnet} is not a subnet')
                    continue
                subnets_mapping[cidr_subnet] = netroute
    return subnets_mapping


print('loading net sites')
netsites = requests.get(
    'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json', verify=False).json()
netsites_mapping = get_subnets_mapping(netsites)

client = base.Client(('memcached', 11211))


class ps:
    def __init__(self, hostname):
        self.VO = []
        self.sitename = []
        self.hostname = hostname
        self.rcsite = ''
        self.netsite = ''
        self.flavor = ''
        self.production = False
        if self.hostname in prod_hosts:
            self.production = True

    def __str__(self):
        s = f'sitename:{self.sitename} host:{self.hostname} prod:{self.production}'
        s += f' flavor:{self.flavor} rcsite:{self.rcsite} vo:{self.VO} netsite:{self.netsite}'
        return s


def request(url, hostcert=None, hostkey=None, verify=False):
    if hostcert and hostkey:
        req = requests.get(url, verify=verify, timeout=120,
                           cert=(hostcert, hostkey))
    else:
        req = requests.get(url, timeout=120, verify=verify)
    req.raise_for_status()
    return req.content


def get_ip(host):

    ip4, ip6 = None, None

    try:
        host_addr = socket.getaddrinfo(host, 80, 0, 0, socket.IPPROTO_TCP)
    except socket.gaierror:
        print(f'Unable to resolve {host}')
        return ip4, ip6

    for family, _, _, _, sockaddr in host_addr:
        if family == socket.AF_INET:
            ip4 = ipaddress.ip_address(sockaddr[0])
        elif family == socket.AF_INET6:
            ip6 = ipaddress.ip_address(sockaddr[0])
    return ip4, ip6


def get_netsite(ip_address):
    for subnet in netsites_mapping.keys():
        if ip_address in subnet:
            return netsites_mapping[subnet]
    return ''


def reload():
    print(" --- getting PerfSonars from WLCG CRIC ---")
    r = requests.get(
        'https://wlcg-cric.cern.ch/api/core/service/query/?json&state=ACTIVE&type=PerfSonar',
        verify=False
    )
    res = r.json()
    for _key, val in res.items():
        if not val['endpoint']:
            print('no hostname? should not happen:', val)
            continue
        p = ps(val['endpoint'])

        try:
            p.flavor = val.get('flavour', 'unknown')
            p.rcsite = val.get('rcsite', "unknown")
            (ip4, ip6) = get_ip(p.hostname)
            if ip4:
                p.netsite = get_netsite(ip4)
            if not p.netsite:
                p.netsite = p.rcsite
            usage = val.get('usage', {})
            if usage:
                for exp in usage:
                    p.VO.append(exp)
                    p.sitename.append(usage[exp][0]['site'])
            else:
                p.VO.append('unknown')
                p.sitename.append('unknown')

            client.set('vo_'+p.hostname, ','.join(p.VO))
            client.set('sitename_'+p.hostname, ','.join(p.sitename))
            client.set('rcsite_'+p.hostname, p.rcsite)
            client.set('netsite_'+p.hostname, p.netsite)
            client.set('production_'+p.hostname, p.production)

            PerfSonars[p.hostname] = p
        except AttributeError as e:
            print('attribute missing.', e)
        print(p)

    print(len(PerfSonars.keys()), 'perfsonars reloaded.')


if __name__ == "__main__":
    reload()
