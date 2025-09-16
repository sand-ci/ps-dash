import json
from elasticsearch.helpers import scan
import utils.helpers as hp
from datetime import datetime, timedelta
from ipaddress import ip_address, ip_network
import pandas as pd

def extract_addresses_cric():
    # Load the JSON file (replace with the actual path to cric.json)
    with open("cric.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract relevant parts
    header = data["header"]
    body = data["body"]

    # Find the index positions for the columns we need
    site_index = header.index("NetworkRoute")
    subnets_index = header.index("Subnets")
    lhcopn_limit_index = header.index("LHCOPN limit")

    # Collect subnets where LHCOPN limit > 0
    site_subnets = {}
    for row in body:
        try:
            limit = int(row[lhcopn_limit_index])
        except ValueError:
            continue  # skip if not a number

        if limit > 0:
            site_name = row[site_index].strip()
            subnets = [s.strip() for s in row[subnets_index].split(",")]
            site_subnets[site_name] = subnets

    return site_subnets
# print(extract_addresses_cric().keys())

T1_NETSITES = [
    "BNL-ATLAS-LHCOPNE", "USCMS-FNAL-WC1-LHCOPNE", "RAL-LCG2-LHCOPN",
    "RRC-KI-T1-LHCOPNE", "JINR-T1-LHCOPNE", "NCBJ-LHCOPN",
    "NLT1-SARA-LHCOPNE", "INFN-T1-LHCOPNE", "NDGF-T1-LHCOPNE",
    "KR-KISTI-GSDC-1-LHCOPNE", "IN2P3-CC-LHCOPNE", "pic-LHCOPNE",
    "FZK-LCG2-LHCOPNE", "CERN-PROD-LHCOPNE", "TRIUMF-LCG2-LHCOPNE", 'TW-ASGC'
]


cric_elastic_mapping = {'CERN-PROD-LHCOPNE': 'CERN-PROD-LHCOPNE', 'BNL-ATLAS-LHCOPNE': 'BNL-ATLAS-LHCOPNE', 
                        'INFN-T1-LHCOPNE': 'INFN-T1-LHCOPNE', 'USCMS-FNAL-WC1-LHCOPNE': 'USCMS-FNAL-WC1-LHCOPNE', 
                        'FZK-LCG2-LHCOPNE': 'FZK-LCG2-LHCOPNE', 'IN2P3-CC-LHCOPNE': 'IN2P3-CC-LHCOPNE', 'RAL-LCG2-LHCOPN': 'RAL-LCG2-LHCOPN', 
                        'JINR-T1-LHCOPNE': 'JINR-T1-LHCOPNE', 'PIC-LHCOPNE': 'pic-LHCOPNE', 'RRC-KI-T1-LHCOPNE': 'RRC-KI-T1-LHCOPNE', 
                        'NLT1-SARA-LHCOPNE': 'NLT1-SARA-LHCOPNE', 'TRIUMF-LCG2-LHCOPNE': 'TRIUMF-LCG2-LHCOPNE', 'NDGF-T1-LHCOPNE': 'NDGF-T1-LHCOPNE', 
                        'KR-KISTI-GSDC-1-LHCOPNE': 'KR-KISTI-GSDC-1-LHCOPNE', 'NCBJ-LHCOPN': 'NCBJ-LHCOPN', 'TW-ASGC': 'Taiwan-LCG2-LHCOPNE'}

def query_records(date_from_str, date_to_str, allowed_sites=None):
    if allowed_sites is None:
        allowed = chosen = T1_NETSITES
    else:
        chosen = set(allowed_sites)
        allowed = set(T1_NETSITES)

    q = {
        "bool": {
            "filter": [
                {
                    "range": {
                        "timestamp": {
                            "gte": date_from_str,
                            "lte": date_to_str,
                            "format": "strict_date_optional_time"
                        }
                    }
                },
                # both ends must be in 'allowed'
                {"terms": {"src_netsite": list(allowed)}},
                {"terms": {"dest_netsite": list(allowed)}},
            ]
        }
    }

    try:
        es_resp = hp.es.search(index='ps_trace', query=q, size=10000)
        data = es_resp['hits']['hits']
    except Exception as exc:
        print(f"Failed to query ps_trace: {exc}")
        data = []
    # return data
    extracted = []
    for item in data:
            src_ipv6 = item['_source'].get('source', {}).get('ipv6')
            dst_ipv6 = item['_source'].get('destination', {}).get('ipv6')
            src_ipv4 = item['_source'].get('source', {}).get('ipv4')
            dst_ipv4 = item['_source'].get('destination', {}).get('ipv4')
            src_ipvs = [src_ipv6, src_ipv4]
            dst_ipvs = [dst_ipv4, dst_ipv6]
            extracted.append({
                'src_netsite': (item['_source'].get('src_netsite')).upper(),
                'dest_netsite': (item['_source'].get('dest_netsite')).upper(),
                'src_host': item['_source'].get('src_host'),
                'dest_host': item['_source'].get('dest_host'),
                'destination_reached': item['_source'].get('destination_reached'),
                'path_complete': item['_source'].get('path_complete'),
                'src_ipvs': src_ipvs,
                'dst_ipvs': dst_ipvs,
                'created_at': item['_source'].get('created_at')
            })
    return extracted


def ip_in_any(ip, networks):
    # print(f"ip: {ip}")
    # print(f"network: {networks}")
    return any(ip_address(ip) in ip_network(net) for net in networks)

# def query_valid_trace_data(hours):
#     """
#     As some tests are run on not perfsonar hosts we are getting wrong image about trace data. 
#     This function extracts data only for OPN/T1 sites from Elasticsearch,
#     and filters the data to return tests for OPN ip addresses mentioned in CRIC.
#     """
#     date_to = datetime.utcnow()
#     date_from = date_to - timedelta(hours=hours)
#     date_to_str = date_to.strftime(hp.DATE_FORMAT)
#     date_from_str = date_from.strftime(hp.DATE_FORMAT)
#     traceroute_records = query_records(date_from_str, date_to_str, allowed_sites=T1_NETSITES)
#     cric_T1_networks = extract_addresses_cric()
#     valid_traceroutes = []
#     invalid_traceroutes = []
#     for record in traceroute_records:
#         # print(record)
#         srcs = record['src_ipvs']
#         dsts = record['dst_ipvs']
#         src_cric_info = cric_T1_networks[cric_elastic_mapping[record['src_netsite']]]
#         dest_cric_info = cric_T1_networks[cric_elastic_mapping[record['dest_netsite']]]
        
#         opn_ips_srcs = [ip_in_any(ip, src_cric_info) for ip in srcs if ip != '']
#         opn_ips_dsts = [ip_in_any(ip, dest_cric_info) for ip in dsts if ip != '']
#         # print(opn_ips_srcs, opn_ips_dsts)
#         if any(opn_ips_srcs) and any(opn_ips_dsts):
#             valid_traceroutes.append(record)
#         else:
#             invalid_traceroutes.append(record)
#     with open('invalid_ips_perfsonar.json', 'w') as f:
#         json.dump(invalid_traceroutes, f)
#     print(invalid_traceroutes)
#     print(f"Before filtering: {len(traceroute_records)}")
#     print(f"After filtering: {len(valid_traceroutes)}")
#     return valid_traceroutes

from datetime import datetime, timedelta
from collections import defaultdict
import json


def query_valid_trace_data(hours):
    """
    As some tests are run on not perfsonar hosts we are getting wrong image about trace data. 
    This function extracts data only for OPN/T1 sites from Elasticsearch,
    and filters the data to return tests for OPN ip addresses mentioned in CRIC.
    Also writes:
      - invalid_ips_perfsonar.json  -> {site: [IPs not in CRIC OPN ranges]}
      - valid_ips_perfsonar_tested.json -> {site: [IPs that match CRIC OPN ranges]}
    """
    date_to = datetime.utcnow()
    date_from = date_to - timedelta(hours=hours)
    date_to_str = date_to.strftime(hp.DATE_FORMAT)
    date_from_str = date_from.strftime(hp.DATE_FORMAT)

    traceroute_records = query_records(date_from_str, date_to_str, allowed_sites=T1_NETSITES)
    cric_T1_networks = extract_addresses_cric()

    valid_traceroutes = []

    # Collect per-site IPs
    invalid_ips_by_site = defaultdict(set)
    valid_ips_by_site = defaultdict(set)
    uscms_in = False
    for record in traceroute_records:
        srcs = record.get('src_ipvs', [])
        dsts = record.get('dst_ipvs', [])

        # Map ps_trace NetSite -> CRIC key, then fetch that site's subnets
        src_site = record['src_netsite']
        dst_site = record['dest_netsite']
        if src_site == 'USCMS-FNAL-WC1-LHCOPNE' or dst_site == 'USCMS-FNAL-WC1-LHCOPNE':    
            uscms_in = True
        src_site_key = cric_elastic_mapping.get(src_site)
        dst_site_key = cric_elastic_mapping.get(dst_site)

        src_cric_info = cric_T1_networks.get(src_site_key, [])
        dest_cric_info = cric_T1_networks.get(dst_site_key, [])

        # Filter empties but preserve alignment for validity tagging
        src_nonempty = [ip for ip in srcs if ip]
        dst_nonempty = [ip for ip in dsts if ip]
        opn_ips_srcs = [ip_in_any(ip, src_cric_info) for ip in src_nonempty]
        opn_ips_dsts = [ip_in_any(ip, dest_cric_info) for ip in dst_nonempty]

        # Tally valid/invalid IPs by site
        for ip, ok in zip(src_nonempty, opn_ips_srcs):
            (valid_ips_by_site if ok else invalid_ips_by_site)[src_site].add(ip)
        for ip, ok in zip(dst_nonempty, opn_ips_dsts):
            (valid_ips_by_site if ok else invalid_ips_by_site)[dst_site].add(ip)

        # Keep original "record is valid" criterion
        if any(opn_ips_srcs) and any(opn_ips_dsts):
            valid_traceroutes.append(record)

    # Convert sets to sorted lists for JSON friendliness
    invalid_traceroutes = {site: sorted(ips) for site, ips in invalid_ips_by_site.items()}
    valid_ips_perfsonar_tested = {site: sorted(ips) for site, ips in valid_ips_by_site.items()}

    # Persist both
    with open('invalid_ips_perfsonar.json', 'w') as f:
        json.dump(invalid_traceroutes, f, indent=2)
    with open('valid_ips_perfsonar_tested.json', 'w') as f:
        json.dump(valid_ips_perfsonar_tested, f, indent=2)

    # print(invalid_traceroutes)
    # print(valid_ips_perfsonar_tested)
    print(f"Before filtering: {len(traceroute_records)}")
    print(f"After filtering: {len(valid_traceroutes)}")

    return valid_traceroutes