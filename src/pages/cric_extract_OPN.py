
import utils.helpers as hp
from datetime import datetime, timedelta
from ipaddress import ip_address, ip_network
from datetime import datetime, timedelta
from collections import defaultdict
import model.queries as qrs

def readParquetCRIC(pq):
    """
    The function reads parquet file that is updated every 24 \
    hours with the data about perfSONARs from CRIC.
    """
    parquet_path = 'parquet/raw/CRICData.parquet'
    try: 
        print("Reading the parquet file with CRIC data...")
        lst = pq.readFile(parquet_path)['host'].tolist()
        return lst
    except Exception as err:
        print(err)
        print(f"Problems with reading the file {parquet_path}")
        
def extract_subnets_cric(pq):
    parquet_path = 'parquet/raw/CRICDataOPNSubnets.parquet'
    try: 
        print("Reading the parquet file with CRIC OPN subnets data...")
        df = pq.readFile(parquet_path)
        return df
    except Exception as err:
        print(err)
        print(f"Problems with reading the file {parquet_path}")


T1_NETSITES = [
    "BNL-ATLAS-LHCOPNE", "USCMS-FNAL-WC1-LHCOPNE", "RAL-LCG2-LHCOPN",
    "JINR-T1-LHCOPNE", "NCBJ-LHCOPN",
    "NLT1-SARA-LHCOPNE", "INFN-T1-LHCOPNE", "NDGF-T1-LHCOPNE",
    "KR-KISTI-GSDC-1-LHCOPNE", "IN2P3-CC-LHCOPNE", "pic-LHCOPNE",
    "FZK-LCG2-LHCOPNE", "CERN-PROD-LHCOPNE", "TRIUMF-LCG2-LHCOPNE"
]


cric_elastic_mapping = {'CERN-PROD-LHCOPNE': 'CERN-PROD-LHCOPNE', 'BNL-ATLAS-LHCOPNE': 'BNL-ATLAS-LHCOPNE', 
                        'INFN-T1-LHCOPNE': 'INFN-T1-LHCOPNE', 'USCMS-FNAL-WC1-LHCOPNE': 'USCMS-FNAL-WC1-LHCOPNE', 
                        'FZK-LCG2-LHCOPNE': 'FZK-LCG2-LHCOPNE', 'IN2P3-CC-LHCOPNE': 'IN2P3-CC-LHCOPNE', 'RAL-LCG2-LHCOPN': 'RAL-LCG2-LHCOPN', 
                        'JINR-T1-LHCOPNE': 'JINR-T1-LHCOPNE', 'PIC-LHCOPNE': 'pic-LHCOPNE', 'RRC-KI-T1-LHCOPNE': 'RRC-KI-T1-LHCOPNE', 
                        'NLT1-SARA-LHCOPNE': 'NLT1-SARA-LHCOPNE', 'TRIUMF-LCG2-LHCOPNE': 'TRIUMF-LCG2-LHCOPNE', 'NDGF-T1-LHCOPNE': 'NDGF-T1-LHCOPNE', 
                        'KR-KISTI-GSDC-1-LHCOPNE': 'KR-KISTI-GSDC-1-LHCOPNE', 'NCBJ-LHCOPN': 'NCBJ-LHCOPN'}


def ip_in_any(ip, networks):
    return any(ip_address(ip) in ip_network(net) for net in networks)


def query_valid_trace_data(hours, parquet):
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

    traceroute_records = qrs.queryOPNTraceroutes(date_from_str, date_to_str, T1_NETSITES)
    cric_T1_networks = extract_subnets_cric(parquet)
    cric_T1_networks = cric_T1_networks.set_index("site")["subnets"].to_dict()
    valid_traceroutes = []

    invalid_ips_by_site = defaultdict(set)
    valid_ips_by_site = defaultdict(set)
    for record in traceroute_records:
        srcs = record.get('src_ipvs', [])
        dsts = record.get('dst_ipvs', [])

        src_site = record['src_netsite']
        dst_site = record['dest_netsite']
        src_site_key = cric_elastic_mapping.get(src_site).upper()
        dst_site_key = cric_elastic_mapping.get(dst_site).upper()
        
        src_cric_info = cric_T1_networks.get(src_site_key, [])
        dest_cric_info = cric_T1_networks.get(dst_site_key, [])

        src_nonempty = [ip for ip in srcs if ip]
        dst_nonempty = [ip for ip in dsts if ip]
        opn_ips_srcs = [ip_in_any(ip, src_cric_info) for ip in src_nonempty]
        opn_ips_dsts = [ip_in_any(ip, dest_cric_info) for ip in dst_nonempty]

        # check whether ips from elasticsearch are in OPN subnets mentioned in CRIC
        for ip, ok in zip(src_nonempty, opn_ips_srcs):
            (valid_ips_by_site if ok else invalid_ips_by_site)[src_site].add(ip)
        for ip, ok in zip(dst_nonempty, opn_ips_dsts):
            (valid_ips_by_site if ok else invalid_ips_by_site)[dst_site].add(ip)

        if any(opn_ips_srcs) and any(opn_ips_dsts):
            valid_traceroutes.append(record)

    print(f"Before filtering Elasticsearch traceroute records (according to CRIC): {len(traceroute_records)}")
    print(f"After filtering: {len(valid_traceroutes)}")

    return valid_traceroutes