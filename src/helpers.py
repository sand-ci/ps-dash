import re
import socket
import ipaddress
import csv
import multiprocessing as mp
from functools import partial
from contextlib import contextmanager
import datetime
import time
import requests 
import os
from datetime import datetime
import time
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import getpass

def ConnectES():
    user = None
    passwd = None
    if user is None and passwd is None:
        with open("/etc/ps-dash/creds.key") as f:
            user = f.readline().strip()
            passwd = f.readline().strip()
    credentials = (user, passwd)

    es_url = 'localhost:9200' if getpass.getuser() == 'petya' else 'atlas-kibana.mwt2.org:9200'
    es = Elasticsearch([es_url], timeout=240, http_auth=credentials)

    if es.ping() == True:
        return es
    else: print("Connection Unsuccessful")


def GetTimeRanges(start, end, intv=1):
    t_format = "%Y-%m-%d %H:%M"
    start = datetime.strptime(start,t_format)
    end = datetime.strptime(end, t_format)
    diff = (end - start ) / intv

    for i in range(intv):
        t = (start + diff * i).strftime(t_format)
        yield int(time.mktime(datetime.strptime(t, t_format).timetuple())*1000)

    yield int(time.mktime(end.timetuple())*1000)


def CalcMinutes4Period(dateFrom, dateTo):
    fmt = '%Y-%m-%d %H:%M'
    d1 = datetime.strptime(dateFrom, fmt)
    d2 = datetime.strptime(dateTo, fmt)
    time_delta = d2-d1

    return (time_delta.days*24*60 + time_delta.seconds//60)


def MakeChunks(minutes):
    if minutes < 60:
        return 1
    else:
        return round(minutes / 60)


def getValueField(idx):
    if idx == 'ps_packetloss':
        return 'packet_loss'
    elif idx == 'ps_owd':
        return 'delay_mean'
    elif idx == 'ps_retransmits':
        return 'retransmits'
    elif idx == 'ps_throughput':
        return 'throughput'

    return None


def GetDestinationsFromPSConfig(host):
    url = "http://psconfig.opensciencegrid.org/pub/auto/" + host.lower()
    r = requests.get(url)
    data = r.json()

    s2d = []
    if 'message' not in data:
        temp = []
        for test in data['tests']:

            if test['parameters']['type'] == 'perfsonarbuoy/owamp':

                if test['members']['type'] == 'mesh':
                    temp.extend(test['members']['members'])
                elif test['members']['type'] == 'disjoint':
                    temp.extend(test['members']['a_members'])
                    temp.extend(test['members']['a_members'])
                else: print('type is different', test['members']['type'], host)

        dests = list(set(temp))

        if host in temp:
            dests.remove(host)
        s2d = [host, len(dests), dests]
    return s2d



# Read data for each source from psconfig.opensciencegrid.org and get total number of destinations
# as well as all members for each type of mesh/disjoing
def LoadPSConfigData(idx_host_list, dateFrom, dateTo):
    start = time.time()
    # Get all hosts for both fields - source and destination
#     time_range = list(GetTimeRanges(dateFrom, dateTo, 1))
#     hosts = GetIdxUniqueHosts(idx, time_range[0], time_range[-1])
#     uhosts = list(set(v for v in hosts.values() if v != 'unresolved'))

    print('Loading PSConfig data...')
    # If file was creted recently only update with new information
    try:
        created = os.path.getmtime('psconfig.csv')
        now = time.time()

        if (int(now-created)/(60*60*24)) > 7:
            os.remove('psconfig.csv')
            print('PSConfig data is older than a week. The file will be recreated.')
            LoadDestInfoFromPSConfig(idx_host_list, dateFrom, dateTo)
        else: dest_df = pd.read_csv('psconfig.csv')
    except (FileNotFoundError) as error:
        dest_df = pd.DataFrame(columns=['host', 'total_num_of_dests', 'members'])

    changed = False
    for h in idx_host_list:
        if h not in dest_df['host'].values:
            conf = GetDestinationsFromPSConfig(h)
            if len(conf) > 0:
                changed = True
                dest_df = dest_df.append({'host':conf[0], 'total_num_of_dests':conf[1], 'members':conf[2]}, ignore_index=True)

    if changed is True:
        dest_df.to_csv('psconfig.csv', index=False)

    return dest_df



def GetHostsMetaData():
    # The query is aggregating results by month, host, ipv4 and ipv6. The order is ascending.
    # That way we ensure the last added host name in the ip_data dictionary is the most recent one. We add it at position 0.
    query = {
      "size" : 0,
      "_source" : False,
      "aggregations" : {
        "perf_meta" : {
          "composite" : {
            "size" : 9999,
            "sources": [
              {
                "ts" : {
                  "terms" : {
                    "script" : {
                      "source" :"InternalSqlScriptUtils.dateTrunc(params.v0,InternalSqlScriptUtils.docValue(doc,params.v1),params.v2)",
                      "lang" : "painless",
                      "params" : {
                        "v0" : "month",
                        "v1" : "timestamp",
                        "v2" : "Z"
                      }
                    },
                    "missing_bucket" : True,
                    "value_type" : "long",
                    "order" : "asc"
                  }
                }
              },
              {
                "host" : {
                  "terms" : {
                    "field" : "host.keyword",
                    "missing_bucket" : True
                    }
                }
              },
              {
                "ipv4" : {
                  "terms" : {
                    "field" : "external_address.ipv4_address",
                    "missing_bucket" : True
                    }
                }
              },
              {
                "ipv6" : {
                  "terms" : {
                    "field" : "external_address.ipv6_address",
                    "missing_bucket" : True
                  }
                }
              }
            ]
          }
        }
      }
    }

    data = es.search(index="ps_meta", body=query)

    ip_data = {}
    def Add2Dict(ip, host):
        temp = []
        if (ip in ip_data):
            if (host not in ip_data[ip]) and (host != ''):
                temp.append(host)
                temp.extend(ip_data[ip])
                ip_data[ip] = temp
        else: 
            ip_data[ip] = [host]

    host_list = []
    for item in data["aggregations"]["perf_meta"]["buckets"]:

        ipv4 = item['key']['ipv4']
        ipv6 = item['key']['ipv6']
        host = item['key']['host']

        if ipv4 is not None:
            Add2Dict(ipv4, host)

        if ipv6 is not None:
            Add2Dict(ipv6, host)

        if (host not in host_list) and (host is not None) and (len(host) > 0):
            host_list.append(host)

    return {'hosts': host_list, 'ips': ip_data}


manager = mp.Manager()
unresolved = manager.dict()
hosts = manager.dict()
lock = mp.Lock()

es = ConnectES()
# Collects all hosts existing in ps_meta index
meta = GetHostsMetaData()
hosts_meta = meta['hosts']
ips_meta = meta['ips']

### That method should be run as a pre-step before ProcessHosts.
### It will fix all hosts beforehand and then look up hosts dictionary during the parallel processing of the dataset.
def GetIdxUniqueHosts(idx, timeFrom, timeTo):
    global hosts
    print('Get all unique hosts for',idx,'and fix them...')
    start = time.time()
    hfields = list(es.indices.get_mapping(str(idx+'*')).values())[0]['mappings']['properties'].keys()
    for fld in hfields:
        if 'host' in fld:
            query = {
                  "size":0,
                  "query": {
                    "bool":{
                      "must":[
                        {
                          "range": {
                            "timestamp": {
                              "gte": timeFrom,
                              "lte": timeTo
                            }
                          }
                        }
                      ]
                    }
                  },
                  "aggs":{
                    "unique":{
                        "terms":{
                            "field":fld,
                            "size": 9999
                        }
                    }
                  }
                }

            data = es.search(index=idx, body=query)

            for item in data["aggregations"]["unique"]["buckets"]:
                host_val = item['key']

                if host_val in hosts:
                    if hosts[host_val] != 'unresolved':
                        host_val = hosts[host_val]
                # If host is not in hosts already, try to resolve it then add the result to the shared dictionary
                else:
                    host_ = ResolveHost(item['key'])
                    if (host_['resolved']):
                        hosts[host_val] = host_['resolved']
                    elif (host_['unresolved']):
                        unresolved[host_['unresolved'][0]] = host_['unresolved'][1]
                        hosts[host_val] = 'unresolved'
    print("Took: %ss" % (int(time.time() - start)))
    return hosts



### Process the dataset in parallel
def ProcessHosts(index, data, tsFrom, tsTo, inParallel, saveUnresolved):
    print('Start:', time.strftime("%H:%M:%S", time.localtime()))
    GetIdxUniqueHosts(index, tsFrom, tsTo)

    print('Process items...')
    start = time.time()

    @contextmanager
    def poolcontext(*args, **kwargs):
        pool = mp.Pool(*args, **kwargs)
        yield pool
        pool.terminate()


    processes = 1
    if inParallel is True:
        processes=mp.cpu_count()

    with poolcontext(processes=mp.cpu_count()) as pool:
        i = 0
        results = []
        for doc in pool.imap_unordered(partial(FixHosts, unres=unresolved), data):
            if (i > 0) and (i % 100000 == 0):
                print('Next',i,'items')
            if doc is not None:
                results.append(doc)
            i = i + 1

    print("Time elapsed = %ss" % (int(time.time() - start)))

    if saveUnresolved:
        try:
            not_found = pd.read_csv('not_found.csv')
        except (pd.io.common.EmptyDataError, FileNotFoundError) as error:
            not_found = pd.DataFrame(columns=['host', 'message'])

        for key, val in dict(unresolved).items():
            if not (not_found['host'].str.contains(key).any()):
                not_found.loc[len(not_found)+1] = [key, val]

        not_found.sort_values(['host', 'message'], ascending=[True, False], inplace=True)
        not_found.to_csv('not_found.csv', index=False)

    return results



#### Fix hosts by: 
####   replacing IP addresses with correct host names: 
####   removing documents that are not part of the configuration
def FixHosts(item, unres):
    with lock:
        fields = []
        for i in item.keys():
            if 'host' in i:
                fields.append(i)

        isOK = False
        statusOK = []
        for fld in fields:
            ip_fld = fld[:fld.index('_')]
            try:
                fld_val = item[fld] if item[fld] != '' else item[ip_fld]
            except:
                continue

            # Check status of host already resolved by GetIdxUniqueHosts
            if hosts[fld_val] != 'unresolved':
                isOK = True
                item[fld] = hosts[fld_val]
            else: isOK = False

            statusOK.append(isOK)

#             logging the process
#             import os.path
#             with open(r'log.csv', 'a', newline='') as csvfile:
#                 fieldnames = ['field','value','resolved','isOK']
#                 writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#                 writer.writeheader()
#                 writer.writerow({'field':fld,'value':fld_val,'resolved':hosts[fld_val],'isOK': isOK})

        if (all(statusOK)):
            return item



### Try to resolve IP addresses that were filled for host names and thus exclude data not relevant to the project
### If it's a host name, verify it's part of the configuration, i.e. search in ps_meta
###   If it's an IP - try to resolve 
###     If it cannot be resolved
###         search the IP in ps_meta for the corresponding host name
###         If not - mark it unresolved
def ResolveHost(host):
    is_host = re.match(".*[a-zA-Z].*", host)
    h = ''
    u = []

    try:
        # There are data that is collected in ElasticSearch, but it's not relevant to the SAND project.
        # It is neccessary to check if it is part of the configuration by searching in ps_meta index.
        if is_host:
            if host in hosts_meta:
                h = host
            else:
                u.append(host)
                u.append("Host not found in ps_meta index")
        else:
            # Sometimes host name is not resolved and instead IP address is added. Try to resolve the given IP
            if host == '127.0.0.1':
                print(host)
                u.append(host)
                u.append("Incorrect value for host")
            elif (socket.gethostbyaddr(host)[0] in hosts_meta):
                h = socket.gethostbyaddr(host)[0]
            elif (host in ips_meta):
                h = ips_meta[host][0]
            else:
                u.append(host)
                u.append('IP not in ps_meta')
    except Exception as inst:
        # It is possible that the IP got changed but the host is still valid. Check if the ip exists in ps_meta and take the host name.
        if host in ips_meta:
            h = ips_meta[host][0]
        else:
            u.append(host)
            u.append(str("socket exception: "+inst.args[1]))

    return {'resolved': h, 'unresolved': u}