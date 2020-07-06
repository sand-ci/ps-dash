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
    
    print("LoadPSConfigData took: %ss" % (int(time.time() - start)))
    return dest_df

es = ConnectES()