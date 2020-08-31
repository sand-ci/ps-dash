import re
import socket
import ipaddress
import csv
import multiprocessing as mp
from functools import partial
from contextlib import contextmanager
from datetime import datetime, timedelta
import dateutil.relativedelta
import time
import requests 
import os
import pandas as pd
import functools

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

    es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200, 'scheme': 'https'}],
                       timeout=240, http_auth=credentials)

    if es.ping() == True:
        return es
    else: print("Connection Unsuccessful")


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()    # 1
        value = func(*args, **kwargs)
        end_time = time.perf_counter()      # 2
        run_time = end_time - start_time    # 3
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer


def defaultTimeRange():
    defaultEnd = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
    defaultStart = datetime.strftime(datetime.now() - timedelta(days = 3), '%Y-%m-%d %H:%M')
    return [defaultStart, defaultEnd]


# Expected values: time in miliseconds or string (%Y-%m-%d %H:%M')
def FindPeriodDiff(dateFrom, dateTo):
    if (isinstance(dateFrom, int) and isinstance(dateTo, int)):
        d1 = datetime.fromtimestamp(dateTo/1000)
        d2 = datetime.fromtimestamp(dateFrom/1000)
        time_delta = (d1 - d2)
    else:
        fmt = '%Y-%m-%d %H:%M'
        d1 = datetime.strptime(dateFrom, fmt)
        d2 = datetime.strptime(dateTo, fmt)
        time_delta = d2-d1
    return time_delta


def GetTimeRanges(dateFrom, dateTo, intv=1):
    diff = FindPeriodDiff(dateFrom, dateTo) / intv
    t_format = "%Y-%m-%d %H:%M"
    tl = []
    for i in range(intv+1):
        if (isinstance(dateFrom, int)):
            t = (datetime.fromtimestamp(dateFrom/1000) + diff * i)
            tl.append(int(time.mktime(t.timetuple())*1000))
        else:
            t = (datetime.strptime(dateFrom,t_format) + diff * i).strftime(t_format)
            tl.append(int(time.mktime(datetime.strptime(t, t_format).timetuple())*1000))

    return tl


def CalcMinutes4Period(dateFrom, dateTo):
    time_delta = FindPeriodDiff(dateFrom, dateTo)
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