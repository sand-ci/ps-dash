
import multiprocessing as mp
from datetime import datetime, timedelta
import time
import requests 
import os
import pandas as pd
import functools

from elasticsearch import Elasticsearch
import getpass

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
INDICES = ['ps_packetloss', 'ps_owd', 'ps_throughput', 'ps_trace']

user, passwd, mapboxtoken = None, None, None
with open("/etc/ps-dash/creds.key") as f:
    user = f.readline().strip()
    passwd = f.readline().strip()
    mapboxtoken = f.readline().strip()

def ConnectES():
    global user, passwd
    credentials = (user, passwd)

    try:
        if getpass.getuser() == 'petya':
            es = Elasticsearch('https://localhost:9200', verify_certs=False, http_auth=credentials, max_retries=20)
        else:
            es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200, 'scheme': 'https'}],
                                http_auth=credentials, max_retries=10)
        print('Success' if es.ping()==True else 'Fail')
        return es
    except Exception as error:
        print (">>>>>> Elasticsearch Client Error:", error)



def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer


def convertDate(dt):
    try:
        parsed_date = datetime.strptime(dt, DATE_FORMAT)
        formatted_date = parsed_date.strftime(DATE_FORMAT)
    except ValueError:
        parsed_date = datetime.strptime(dt, DATE_FORMAT)
        formatted_date = parsed_date.strftime(DATE_FORMAT)

    return formatted_date


def getPriorNhPeriod(end, daysBefore=2, midPoint=True):
    daysAfter = daysBefore
    if not midPoint:
        daysAfter = 0

    end = convertDate(end)
    endT = datetime.strptime(end, DATE_FORMAT)
    start = datetime.strftime(endT - timedelta(daysBefore), DATE_FORMAT)
    end = datetime.strftime(endT + timedelta(daysAfter), DATE_FORMAT)
    return start, end


def getValueUnit(test_type):
    if (test_type == 'ps_packetloss'):
        return '% lost (packets) avg'
    elif (test_type == 'ps_throughput'):
        return 'throughput (MBps) avg'
    elif (test_type == 'ps_owd'):
        return 'delay (ms) avg'


def defaultTimeRange(days=3, datesOnly=False):
    format = DATE_FORMAT
    if datesOnly:
        format = '%Y-%m-%d'
    
    now = roundTime(datetime.now())  # 1 hour
    defaultEnd = datetime.strftime(now, format)
    defaultStart = datetime.strftime(now - timedelta(days), format)
    return [defaultStart, defaultEnd]


def roundTime(dt=None, round_to=60*60):
    if dt == None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds
    rounding = (seconds+round_to/2) // round_to * round_to
    return dt + timedelta(0,rounding-seconds,-dt.microsecond)


# Expected values: time in miliseconds or string (%Y-%m-%dT%H:%M:%S.000Z')
def FindPeriodDiff(dateFrom, dateTo):
    d1 = datetime.strptime(dateFrom, DATE_FORMAT)
    d2 = datetime.strptime(dateTo, DATE_FORMAT)
    time_delta = d2-d1
    return time_delta


def GetTimeRanges(dateFrom, dateTo, intv=1):
    diff = FindPeriodDiff(dateFrom, dateTo) / intv
    tl = []
    for i in range(intv+1):
        if isinstance(dateFrom, int):
            t = (datetime.fromtimestamp(dateFrom/1000) + diff * i).strftime(DATE_FORMAT)
        else:
            t = (datetime.strptime(dateFrom, DATE_FORMAT) + diff * i).strftime(DATE_FORMAT)
        tl.append(t)
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