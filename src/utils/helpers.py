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


def defaultTimeRange(days=3, datesOnly=False, startEndOfDay=False):
    format = DATE_FORMAT
    if datesOnly:
        format = '%Y-%m-%d'
    now = roundTime(datetime.now())  # 1 hour
    formatStart = format
    if startEndOfDay:
        formatStart = '%Y-%m-%dT00:00:00.000Z'
    defaultEnd = datetime.strftime(now, format)
    defaultStart = datetime.strftime(now - timedelta(days), formatStart)
    return [defaultStart, defaultEnd]


def roundTime(dt=None, round_to=60*60):
    if dt == None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds
    rounding = (seconds+round_to/2) // round_to * round_to
    return dt + timedelta(0,rounding-seconds,-dt.microsecond)


# Expected values: time in miliseconds or string (%Y-%m-%dT%H:%M:%S.000Z')
def FindPeriodDiff(dateFrom, dateTo):
    # Try multiple formats to handle both with and without .000Z
    for fmt in [DATE_FORMAT, "%Y-%m-%dT%H:%M:%S"]:
        try:
            d1 = datetime.strptime(dateFrom, fmt)
            d2 = datetime.strptime(dateTo, fmt)
            return d2 - d1
        except ValueError:
            continue
    raise ValueError(f"Time data '{dateFrom}' or '{dateTo}' does not match expected formats.")


def parse_datetime_multi(date_str):
    for fmt in [DATE_FORMAT, "%Y-%m-%dT%H:%M:%S"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{date_str}' does not match expected formats.")


def GetTimeRanges(dateFrom, dateTo, intv=1):
    diff = FindPeriodDiff(dateFrom, dateTo) / intv
    tl = []
    for i in range(intv+1):
        if isinstance(dateFrom, int):
            t = (datetime.fromtimestamp(dateFrom/1000) + diff * i).strftime(DATE_FORMAT)
        else:
            t = (parse_datetime_multi(dateFrom) + diff * i).strftime(DATE_FORMAT)
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



es = ConnectES()