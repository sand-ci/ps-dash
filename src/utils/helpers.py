import multiprocessing as mp
from datetime import datetime, timedelta
import time
import requests
import os
import pandas as pd
import functools

from elasticsearch import Elasticsearch

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
INDICES = ['ps_packetloss', 'ps_owd', 'ps_throughput', 'ps_trace']

user, passwd, mapboxtoken, creds_source = None, None, None, 'none'

_es_user = os.environ.get('ES_USER')
_es_pass = os.environ.get('ES_PASS')
if _es_user and _es_pass:
    user, passwd, creds_source = _es_user, _es_pass, 'env'
    mapboxtoken = os.environ.get('MAPBOX_TOKEN', '')
else:
    try:
        with open("/etc/ps-dash/creds.key") as f:
            user = f.readline().strip()
            passwd = f.readline().strip()
            mapboxtoken = f.readline().strip()
        creds_source = 'file'
    except FileNotFoundError:
        print(">>>>>> /etc/ps-dash/creds.key not found and ES_USER/ES_PASS not set.")
        print(">>>>>> Set ES_USER, ES_PASS (and optionally ES_HOST) as environment variables.")


def _env_int(name, default, minimum=0):
    try:
        return max(minimum, int(os.environ.get(name, default)))
    except (ValueError, TypeError):
        return default


def _env_bool(name, default):
    val = os.environ.get(name)
    if val is None:
        return default
    return val.lower() in ('1', 'true', 'yes')


def ConnectES():
    global user, passwd, creds_source
    if not user or not passwd:
        print(">>>>>> No ES credentials — cannot connect.")
        return None

    es_host = os.environ.get('ES_HOST') or (
        'localhost:9200' if creds_source == 'file' else 'atlas-kibana.mwt2.org:9200'
    )
    es_url = f'https://{es_host}'
    request_timeout = _env_int('ES_REQUEST_TIMEOUT_SECONDS', 30, minimum=1)
    max_retries = _env_int('ES_MAX_RETRIES', 3, minimum=0)
    retry_on_timeout = _env_bool('ES_RETRY_ON_TIMEOUT', True)

    try:
        es = Elasticsearch(
            es_url,
            basic_auth=(user, passwd),
            verify_certs=False,
            ssl_show_warn=False,
            max_retries=max_retries,
            retry_on_timeout=retry_on_timeout,
            request_timeout=request_timeout,
        )
        try:
            es.info()
            print(f'Connected to Elasticsearch ({es_host}, timeout={request_timeout}s, retries={max_retries})')
        except Exception as ping_err:
            print(f">>>>>> ES connection check failed ({es_host}): {ping_err}")
            if creds_source == 'file':
                print(">>>>>> Ensure the SSH tunnel is up: ssh -NL 9200:atlas-kibana.mwt2.org:9200 <user>@lxplus.cern.ch")
        return es
    except Exception as error:
        print(f">>>>>> Elasticsearch Client Error ({es_host}): {error}")
        return None


class _LazyElasticsearchClient:
    """Defers ES connection to first use so the app starts without ES available."""
    def __init__(self):
        self._client = None
        self._attempted = False

    def _get_client(self):
        if not self._attempted:
            self._attempted = True
            self._client = ConnectES()
        return self._client

    def __getattr__(self, name):
        client = self._get_client()
        if client is None:
            raise RuntimeError("Elasticsearch is not available — check credentials and connection")
        return getattr(client, name)



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
    return datetime.strptime(dt, DATE_FORMAT).strftime(DATE_FORMAT)


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



es = _LazyElasticsearchClient()