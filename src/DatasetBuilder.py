import re
import socket
import ipaddress
from datetime import datetime, timedelta
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import numpy as np
import pandas as pd
from ipwhois import IPWhois
import os

import helpers as hp
import queries as qrs
import HostsMetaData as hmd


packetloss = hmd.HostsMetaData('ps_packetloss')
pls = packetloss.df

owd = hmd.HostsMetaData('ps_owd')
owd = owd.df

throughput = hmd.HostsMetaData('ps_throughput')
thp = throughput.df

retransmits = hmd.HostsMetaData('ps_retransmits')
rtt = retransmits.df


def PrepareHostsMetaData():
    dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
    dateFrom = datetime.strftime(datetime.now() - timedelta(hours = 24), '%Y-%m-%d %H:%M')
    time_range = list(hp.GetTimeRanges(dateFrom, dateTo, 1))
    for index in ['ps_packetloss', 'ps_owd']:
        hp.GetIdxUniqueHosts(index, time_range[0], time_range[1])


def BubbleChartDataset(idx, dateFrom, dateTo, fld_type):
    time_range = list(hp.GetTimeRanges(dateFrom, dateTo, 1))
    data = qrs.queryAvgValuebyHost(idx, time_range[0], time_range[1])
    df = pd.DataFrame(data[fld_type])
    df['period'] = pd.to_datetime(df['period'], unit='ms')
    return df


def RemovedHosts():
    if len(hp.hosts) == 0:
        PrepareHostsMetaData()

    def getIPWhoIs(row):
        item = row['Host']
        is_host = re.match(".*[a-zA-Z].*", item)
        val = ''
        try:
            obj = IPWhois(item)
            res = obj.lookup_whois()
            val = res['asn_description']
        except Exception as inst:
            if is_host:
                val = ''
            else: val = inst.args
        return val

    removed = pd.DataFrame(hp.unresolved.items(), columns=['Host', 'Message'])
    removed['IPWhois_ASN_desc'] = removed.apply(getIPWhoIs, axis=1)
    removed['Keep'] = 'N'

    return removed


def SrcDestTables(host):
    dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
    dateFrom = datetime.strftime(datetime.now() - timedelta(hours = 24), '%Y-%m-%d %H:%M')
    pdata = ProcessDataInChunks(qrs.GetPairsForAHost, 'ps_packetloss', dateFrom, dateTo,
                                 chunks=1, args=[host], inParallel=False)
    odata = ProcessDataInChunks(qrs.GetPairsForAHost, 'ps_owd', dateFrom, dateTo,
                                 chunks=1, args=[host], inParallel=False)
    pdf = pd.DataFrame(pdata, columns=['src_host', 'dest_host', 'packet_loss'])
    pdf['packet_loss(%)'] = round(pdf['packet_loss']*100,3) if len(pdf) > 0 else 'N/A'
    odf = pd.DataFrame(odata, columns=['src_host', 'dest_host', 'delay_mean'])
    odf['delay_mean'] = round(odf['delay_mean'],3)  if len(odf) > 0 else 'N/A'
    df = pd.merge(pdf, odf, how='outer',
            left_on=['src_host', 'dest_host'],
            right_on=['src_host', 'dest_host'])
    df = df[['src_host', 'dest_host', 'packet_loss(%)', 'delay_mean']]
    as_source = df[df['src_host']==host].sort_values(['packet_loss(%)', 'delay_mean'], ascending=False)
    as_destination = df[df['dest_host']==host].sort_values(['packet_loss(%)', 'delay_mean'], ascending=False)
    return [as_source, as_destination]


def LossDelayTestCountGroupedbyHost(dateFrom, dateTo):
    # dateFrom = '2020-03-23 10:00'
    # dateTo = '2020-03-23 10:10'

    minutesDiff = hp.CalcMinutes4Period(dateFrom, dateTo)
    p_data = ProcessDataInChunks(qrs.AggBySrcDestIP, 'ps_packetloss', dateFrom, dateTo,
                                 chunks=1, inParallel=False)
    o_data = ProcessDataInChunks(qrs.AggBySrcDestIP, 'ps_owd', dateFrom, dateTo,
                                 chunks=1, inParallel=False)

    pl_config = hp.LoadPSConfigData('ps_packetloss', dateFrom, dateTo)
    owd_config = hp.LoadPSConfigData('ps_owd', dateFrom, dateTo)

    pldf = pd.DataFrame(p_data)
    owdf = pd.DataFrame(o_data)

    host_df0 = pldf.groupby(['src_host']).size().reset_index().rename(columns={0:'packet_loss-total_dests'})
    host_df = pldf.groupby(['src_host']).agg({'packet_loss':'mean'}).reset_index()
    host_df = pd.merge(host_df0, host_df, how='outer', left_on=['src_host'], right_on=['src_host'])
    host_df.rename(columns={'src_host': 'host'}, inplace=True)

    ddf0 = pd.merge(host_df, pl_config, how='left', left_on=['host'], right_on=['host'])

    host_df0 = owdf.groupby(['src_host']).size().reset_index().rename(columns={0:'owd-total_dests'})
    host_df = owdf.groupby(['src_host']).agg({'delay_mean':'mean'}).reset_index()
    host_df = pd.merge(host_df0, host_df, how='outer', left_on=['src_host'], right_on=['src_host'])
    host_df.rename(columns={'src_host': 'host'}, inplace=True)

    ddf1 = pd.merge(host_df, owd_config, how='left', left_on=['host'], right_on=['host'])

    ddf = pd.merge(ddf0[['host', 'packet_loss-total_dests', 'packet_loss', 'total_num_of_dests']],
                   ddf1[['host', 'owd-total_dests', 'delay_mean', 'total_num_of_dests']],
                   how='outer', left_on=['host', 'total_num_of_dests'], right_on=['host', 'total_num_of_dests'])

    return ddf[['host', 'delay_mean', 'packet_loss', 'total_num_of_dests', 'owd-total_dests',  'packet_loss-total_dests']]


def LossDelaySubplotData(dateFrom, dateTo, args):
    time_range = list(hp.GetTimeRanges(dateFrom, dateTo, 1))
    tsFrom = time_range[0]
    tsTo = time_range[1]

    src = args[0]
    dest = args[1]
    def buildDataFrame(idx):
        data = qrs.PairAverageValuesQuery(idx, tsFrom, tsTo, [src, dest])
        df = pd.DataFrame(data)
        if data:
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
            df.sort_values('ts')
        return df

    pldf = buildDataFrame('ps_packetloss')
    owddf = buildDataFrame('ps_owd')

    agg_interval = 30

    def findPercentageOfTestsDone(df, fld):
        def findRatio(row):
            if pd.isna(row['doc_count']):
                count = '0'
            else: count = str(round((row['doc_count']/agg_interval)*100))+'%'
            return count
        if not df.empty:
            df[str(fld+'_tests_done')] = df.apply(lambda x: findRatio(x), axis=1)
            df.rename(columns={'doc_count': str(fld+'_doc_count')}, inplace=True)

    findPercentageOfTestsDone(pldf, 'pl')
    findPercentageOfTestsDone(owddf, 'owd')

    try:
        df = pd.merge(pldf, owddf, left_on=['ts', 'ipv6'], right_on=['ts', 'ipv6'],
                      left_index=False, right_index=False, how='outer')
    except KeyError:
        df = pldf if not pldf.empty else owddf
    print(len(df), len(pldf), len(owddf), dateFrom, dateTo, args)
    df.sort_values('ts', inplace=True)
    # in case one of the dataframes is empty, we add empty columns
    if not "delay_mean" in df:
        df['delay_mean'] = ''
        df['owd_tests_done'] = ''
        df['owd_doc_count'] = ''
    elif not "packet_loss" in df:
        df['delay_mean'] = ''
        df['owd_tests_done'] = ''
        df['owd_doc_count'] = ''

    def isSymetric(idx, ipv6):
        return qrs.PairSymmetryQuery(idx, tsFrom, tsTo, [dest, src, ipv6])

    pl_symmetry = {'ipv6':isSymetric('ps_packetloss', True), 'ipv4':isSymetric('ps_packetloss', False)}
    owd_symmetry = {'ipv6':isSymetric('ps_owd', True), 'ipv4':isSymetric('ps_owd', False)}
    ntp = qrs.GetNTP(tsFrom, tsTo, [src, dest])

    return {'ipv6df': df[df['ipv6']==True], 'ipv4df': df[df['ipv6']==False], 'ntp': ntp, 'pl_symmetry': pl_symmetry, 'owd_symmetry': owd_symmetry}


def SrcDestLossDelayNTP(host, dateFrom, dateTo):
    global ntpdf
    print('ntp data is already loaded:',len(ntpdf))
    def buildDataFrames(index, args):
        data = ProcessDataInChunks(qrs.AggBySrcDestIPv6QueryHost, index, dateFrom, dateTo, 1, args, False)
        dff = pd.DataFrame(data)
        if len(data)> 0:
            dff['ts'] = pd.to_datetime(dff['ts'], unit='ms')
        return dff

    pldf = buildDataFrames('ps_packetloss', [host])
    owddf = buildDataFrames('ps_owd', [host])

    df = pd.merge(pldf, owddf, how='left', 
            left_on=['src_host', 'dest_host', 'ipv6', 'ts'],
            right_on=['src_host', 'dest_host', 'ipv6', 'ts'])

    df = pd.merge(df, ntpdf, how='left', left_on=['src_host'], right_on=['host'])
    df = df.drop(['host'], axis = 1)
    df.rename(columns = {'ntp_delay': "src_ntp", "doc_count_x": "src_test_count"}, inplace = True)
    df = pd.merge(df, ntpdf, how='left', left_on=['dest_host'], right_on=['host'])
    df = df.drop(['host'], axis = 1)
    df.rename(columns = {'ntp_delay': "dest_ntp", "doc_count_y": "dest_test_count"}, inplace = True)

    print('Data is ready')
    return df[['ts', 'src_host', 'dest_host', 'ipv6', 'packet_loss',
               'delay_mean', 'src_ntp', 'dest_ntp', 'src_test_count','dest_test_count']]


def PrepareNTPData(dfs, dateTo):
    print('Collect NTP data from ps_meta...')
    unique_hosts = []
    for d in dfs:
        unique_hosts.extend(set(list(d['src_host'].unique()) + list(d['dest_host'].unique())))

    ntp_from = datetime.strftime(datetime.strptime(dateTo,'%Y-%m-%d %H:%M') - timedelta(hours = 24), '%Y-%m-%d %H:%M')
    ntp_period = hp.GetTimeRanges(ntp_from, dateTo, 1)
    ntp_ts = [next(ntp_period), next(ntp_period)]
    ntp_info = []
    for uh in unique_hosts:
        ntp_info.extend(qrs.QueryNTPDelay('ps_meta', ntp_ts[0], ntp_ts[1], [uh]))

    ntpdf = pd.DataFrame(ntp_info)
    ntpdf['ts'] = pd.to_datetime(ntpdf['ts'], unit='ms')
    ntpdf.set_index('ts', inplace = True)
    ntpdf['ntp_delay'] = ntpdf['ntp_delay'].fillna(ntpdf.groupby([ntpdf['host'], ntpdf.index.day])['ntp_delay'].transform('mean'))

    return ntpdf


def ProcessDataInChunks(func, idx, dateFrom, dateTo, chunks, args=[], inParallel=False):
    start = time.time()
    print('>>> Main process start:', time.strftime("%H:%M:%S", time.localtime()))

    time_range = list(hp.GetTimeRanges(dateFrom, dateTo, chunks))

    data = [] 
    for i in range(len(time_range)-1):
        curr_t = time_range[i]
        next_t = time_range[i+1]
        print(i, curr_t, next_t, args)
        if len(args) > 0:
            results = func(idx, curr_t, next_t, args)
        else: results = func(idx, curr_t, next_t)

        prdata = hp.ProcessHosts(index=idx, data=results, tsFrom=time_range[0], tsTo=time_range[-1], inParallel=inParallel, saveUnresolved=False)

        percetage = round(((len(results)-len(prdata))/ len(results))*100) if len(results) > 0 else 0
        print('before:', len(results), 'after:', len(prdata), 'reduced by', percetage, '%')

        data.extend(prdata)

    print('Number of active hosts: total(',len(hp.hosts),') - unresolved(',len(hp.unresolved),') = ',len(hp.hosts) - len(hp.unresolved))
    print(">>> Overall elapsed = %ss" % (int(time.time() - start)))

    return data


def RefreshData():
    for period in [1, 12, 24, 72, 168]:
        dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
        dateFrom = datetime.strftime(datetime.now() - timedelta(hours = period), '%Y-%m-%d %H:%M')
        if os.path.exists("data") == False:
            os.mkdir("data")
        file_name = "data/LossDelayTestCountGroupedbyHost-"+str(period)+".csv"
        df_tab = LossDelayTestCountGroupedbyHost(dateFrom, dateTo)
        df_tab.to_csv(file_name, index = False)


def StartCron():
    import atexit
    from apscheduler.schedulers.background import BackgroundScheduler
    print('Start cron')
    cron = BackgroundScheduler()
    cron.add_job(func=PrepareHostsMetaData, trigger="interval", minutes=60, start_date=datetime.now())
    cron.add_job(func=RefreshData, trigger="interval", minutes=60, start_date=datetime.now())
    cron.start()
    atexit.register(lambda: cron.shutdown())