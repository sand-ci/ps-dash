import re
import socket
import ipaddress
from datetime import datetime, timedelta
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import numpy as np
import pandas as pd

import helpers as hp
import queries as qrs
import os


ntpdf = qrs.GetNTP()


def BubbleChartDataset(idx, dateFrom, dateTo, fld_type):
    time_range = list(hp.GetTimeRanges(dateFrom, dateTo, 1))
    data = qrs.queryAvgValuebyHost(idx, time_range[0], time_range[1])
    df = pd.DataFrame(data[fld_type])
    df['period'] = pd.to_datetime(df['period'], unit='ms')
    return df


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


def SrcDestLossDelayNTP(host, dateFrom, dateTo):
    global ntpdf
    print('ntp data is already loaded:',len(ntpdf))
    def buildDataFrames(index, args):
        data = ProcessDataInChunks(qrs.AggBySrcDestIPv6QueryHost, index, dateFrom, dateTo, 1, args, False)
        dff = pd.DataFrame(data)
        if len(data)> 0:
            dff['ts'] = pd.to_datetime(dff['ts'], unit='ms')
        return dff

#     pldf = buildDataFrames('ps_packetloss', ['ccperfsonar2.in2p3.fr'])
#     owddf = buildDataFrames('ps_owd', ['ccperfsonar2.in2p3.fr'])

    pldf = buildDataFrames('ps_packetloss', [host])
    owddf = buildDataFrames('ps_owd', [host])

#     ntpdf = PrepareNTPData([pldf, owddf], dateTo).reset_index()
#     ntpdf = PrepareNTPData([pldf, owddf], dateTo)

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
        file_name = "data/LossDelayTestCountGroupedbyHost-"+str(period)+".csv"
        df_tab = LossDelayTestCountGroupedbyHost(dateFrom, dateTo)
        df_tab.to_csv(file_name, index = False)


def StartCron():
    import atexit
    from apscheduler.schedulers.background import BackgroundScheduler
    print('Start cron')
    cron = BackgroundScheduler()
    cron.add_job(func=RefreshData, trigger="interval", minutes=60)
    cron.start()
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: cron.shutdown())
