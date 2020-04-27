import re
import socket
import ipaddress
from datetime import datetime
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import numpy as np
import pandas as pd

import helpers as hp



def queryAvgPacketLossbyHost(fld, group, fromDate, toDate):

    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": fromDate,
                                "lte": toDate
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "host": {
                "terms": {
                    "field": fld,
                    "size": 9999
                },
                "aggs": {
                    "period": {
                        "date_histogram": {
                            "field": "timestamp",
                            "calendar_interval": group
                        },
                        "aggs": {
                            "avg_loss": {
                                "avg": {
                                    "field": "packet_loss"
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    data = hp.es.search(index="ps_packetloss", body=query)

    result = []
    unknown = []

    for host in data['aggregations']['host']['buckets']:
        resolved = hp.ResolveHost(hp.es, host['key'])
        if (resolved['resolved']):
            h = resolved['resolved']
        elif (len(resolved['unknown'][0]) != 0) and (resolved['unknown'][0] not in unknown):
            unknown.append(resolved['unknown'])

        for period in host['period']['buckets']:
            result.append({'host': h, 'period': period['key'], 'avg_loss': period['avg_loss']['value']})

    return {'resolved': result, 'unknown': unknown}


def BubbleChartDataset():
    # from 01-12-2019 to 22-01-2020 Get a list hosts and theis avg packet loss being a src_host and a dest_host
    # ps_packetloss has data since mid December 2019 only
    ssite = queryAvgPacketLossbyHost('src_host', 'day', '1575151349000', '1579687349000')
    dsite = queryAvgPacketLossbyHost('dest_host', 'day', '1575151349000', '1579687349000')

    sdf = pd.DataFrame(ssite['resolved'])
    ddf = pd.DataFrame(dsite['resolved'])

    sdf['period'] = pd.to_datetime(sdf['period'], unit='ms')
    ddf['period'] = pd.to_datetime(ddf['period'], unit='ms')

    mdf = pd.merge(sdf, ddf, on=['host', 'period'])
    # calculate the mean for all hosts
    mdf['mean'] = mdf[['avg_loss_x', 'avg_loss_y']].mean(axis=1)

    return mdf

def CalcMinutes4Period(dateFrom, dateTo):
    fmt = '%Y-%m-%d %H:%M'
    d1 = datetime.strptime(dateFrom, fmt)
    d2 = datetime.strptime(dateTo, fmt)
    daysDiff = (d2-d1).seconds

    return round((d2-d1).seconds / 60)


def MakeChunks(minutes):
    if minutes < 60:
        return 1
    else:
        return round(minutes / 60)


def CountTestsGroupedByHost():
    dateFrom = '2020-03-23 10:00'
    dateTo = '2020-03-23 10:10'

    minutesDiff = CalcMinutes4Period(dateFrom, dateTo)
    p_data = ProcessDataInChunks('ps_packetloss', dateFrom, dateTo, chunks=hp.MakeChunks(minutesDiff))
    o_data = ProcessDataInChunks('ps_owd', dateFrom, dateTo, 1)

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


def RunQuery(idx, time_from, time_to):
    field = 'packet_loss' if idx == 'ps_packetloss' else 'delay_mean'
    query = {
              "size" : 0,
              "_source" : False,
              "query" : {
                "range" : {
                  "timestamp" : {
                    "from" : time_from,
                    "to" : time_to
                  }
                }
              },
              "aggregations" : {
                "groupby" : {
                  "composite" : {
                    "size" : 10000,
                    "sources" : [
                      {
                        "src_host" : {
                          "terms" : {
                            "field" : "src_host",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      },
                      {
                        "dest_host" : {
                          "terms" : {
                            "field" : "dest_host",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      }
                    ]
                  },
                  "aggregations" : {
                    "mean_field" : {
                      "avg" : {
                        "field" : field
                      }
                    }
                  }
                }
              }
            }


    results = hp.es.search( index=idx, body=query)

    data = []
    data1 = []
    for item in results["aggregations"]["groupby"]["buckets"]:
        data1.append(item)
        data.append({'dest_host':item['key']['dest_host'], 'src_host':item['key']['src_host'], 
                     field: item['mean_field']['value'], 'num_tests': item['doc_count']})
            
    return data


def ProcessDataInChunks(idx, dateFrom, dateTo, chunks):
    start = time.time()
    print('>>> Main process start:', time.strftime("%H:%M:%S", time.localtime()))
    
    time_range = list(hp.GetTimeRanges(dateFrom, dateTo, chunks))
    
    for field in ['src_host', 'dest_host']:
        hp.GetIdxUniqueHosts(idx, field, time_range[0], time_range[-1])

    data = [] 
    for i in range(len(time_range)-1):
        curr_t = time_range[i]
        next_t = time_range[i+1]
      
        results = RunQuery(idx, curr_t, next_t)
        prdata = hp.ProcessHosts(data=results, saveUnresolved=True)
        
        print('before:', len(results), 'after:', len(prdata), 'reduced by', round(((len(results)-len(prdata))/ len(results))*100), '%')
        
        data.extend(prdata)
        
    print('Number of active hosts: total(',len(hp.hosts),') - unresolved(',len(hp.unresolved),') = ',len(hp.hosts) - len(hp.unresolved))   
    print(">>> Overall elapsed = %ss" % (int(time.time() - start)))
        
    return data
    
    
