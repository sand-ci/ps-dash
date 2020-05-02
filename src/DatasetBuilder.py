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



def queryAvgValuebyHost(idx, fromDate, toDate):
    val_fld = 'packet_loss' if idx == 'ps_packetloss' else 'delay_mean'
    def runQuery(fld):
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
                                "calendar_interval": "day"
                            },
                            "aggs": {
                                val_fld: {
                                    "avg": {
                                        "field": val_fld
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return hp.es.search(index=idx, body=query)

    result = {}
    for ft in ['src_host', 'dest_host']:
        data = runQuery(ft)
        temp = []
        for host in data['aggregations']['host']['buckets']:
            for period in host['period']['buckets']:
                temp.append({'host': host['key'], 'period': period['key'],val_fld: period[val_fld]['value']})

        result[ft] = temp
    return result



def BubbleChartDataset(idx, dateFrom, dateTo, fld_type):
    time_range = list(hp.GetTimeRanges(dateFrom, dateTo, 1))
    data = queryAvgValuebyHost(idx, time_range[0], time_range[1])
    df = pd.DataFrame(data[fld_type])
    df['period'] = pd.to_datetime(df['period'], unit='ms')
    return df



def CountTestsGroupedByHost(isDev, dateFrom, dateTo):
    if isDev == True:
        return pd.read_csv("df.csv")
    else:
        minutesDiff = hp.CalcMinutes4Period(dateFrom, dateTo)
        p_data = ProcessDataInChunks(AggBySrcDestIP, 'ps_packetloss', dateFrom, dateTo, chunks=1)
        o_data = ProcessDataInChunks(AggBySrcDestIP, 'ps_owd', dateFrom, dateTo, chunks=1)

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



def AggBySrcDestIP(idx, time_from, time_to):
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
                        "ipv6" : {
                          "terms" : {
                            "field" : "ipv6",
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
        data.append({'dest_host':item['key']['dest_host'], 'src_host':item['key']['src_host'], 'ipv6':item['key']['ipv6'],
                     field: item['mean_field']['value'], 'num_tests': item['doc_count']})

    return data



def ProcessDataInChunks(func, idx, dateFrom, dateTo, chunks):
    start = time.time()
    print('>>> Main process starts for index', idx, 'at:', time.strftime("%H:%M:%S", time.localtime()))

    time_range = list(hp.GetTimeRanges(dateFrom, dateTo, chunks))

    data = [] 
    for i in range(len(time_range)-1):
        curr_t = time_range[i]
        next_t = time_range[i+1]
        print('interval' , i, curr_t, next_t)
        results = func(idx, curr_t, next_t)
        prdata = hp.ProcessHosts(index=idx, data=results, tsFrom=time_range[0], tsTo=time_range[-1], saveUnresolved=True)

        percetage = round(((len(results)-len(prdata))/ len(results))*100) if len(results) > 0 else 0
        print('before:', len(results), 'after:', len(prdata), 'reduced by', percetage, '%')

        data.extend(prdata)

    print('Number of active hosts: total(',len(hp.hosts),') - unresolved(',len(hp.unresolved),') = ',len(hp.hosts) - len(hp.unresolved))
    print(">>> Overall elapsed = %ss" % (int(time.time() - start)))

    return data