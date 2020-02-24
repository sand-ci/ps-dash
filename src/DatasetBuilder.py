import re
import socket
import ipaddress

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import numpy as np
import pandas as pd

import helpers as hp


def ConnectES():
    user = None
    passwd = None
    if user is None and passwd is None:
        with open("/etc/ps-dash/creds.key") as f:
            user = f.readline().strip()
            passwd = f.readline().strip()
    credentials = (user, passwd)
    es = Elasticsearch(['atlas-kibana.mwt2.org:9200'], timeout=240, http_auth=credentials)

    if es.ping() == True:
        return es
    else:
        print("Connection Unsuccessful")


def queryAvgPacketLossbyHost(fld, group, fromDate, toDate):
    es = ConnectES()

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

    data = es.search("ps_packetloss", body=query)

    result = []
    unknown = []

    for host in data['aggregations']['host']['buckets']:
        resolved = hp.ResolveHost(es, host['key'])
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
