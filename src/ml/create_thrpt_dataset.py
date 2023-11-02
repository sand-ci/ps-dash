import pandas as pd

import model.queries as qrs
import utils.helpers as hp

def query4Avg(dateFrom, dateTo):
    query = {
        "bool": {
            "must": [
                {
                    "range": {
                        "timestamp": {
                            "gt": dateFrom,
                            "lte": dateTo
                        }
                    }
                },
                {
                    "term": {
                        "src_production": True
                    }
                },
                {
                    "term": {
                        "dest_production": True
                    }
                }
            ]
        }
    }

    aggregations = {
        "groupby": {
            "composite": {
                "size": 9999,
                "sources": [
                    {
                        "ipv6": {
                            "terms": {
                                "field": "ipv6"
                            }
                        }
                    },
                    {
                        "src": {
                            "terms": {
                                "field": "src"
                            }
                        }
                    },
                    {
                        "dest": {
                            "terms": {
                                "field": "dest"
                            }
                        }
                    },
                    {
                        "src_host": {
                            "terms": {
                                "field": "src_host"
                            }
                        }
                    },
                    {
                        "dest_host": {
                            "terms": {
                                "field": "dest_host"
                            }
                        }
                    },
                    {
                        "src_site": {
                            "terms": {
                                "field": "src_site"
                            }
                        }
                    },
                    {
                        "dest_site": {
                            "terms": {
                                "field": "dest_site"
                            }
                        }
                    }
                ]
            },
            "aggs": {
                "throughput": {
                    "avg": {
                        "field": "throughput"
                    }
                }
            }
        }
    }

    #     print(idx, str(query).replace("\'", "\""))
    aggrs = []

    aggdata = hp.es.search(index='ps_throughput', query=query, aggregations=aggregations)
    for item in aggdata['aggregations']['groupby']['buckets']:
        aggrs.append({'hash': str(item['key']['src'] + '-' + item['key']['dest']),
                      'from': dateFrom, 'to': dateTo,
                      'ipv6': item['key']['ipv6'],
                      'src': item['key']['src'], 'dest': item['key']['dest'],
                      'src_host': item['key']['src_host'], 'dest_host': item['key']['dest_host'],
                      'src_site': item['key']['src_site'], 'dest_site': item['key']['dest_site'],
                      'value': item['throughput']['value'],
                      'doc_count': item['doc_count']
                      })

    return aggrs


# Fill in hosts and site names where missing by quering the ps_alarms_meta index
def fixMissingMetadata(rawDf):
    metaDf = qrs.getMetaData()
    rawDf['pair'] = rawDf['src'] + rawDf['dest']
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='src', how='right').rename(
        columns={'host': 'host_src', 'site': 'site_src'}).drop(columns=['ip'])
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='dest', how='right').rename(
        columns={'host': 'host_dest', 'site': 'site_dest'}).drop(columns=['ip'])

    rawDf['src_site'] = rawDf['site_src'].fillna(rawDf.pop('src_site'))
    rawDf['dest_site'] = rawDf['site_dest'].fillna(rawDf.pop('dest_site'))
    rawDf['src_host'] = rawDf['host_src'].fillna(rawDf.pop('src_host'))
    rawDf['dest_host'] = rawDf['host_dest'].fillna(rawDf.pop('dest_host'))

    return rawDf


def queryData(dateFrom, dateTo):
    data = []
    # query in portions since ES does not allow aggregations with more than 10000 bins
    intv = int(hp.CalcMinutes4Period(dateFrom, dateTo) / 60)
    time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
    for i in range(len(time_list) - 1):
        data.extend(query4Avg(time_list[i], time_list[i + 1]))

    return data


def getStats(df):
    print('before', len(df))
    df = fixMissingMetadata(df)
    print('after', len(df))
    # convert to MB
    df['value'] = round(df['value'] * 1e-6)

    return df

def createThrptDataset(dateTo, dateFrom):

    # dateTo = '2023-05-31 07:04'
    # dateFrom = '2023-01-01 04:28'
    print(f'----- {dateFrom} - {dateTo} ----- ')

    # get the data
    rawDf = pd.DataFrame(queryData(dateFrom, dateTo))
    rawDf['dt'] = pd.to_datetime(rawDf['from'], unit='ms')
    rawDf['src_site'] = rawDf['src_site'].str.upper()
    rawDf['dest_site'] = rawDf['dest_site'].str.upper()

    booleanDictionary = {True: 'ipv6', False: 'ipv4'}
    rawDf['ipv'] = rawDf['ipv6'].map(booleanDictionary)

    # calculate the statistics
    rawDf = getStats(rawDf)
    return rawDf