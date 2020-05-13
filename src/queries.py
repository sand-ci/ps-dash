from datetime import datetime, timedelta
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import helpers as hp



def GetNTP():
    print('Collect NTP data from ps_meta...')
    dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
    dateFrom = datetime.strftime(datetime.now() - timedelta(hours = 24), '%Y-%m-%d %H:%M')
    ntp_period = hp.GetTimeRanges(dateFrom, dateTo, 1)
    ntp_ts = [next(ntp_period), next(ntp_period)]

    query = {
        "size": 0,
        "query" : {
          "bool" : {
            "must" : [
              {
                "range" : {
                  "timestamp" : {
                    "from" : ntp_ts[0],
                    "to" : ntp_ts[1]
                  }
                }
              }
            ],
            "adjust_pure_negative" : "true"
          }
        },
        "_source" : False,
        "aggregations" : {
          "groupby" : {
            "composite" : {
              "size" : 10000,
              "sources" : [
                {
                  "host" : {
                    "terms" : {
                      "field" : "host.keyword",
                      "order" : "asc"
                    }
                  }
                }
              ]
            },
            "aggregations" : {
              "ts": {
                "date_histogram": {
                    "field": "timestamp",
                    "fixed_interval": "1d"
                },
                "aggs": {
                    "ntp_delay": {
                        "avg": {
                            "field": "ntp.delay"
                        }
                    }
                }
              }
            }
          }
        }
      }

    results = hp.es.search(index='ps_meta', body=query)
    data = []
    for item in results["aggregations"]["groupby"]["buckets"]:
        data.append({'host': item['key']['host'].lower(), 'ts': item['ts']['buckets'][0]['key'],
                     'ntp_delay': item['ts']['buckets'][0]['ntp_delay']['value']})

    df = pd.DataFrame(data)
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df.set_index('ts', inplace = True)

    return df



def AggBySrcDestIPv6QueryHost(idx, time_from, time_to, args):
    host = args[0]
    field = hp.getValueField(idx)

    hfields = list(hp.es.indices.get_mapping(str(idx+'*')).values())[0]['mappings']['properties'].keys()
    res = []
    for es_field in hfields:
         if 'host' in es_field:
            query = {
                      "size" : 0,
                      "query" : {
                        "bool" : {
                          "must" : [
                            {
                              "term" : {
                                es_field : {
                                  "value" : host
                                }
                              }
                            },
                            {
                              "range" : {
                                "timestamp" : {
                                  "from" : time_from,
                                  "to" : time_to
                                }
                              }
                            }
                          ],
                          "adjust_pure_negative" : True
                        }
                      },
                      "_source" : False,
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
                              },
                              {
                                "ipv6" : {
                                  "terms" : {
                                    "field" : "ipv6",
                                    "missing_bucket" : True,
                                    "order" : "asc"
                                  }
                                }
                              }
                            ]
                          },
                          "aggregations" : {
                            "ts": {
                              "date_histogram": {
                                  "field": "timestamp",
                                  "fixed_interval": "1h"
                              },
                              "aggs": {
                                  field: {
                                      "avg": {
                                          "field": field
                                      }
                                  }
                              }
                            }
                          }
                        }
                      }
                    }

            results = hp.es.search(index=idx, body=query)
            temp = []
            for item in results["aggregations"]["groupby"]["buckets"]:
                for p in item['ts']['buckets']:
                    temp.append({'dest_host':item['key']['dest_host'], 'src_host':item['key']['src_host'],
                                 'ipv6':item['key']['ipv6'], 'ts': p['key'],field: p[field]['value'],
                                 'doc_count': p['doc_count']})
            res.extend(temp)

    return res



def QueryNTPDelay(idx, time_from, time_to, args):
    host = args[0]
    query = {
              "size" : 0,
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "term" : {
                        "host.keyword" : {
                          "value" : host
                        }
                      }
                    },
                    {
                      "range" : {
                        "timestamp" : {
                          "from" : time_from,
                          "to" : time_to
                        }
                      }
                    }
                  ]
                }
              },
                    "aggregations" : {
                    "groupby": {
                      "date_histogram": {
                          "field": "timestamp",
                          "fixed_interval": "1h"
                      },
                      "aggs": {
                          "ntp_delay": {
                              "avg": {
                                  "field": "ntp.delay"
                              }
                          }
                      }
                    }
                  },
              "_source" : False,
              "sort" : [
                {
                  "_doc" : {
                    "order" : "asc"
                  }
                }
              ]
            }

    results = hp.es.search(index=idx, body=query)
    data = []
    for item in results["aggregations"]["groupby"]["buckets"]:
#         if item['doc_count'] > 0:
#             print(host, item, item['key'],  item['ntp_delay'])
        data.append({'host': host, 'ts': item['key'], 'ntp_delay': item['ntp_delay']['value']})

#     print(data)
    return data



def queryAvgValuebyHost(idx, fromDate, toDate):
    val_fld = hp.getValueField(idx)

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



def AggBySrcDestIP(idx, time_from, time_to):
    val_fld = hp.getValueField(idx)

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
                        "field" : val_fld
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
                     val_fld: item['mean_field']['value'], 'num_tests': item['doc_count']})

    return data