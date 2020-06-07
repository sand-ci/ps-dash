from datetime import datetime, timedelta
import pandas as pd
import itertools

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import helpers as hp
import time


def GetPairsForAHost(idx, time_from, time_to, args):
    host = args[0]
    field = hp.getValueField(idx)

    def runQuery(host):
        query = {
          "size" : 0,
          "query" : {
            "bool" : {
              "must" : [
                {
                  "bool" : {
                    "should" : [
                      {
                        "term" : {
                          "src_host" : {
                            "value" : host
                          }
                        }
                      },
                      {
                        "term" : {
                          "dest_host" : {
                            "value" : host
                          }
                        }
                      }
                    ]
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
          "_source" : False,
          "aggregations" : {
            "groupby" : {
              "composite" : {
                "size" : 9999,
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
                field : {
                  "avg" : {
                    "field" : field
                  }
                }
              }
            }
          }
        }

#         print(str(query).replace("\'", "\""))
        results = hp.es.search(index=idx, body=query)
        res = []
        for item in results["aggregations"]["groupby"]["buckets"]:
            res.append({'src_host':item['key']['src_host'], 'dest_host':item['key']['dest_host'],
                        field: item[field]['value']})
        return res

    data, host_items = [], []

    for k,v in hp.hosts.items():
        if host == v:
            output = runQuery(k)
            if len(output) > 0:
                data.extend(output)

    return data



def GetNTP(date_from, date_to, args):
    src = args[0]
    dest = args[1]
    def runQuery(host):
        query = {
            "size": 0,
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
                          "from" : date_from,
                          "to" : date_to
                        }
                      }
                    }
                  ]
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
                        "fixed_interval": "24h"
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
        res = {}
        for item in results["aggregations"]["groupby"]["buckets"]:
            ntp = item['ts']['buckets'][0]['ntp_delay']['value']
            if ntp is not None:
                res[item['key']['host']] = round(ntp, 4)
            else: res[item['key']['host']] = 'None'

#         if host not in res:
#             res[host] = 'N/A'
        return res

    data = []
    items = []
    for k,v in hp.hosts.items():
        if src == v:
            runQuery(k)
            data.append(runQuery(k))
        elif dest == v:
            runQuery(k)
            data.append(runQuery(k))


    return data



def AggBySrcDestIPv6QueryHostV1(idx, time_from, time_to, args):
    host = args[0]
    es_field = args[1]
    intv = args[2]
    field = hp.getValueField(idx)
    res = []
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
                          "fixed_interval": intv
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



def PairAverageValuesQuery(idx, time_from, time_to, args):
    src = args[0]
    dest = args[1]

    field = hp.getValueField(idx)

    def runQuery(src, dest):
        query = {
                  "size" : 0,
                  "query" : {
                    "bool" : {
                      "must" : [
                        {
                          "term" : {
                            "src_host" : {
                              "value" : src
                            }
                          }
                        },
                        {
                          "term" : {
                            "dest_host" : {
                              "value" : dest
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
                              "fixed_interval": "30m"
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
        res = []
        for item in results["aggregations"]["groupby"]["buckets"]:
            for p in item['ts']['buckets']:
                res.append({'ipv6':item['key']['ipv6'], 'ts': p['key'], field: p[field]['value'],
                                     'doc_count': p['doc_count']})
        return res



    # In case an IP was replaced by the host name at a previous step we need to find all possible values for the given hosts and run the query with each combination of the two
    data = []
    src_items, dest_items = [], []
    for k,v in hp.hosts.items():
        if src == v:
            src_items.append(k)
        if dest == v:
            dest_items.append(k)

    combinations = list(itertools.product(src_items, dest_items))
    print(combinations)
    for c in combinations:
        output = runQuery(c[0], c[1])
        if len(output) > 0:
            data.extend(output)

    return data



def PairSymmetryQuery(idx, date_from, date_to, args):
    src = args[0]
    dest = args[1]
    ipv6 = args[2]
    field = hp.getValueField(idx)

    def runQuery(src, dest):
        query = {
              "size" : 1,
              "_source": "ipv6",
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "term" : {
                        "src_host" : {
                          "value" : src
                        }
                      }
                    },
                    {
                      "term" : {
                        "dest_host" : {
                          "value" : dest
                        }
                      }
                    },
                    {
                      "term" : {
                        "ipv6" : {
                          "value" : ipv6
                        }
                      }
                    },
                    {
                      "range" : {
                        "timestamp" : {
                          "from" : date_from,
                          "to": date_to
                        }
                      }
                    }
                  ]
                }
              }
            }

        results = hp.es.search(index=idx, body=query)

        if results["hits"]["total"]["value"] > 0:
            return True
        return False

    data = {}
    src_items, dest_items = [], []
    for k,v in hp.hosts.items():
        if src == v:
            src_items.append(k)
        if dest == v:
            dest_items.append(k)

    combinations = list(itertools.product(src_items, dest_items))
    for c in combinations:
        output = runQuery(c[0], c[1])
        data[(c[0], c[1])] = output

    return data



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
                                  "fixed_interval": "30m"
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
        data.append({'host': host, 'ts': item['key'], 'ntp_delay': item['ntp_delay']['value']})

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