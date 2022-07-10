import pandas as pd
import itertools
from elasticsearch.helpers import scan

import utils.helpers as hp

import urllib3
urllib3.disable_warnings()

def alarms(period):
    q = {
      "query" : {
        "bool": {
          "must": [
              {
                "range" : {
                  "created_at" : {
                    "from" : period[0],
                    "to": period[1]
                  }
                }
              },
              {
              "term" : {
                "subcategory" : {
                  "value" : "Perfsonar",
                  "boost" : 1.0
                }
              }
            }
          ]
        }
      }
    }
    result = scan(client=hp.es,index='aaas_alarms',query=q)
    data = {}

    for item in result:
        event = item['_source']['event']
        temp = []
        if event in data.keys(): 
            temp = data[event]
        
        desc = item['_source']['source']
        desc['tag'] = item['_source']['tags']
        temp.append(item['_source']['source'])
        data[event] = temp

    return data



def allTestedNodes(period):
    def query(direction):
        return {
      "size" : 0,
      "query" : {
        "bool" : {
          "must" : [
            {
              "range" : {
                "timestamp" : {
                  "gt" : period[0],
                  "lte": period[1]
                }
              }
            }
          ]
        }
      },
      "aggregations" : {
        "groupby" : {
          "composite" : {
            "size" : 9999,
            "sources" : [
              {
                direction : {
                  "terms" : {
                    "field" : "src"
                  }
                }
              },
              {
                f"{direction}_host" : {
                  "terms" : {
                    "field" : "src_host"
                  }
                }
              },
              {
                f"{direction}_site" : {
                  "terms" : {
                    "field" : "src_site"
                  }
                }
              },
              {
                "ipv6" : {
                  "terms" : {
                    "field" : "ipv6"
                  }
                }
              }
            ]
          }
        }
      }
    }
    aggrs = []
    pairsDf = pd.DataFrame()
    for idx in hp.INDICES:
        aggdata = hp.es.search(index=idx, body=query('src'))
        aggrs = []
        for item in aggdata['aggregations']['groupby']['buckets']:
            aggrs.append({
                          'ip': item['key']['src'],
                          'ipv6': item['key']['ipv6'],
                          'host': item['key']['src_host'],
                          'site': item['key']['src_site'],
            })

        aggdata = hp.es.search(index=idx, body=query('dest'))
        for item in aggdata['aggregations']['groupby']['buckets']:
            aggrs.append({
                          'ip': item['key']['dest'],
                          'ipv6': item['key']['ipv6'],
                          'host': item['key']['dest_host'],
                          'site': item['key']['dest_site'],
                         })
    
            
        pairsDf = pairsDf.append(aggrs)
        pairsDf = pairsDf.drop_duplicates()
        print(idx, 'Len unique nodes ',len(pairsDf), 'period:', period)
    return pairsDf



def mostRecentMetaRecord(ip, ipv6, period):
    forTimeRange=''
    if period:
        forTimeRange = {
                  "range" : {
                    "timestamp" : {
                      "gt" : period[0],
                      "lte": period[1]
                    }
                  }
                }

    def q(ip, ipv):
        return {
          "size" : 1,
          "_source": ["geolocation",f"external_address.{ipv}_address", "config.site_name", "host","administrator.name","administrator.email","timestamp"],  
            "sort" : [
            {
              "timestamp" : {
                "order" : "desc"
              }
            }
          ],
          "query" : {
            "bool" : {
              "must" : [
                forTimeRange,
                {
                  "term" : {
                    f"external_address.{ipv}_address" : {
                      "value" : ip
                    }
                  }
                },
                {
                  "bool": {
                    "should": [
                      {
                        "exists": {"field": "host"}
                      },
                      {
                        "exists": {"field": "config.site_name"}
                      },
                      {
                        "exists": {"field": "geolocation"}
                      },
                      {
                        "exists": {"field": "administrator.email"}
                      }
                    ]
                  }
                }
              ]
            }
          }
        }
    
    
    ipv = 'ipv6' if ipv6 == True else 'ipv4'
#     print(str(q).replace("\'", "\""))
    values = {}
    data = hp.es.search(index='ps_meta', body=q(ip,ipv))

    if data['hits']['hits']:
        records = data['hits']['hits'][0]['_source']
        values['ip'] = ip
        if 'timestamp' in records:
            values['timestamp'] = records['timestamp']
        if 'host' in records:
            values['host'] = records['host']
        if 'config' in records:
            if 'site_name' in records['config']:
                values['site_meta'] = records['config']['site_name']
        if 'administrator' in records:
            if 'name' in records['administrator']:
                values['administrator'] = records['administrator']['name']
            if 'email' in records['administrator']:
                values['email'] = records['administrator']['email']
        if 'geolocation' in records:
            values['lat'], values['lon'] = records['geolocation'].split(",")
    return values



def queryIndex(datefrom, dateto, idx):
    print('query ^^^^^^^^^^^^^^^^^^^',datefrom,dateto,idx)
    query = {
        "query": {
            "bool": {
                    "filter": [
                    {
                        "range": {
                            "timestamp": {
                            "gte": datefrom,
                            "lt": dateto
                            }
                        }
                    }
                ]
            }
        }
      }
    try:
        data = scan(client=hp.es,index=idx,query=query)
        ret_data = {}
        count = 0
        last_entry = 0
        for item in data:
            if not count%10000: print(idx,count)
            ret_data[count] = item
            ret_data[count] = ret_data[count]['_source']
            count+=1

        return ret_data
    except Exception as e:
        print(traceback.format_exc())


def queryNodesGeoLocation():

    include=["geolocation","external_address.ipv4_address", "external_address.ipv6_address", "config.site_name", "host"]
    period = hp.GetTimeRanges(*hp.defaultTimeRange(days=30))

    query = {
            "query": {
                "bool": {
                        "filter": [
                        {
                            "range": {
                                "timestamp": {
                                "gte": period[0],
                                "lte": period[1]
                                }
                            }
                        }
                    ]
                }
            }
          }
    data = scan(client=hp.es, index='ps_meta', query=query,
                _source=include, filter_path=['_scroll_id', '_shards', 'hits.hits._source'])
       

    count = 0
    neatdata = []
    ddict = {}
    for res in data:
        if not count%100000: print(count)
        data = res['_source']

        if 'config' in data:
            site = data['config']['site_name']
        else: site = None

        if 'ipv4_address' in  data['external_address']:
            ip = data['external_address']['ipv4_address']
        else: ip = data['external_address']['ipv6_address']

        geoip = [None, None]
        if 'geolocation' in data:
            geoip = data['geolocation'].split(",")

#         if 'speed' in  data['external_address']:
#             speed = data['external_address']['speed']

        if (ip in ddict) and (site is not None):
            ddict[ip]['site'] = site
        else: ddict[ip] = {'lat': geoip[0], 'lon': geoip[1], 'site': site, 'host':data['host']}

        count=count+1
    return ddict


def queryAllTestedPairs(period):
    query = {
      "size" : 0,
      "query" : {
        "bool" : {
          "must" : [
            {
              "range" : {
                "timestamp" : {
                  "gt" : period[0],
                  "lte": period[1]
                }
              }
            },
            {
              "term" : {
                "src_production" : True
              }
            },
            {
              "term" : {
                "dest_production" : True
              }
            }
          ]
        }
      },
      "aggregations" : {
        "groupby" : {
          "composite" : {
            "size" : 9999,
            "sources" : [
              {
                "src" : {
                  "terms" : {
                    "field" : "src"
                  }
                }
              },
              {
                "dest" : {
                  "terms" : {
                    "field" : "dest"
                  }
                }
              }
            ]
          }
        }
      }
    }

    aggrs = []
    for idx in hp.INDECES:
        aggdata = hp.es.search(index=idx, body=query)
        for item in aggdata['aggregations']['groupby']['buckets']:
            aggrs.append({'idx': idx,
                          'src': item['key']['src'],
                          'dest': item['key']['dest']
                         })
    return aggrs


def queryAllValuesFromList(idx, fld_type, val_list, period):
    val_fld = hp.getValueField(idx)
    query = {
        "size": 0,
        "_source": {
            "includes": ["timestamp", "dest", "src", val_fld]
          },
        "query": {
          "bool": {
            "must": [
              {
                "range": {
                  "timestamp": {
                    "gte": period[0],
                    "lte": period[1]
                  }

                }
              },
              {
                "terms": {
                  fld_type: val_list
                }
              }
            ]
          }
        }
      }
    data = scan(client=es, index=idx, query=query, _source=["timestamp", "dest", "src", val_fld], filter_path=['_scroll_id', '_shards', 'hits.hits._source'])
#     scan(client=hp.es, index=idx, query=query)
#     print(idx, str(query).replace("\'", "\""))
    count = 0
    allData=[]
    for res in data:
        if not count%100000: print(count)
        allData.append(res['_source'])
        count=count+1

    return allData


def queryAllValues(idx, src, dest, period):
    val_fld = hp.getValueField(idx)
    query = {
            "size": 0,
            "_source": ["timestamp", val_fld],
            "sort": [
                {
                  "timestamp": {
                    "order": "asc"
                  }
                }
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": period[0],
                                    "lte": period[1]
                                }
                            }
                        },
                        {
                          "term" : {
                            "src" : {
                              "value" : src
                            }
                          }
                        },
                        {
                          "term" : {
                            "dest" : {
                              "value" : dest
                            }
                          }
                        },
                        {
                          "term" : {
                            "src_production" : True
                          }
                        },
                        {
                          "term" : {
                            "dest_production" : True
                          }
                        }
                    ]
                }
            }
            }

    data = scan(client=hp.es, index=idx, query=query)
#     print(idx, str(query).replace("\'", "\""))
    count = 0
    allData=[]
    for res in data:
        if not count%100000: print(count)
        allData.append(res['_source'])
        count=count+1
    return allData


def query4Avg(idx, dateFrom, dateTo):
    val_fld = hp.getValueField(idx)
    query = {
              "size" : 0,
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "range" : {
                        "timestamp" : {
                          "gt" : dateFrom,
                          "lte": dateTo
                        }
                      }
                    },
                    {
                      "term" : {
                        "src_production" : True
                      }
                    },
                    {
                      "term" : {
                        "dest_production" : True
                      }
                    }
                  ]
                }
              },
              "aggregations" : {
                "groupby" : {
                  "composite" : {
                    "size" : 9999,
                    "sources" : [
                      {
                        "src" : {
                          "terms" : {
                            "field" : "src"
                          }
                        }
                      },
                      {
                        "dest" : {
                          "terms" : {
                            "field" : "dest"
                          }
                        }
                      }
                    ]
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

    aggrs = []

    aggdata = hp.es.search(index=idx, body=query)
    for item in aggdata['aggregations']['groupby']['buckets']:
        aggrs.append({'pair': str(item['key']['src']+'-'+item['key']['dest']),
                      'src': item['key']['src'], 'dest': item['key']['dest'],
                      # 'src_host': item['key']['src'], 'dest_host': item['key']['dest'],
                      # 'src_site': item['key']['src'], 'dest_site': item['key']['dest'],
                      'value': item[val_fld]['value'],
                      'from': dateFrom,
                      'to': dateTo,
                      'doc_count': item['doc_count']
                     })

    return aggrs



def queryDailyAvg(idx, fld, dateFrom, dateTo):
    val_fld = hp.getValueField(idx)
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": dateFrom,
                                "lte": dateTo
                            }
                        }
                    },
                    {
                      "term" : {
                        "src_production" : True
                      }
                    },
                    {
                      "term" : {
                        "dest_production" : True
                      }
                    }
                ]
            }
        },
        "aggs": {
            "avg_values": {
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

    data = hp.es.search(index=idx, body=query)

    result = {}
    # i = 0
    for ip in data['aggregations']['avg_values']['buckets']:
        temp = {}
        for period in ip['period']['buckets']:
            temp[period['key']] = period[val_fld]['value']
        result[ip['key']] = temp

    return result


def get_ip_host(idx, dateFrom, dateTo):
    def q_ip_host (fld):
        return {
                  "size" : 0,
                  "query" : {  
                    "bool" : {
                      "must" : [
                        {
                          "range" : {
                            "timestamp" : {
                              "from" : dateFrom,
                              "to" : dateTo
                            }
                          }
                        },
                        {
                          "term" : {
                            "src_production" : True
                          }
                        },
                        {
                          "term" : {
                            "dest_production" : True
                          }
                        }
                      ]
                    }
                  },
                  "_source" : False,
                  "stored_fields" : "_none_",
                  "aggregations" : {
                    "groupby" : {
                      "composite" : {
                        "size" : 9999,
                        "sources" : [
                          {
                            fld : {
                              "terms" : {
                                "field" : fld,
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                            str(fld+"_host") : {
                              "terms" : {
                                "field" : str(fld+"_host"),
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                }

    res_ip_host = {}
    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_ip_host(field))

        for item in results["aggregations"]["groupby"]["buckets"]:
            ip = item['key'][field]
            host = item['key'][str(field+'_host')]
            if ((ip in res_ip_host.keys()) and (host is not None) and (host != ip)) or (ip not in res_ip_host.keys()):
                res_ip_host[ip] = host
    return res_ip_host


def get_ip_site(idx, dateFrom, dateTo):
    def q_ip_site (fld):
        return {
                  "size" : 0,
                  "query" : {  
                    "bool" : {
                      "must" : [
                        {
                          "range" : {
                            "timestamp" : {
                              "from" : dateFrom,
                              "to" : dateTo
                            }
                          }
                        },
                        {
                          "term" : {
                            "src_production" : True
                          }
                        },
                        {
                          "term" : {
                            "dest_production" : True
                          }
                        }
                      ]
                    }
                  },
                  "_source" : False,
                  "stored_fields" : "_none_",
                  "aggregations" : {
                    "groupby" : {
                      "composite" : {
                        "size" : 9999,
                        "sources" : [
                          {
                            fld : {
                              "terms" : {
                                "field" : fld,
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                            str(fld+"_site") : {
                              "terms" : {
                                "field" : str(fld+"_site"),
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
                      }
                    }
                  }
                }

    res_ip_site = {}
    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_ip_site(field))

        for item in results["aggregations"]["groupby"]["buckets"]:
            ip = item['key'][field]
            site = item['key'][str(field+'_site')]
            ipv6 = item['key']['ipv6']
            if ((ip in res_ip_site.keys()) and (site is not None)) or (ip not in res_ip_site.keys()):
                res_ip_site[ip] = [site, ipv6]
    return res_ip_site


def get_host_site(idx, dateFrom, dateTo):
    def q_host_site (fld):
        return {
          "size" : 0,
          "query" : {  
            "bool" : {
              "must" : [
                {
                  "range" : {
                    "timestamp" : {
                      "from" : dateFrom,
                      "to" : dateTo
                    }
                  }
                },
                {
                  "term" : {
                    "src_production" : True
                  }
                },
                {
                  "term" : {
                    "dest_production" : True
                  }
                }
              ]
            }
          },
          "_source" : False,
          "stored_fields" : "_none_",
          "aggregations" : {
            "groupby" : {
              "composite" : {
                "size" : 9999,
                "sources" : [
                  {
                    str(fld+"_site") : {
                      "terms" : {
                        "field" : str(fld+"_site"),
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    str(fld+"_host") : {
                      "terms" : {
                        "field" : str(fld+"_host"),
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  }
                ]
              }
            }
          }
        }

    res_host_site = {}
    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_host_site(field))

        for item in results["aggregations"]["groupby"]["buckets"]:
            site = item['key'][str(field+"_site")]
            host = item['key'][str(field+'_host')]
            if ((host in res_host_site.keys()) and (site is not None)) or (host not in res_host_site.keys()):
                res_host_site[host] = site
    return res_host_site


def get_metadata(dateFrom, dateTo):
    def q_metadata():
        return {
          "size" : 0,
          "query" : {
            "range" : {
              "timestamp" : {
                "from" : dateFrom,
                "to" : dateTo
              }
            }
          },
          "_source" : False,
          "aggregations" : {
            "groupby" : {
              "composite" : {
                "size" : 9999,
                "sources" : [
                  {
                    "site" : {
                      "terms" : {
                        "field" : "config.site_name.keyword",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "admin_email" : {
                      "terms" : {
                        "field" : "administrator.email",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "admin_name" : {
                      "terms" : {
                        "field" : "administrator.name",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "ipv6" : {
                      "terms" : {
                        "field" : "external_address.ipv6_address",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "ipv4" : {
                      "terms" : {
                        "field" : "external_address.ipv4_address",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "host" : {
                      "terms" : {
                        "field" : "host.keyword",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  }
                ]
              }
            }
          }
        }

    results = hp.es.search(index='ps_meta', body=q_metadata())
    res_meta = {}
    for item in results["aggregations"]["groupby"]["buckets"]:
        host = item['key']['host']
        if ((host in res_meta.keys()) and (item['key']['site'] is not None)) or (host not in res_meta.keys()):
            res_meta[host] = {'site': item['key']['site'], 'admin_name': item['key']['admin_name'],
                              'admin_email': item['key']['admin_email'], 'ipv6': item['key']['ipv6'],
                              'ipv4': item['key']['ipv4']}
    return res_meta



def GetPairsForAHost(idx, time_from, time_to, args):
    host = args[0]
    field = hp.getValueField(idx)
    print(host, field, idx, time_from, time_to)
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


def GetPairsForAHostV1(idx, time_from, time_to, args):
    host = args[0]
    field = hp.getValueField(idx)
    print(host, field, idx, time_from, time_to)
#     def runQuery(host):
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

    print(str(query).replace("\'", "\""))
    results = hp.es.search(index=idx, body=query)
    res = []
    for item in results["aggregations"]["groupby"]["buckets"]:
        res.append({'src_host':item['key']['src_host'], 'dest_host':item['key']['dest_host'],
                    field: item[field]['value']})
    return res



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

    results = hp.es.search(index=idx, body=query)

    data = []
    for item in results["aggregations"]["groupby"]["buckets"]:
        data.append({'dest_host':item['key']['dest_host'], 'src_host':item['key']['src_host'], 'ipv6':item['key']['ipv6'],
                     val_fld: item['mean_field']['value'], 'num_tests': item['doc_count']})

    return data