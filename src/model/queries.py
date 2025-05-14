import traceback
from elasticsearch.helpers import scan
from datetime import datetime, timedelta
import pandas as pd
from dateutil.parser import parse

import utils.helpers as hp

import urllib3
urllib3.disable_warnings()




  

# On 18 Nov 2023 all alarms moved to querying netsites instead of sites, 
# but kept src_site and dest_site for backward compatibility
def obtainFieldNames(dateFrom):
  dateFrom = parse(dateFrom)

  target_date = datetime(2023, 11, 18, tzinfo=dateFrom.tzinfo)

  if dateFrom > target_date:
    return 'src_netsite', 'dest_netsite'
  else:
    return 'src_site', 'dest_site'


def queryThroughputIdx(dateFrom, dateTo):
  # dateFrom = datetime.fromisoformat(dateFrom)
  # dateTo = datetime.fromisoformat(dateTo)
  query = {
    "bool": {
      "must": [
        {
          "range": {
            "timestamp": {
              "gt": dateFrom,
              "lte": dateTo,
              "format": "strict_date_optional_time"
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

  
  src_field_name, dest_field_name = obtainFieldNames(dateFrom)

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
                "field": src_field_name
              }
            }
          },
          {
            "dest_site": {
              "terms": {
                "field": dest_field_name
              }
            }
          },
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

  aggdata = hp.es.search(index='ps_throughput', query=query, aggregations=aggregations, _source=False)
  for item in aggdata['aggregations']['groupby']['buckets']:
      aggrs.append({'hash': str(item['key']['src'] + '-' + item['key']['dest']),
                    'from': dateFrom, 'to': dateTo,
                    'ipv6': item['key']['ipv6'],
                    'src': item['key']['src'].upper(), 'dest': item['key']['dest'].upper(),
                    'src_host': item['key']['src_host'], 'dest_host': item['key']['dest_host'],
                    'src_site': item['key']['src_site'].upper(), 'dest_site': item['key']['dest_site'].upper(),
                    'value': item['throughput']['value'],
                    'doc_count': item['doc_count']
                    })

  return aggrs


def query_ASN_paths_pos_probs(src, dest, dt, ipv):
  """
  Fetch the document with this alarm_id, and render its heatmap.
  """
  try:
      must_conditions = [
          {
            "exists": {
              "field": "transitions"
            }
              },
              {
            "range": {
              "to_date": {
                "gte": dt,
                "format": "strict_date_optional_time"
              }
            }
              },
              {
            "term": {
              "src_netsite.keyword": src
            }
              },
              {
            "term": {
              "dest_netsite.keyword": dest
            }
          }
      ]

      if ipv != -1:
          ipv_q = {
            "term": {
              "ipv6": True if ipv == 1 else False
            }
          }
          must_conditions.append(ipv_q)

      q = {
          "bool": {
          "must": must_conditions
          }
      }
      # print(str(q).replace('\'', '"'))
      res = hp.es.search(index='ps_traces_changes', query=q)
  except Exception:
      # not found / error
      return []

  doc = res['hits']['hits'][0]["_source"]
  return doc


def queryPathAnomaliesDetails(dateFrom, dateTo, idx='ps_traces_changes'):
    query = {
        "bool": {
            "must": [
                {"exists": {"field": "transitions"}},
                  {
                  "range": {
                    "to_date": {
                      "format": "strict_date_optional_time",
                      "gte": dateFrom,
                      # "lte": dateTo,
                    }
                  }
                }
            ]
        }
    }
    res = hp.es.search(index=idx, query=query, size=10000)
    # collect every transition record from every hit
    records = []
    for hit in res["hits"]["hits"]:
        for t in hit["_source"].get("transitions", []):
            records.append(t)
    df = pd.DataFrame(records)
    return df


def queryAlarms(dateFrom, dateTo):
  period = hp.GetTimeRanges(dateFrom, dateTo)
  print(period)
  # this is the list of events currently tracked in pSDash interface
  allowed_events = ['bad owd measurements',
                    'large clock correction',
                    'high packet loss',
                    'complete packet loss',
                    'high packet loss on multiple links',
                    'firewall issue',
                    'ASN path anomalies',
                    'destination cannot be reached from any',
                    'destination cannot be reached from multiple',
                    'source cannot reach any',
                    'hosts not found',
                    'unresolvable host',
                    'bandwidth decreased',
                    'bandwidth increased',
                    'bandwidth increased from/to multiple sites',
                    'bandwidth decreased from/to multiple sites']
  q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "created_at": {
                                "gte": period[0],
                                "lte": period[1],
                                "format": "strict_date_optional_time"
                            }
                        }
                    },
{
                        "term": {
                            "category": {
                                "value": "Networking",
                                "boost": 1.0
                            }
                        }
                    },
                    {
                        "terms": {
                            "event": allowed_events
                        }
                    }
                ]
            }
        }
      }
  # print(str(q).replace("\'", "\""))
  try:
    result = scan(client=hp.es, index='aaas_alarms', query=q)
    data = {}

    for item in result:
        event = item['_source']['event']
        temp = []
        if event in data.keys():
            temp = data[event]

        if 'source' in item['_source'].keys():
          desc = item['_source']['source']
          if 'tags' in item['_source'].keys():
            tags = item['_source']['tags']
            desc['tag'] = tags

          if 'to' not in desc.keys():
            desc['to'] = pd.to_datetime(item['_source']['created_at'], unit='ms', utc=True)

          if 'from' in desc.keys() and 'to' in desc.keys():
            desc['from'] = desc['from'].replace('T', ' ')
            desc['to'] = desc['to'].replace('T', ' ')
          
          if 'to_date' in desc.keys():
             desc['to'] = desc['to_date'].split('T')[0]

          if 'avg_value%' in desc.keys():
              desc['avg_value'] = desc['avg_value%']
              desc.pop('avg_value%')
          
          temp.append(desc)

          data[event] = temp

    return data
  except Exception as e:
    print('Exception:', e)
    print(traceback.format_exc())



def getASNInfo(ids):
    query = {
        "query": {
            "terms": {
                "_id": ids
            }
        }
    }

    # print(str(query).replace("\'", "\""))
    asnDict = {}
    try:
      data = scan(hp.es, index='ps_asns', query=query)
      if data:
        for item in data:
            asnDict[str(item['_id'])] = item['_source']['owner']
      else:
          print(ids, 'Not found')
    except Exception as e:
      print('Exception:', e)
      print(traceback.format_exc())

    return asnDict


# TODO: start querying form ps_meta
def getMetaData():
    meta = []
    data = scan(hp.es, index='ps_alarms_meta')
    for item in data:
        meta.append(item['_source'])

    if meta:
        return pd.DataFrame(meta)
    else:
        print('No metadata!')


def getAlarm(id):
  q = {
      "term": {
          "source.alarm_id": id
      }
  }
  data = []
  results = hp.es.search(index='aaas_alarms', size=100, query=q)
  for res in results['hits']['hits']:
    data.append(res['_source'])

  # for d in data: print(d)
  if len(data) >0:
    return data[0]


def getCategory(event):

  q = {
      "term": {
          "event": event
      }
  }


  results = hp.es.search(index='aaas_categories', query=q)

  for res in results['hits']['hits']:
    return res['_source']
  

def getSubcategories():
  # TODO: query from aaas_categories when the alarms are reorganized
  # q = {
  #       "query": {
  #           "term": {
  #             "category": "Networking"
  #         }
  #       }
  #     }

  # results = scan(hp.es, index='aaas_categories', query=q)

  # subcategories = []

  # for res in results:
  #     subcategories.append({'event': res['_source']['event'], 'category': res['_source']['subcategory']})
  # # "path changed between sites" is not in aaas_alarms, because the alarms are too many
  # # the are gouped under an ASN in path_changed alarm
  # subcategories.append({'event': 'path changed between sites', 'category': 'RENs'})

  # catdf = pd.DataFrame(subcategories)

  description = {
  'Infrastructure': 	['bad owd measurements','large clock correction',
	 		 'destination cannot be reached from multiple', 'destination cannot be reached from any',
			 'source cannot reach any', 'firewall issue', 'complete packet loss',
                       	 'unresolvable host', 'hosts not found'],

  'Network': 		 ['bandwidth decreased from/to multiple sites',
                          'ASN path anomalies'],

  'Other': 		 ['bandwidth increased from/to multiple sites', 'bandwidth increased', 'bandwidth decreased',
                          'high packet loss', 'high packet loss on multiple links']
  }

  subcategories = []
  for cat, ev in description.items():
    for e in ev:
      subcategories.append({'category': cat, 'event': e})

  catdf = pd.DataFrame(subcategories)

  return catdf


def query_ASN_anomalies(src, dest, dt):
  dateFrom = f"{dt}T00:00:00"
  dateTo = f"{dt}T23:59:59"
  q = {
    "query": {
      "bool": {
        "must": [
          {
            "range": {
              "to_date": {
                "gte": dateFrom,
                "lte": dateTo,
                "format": "strict_date_optional_time"
              }
            }
          },
          {
            "term": {
              "event.keyword": "ASN path anomalies"
            }
          },
          {
            "term": {
              "src_netsite.keyword": src
            }
          },
          {
            "term": {
              "dest_netsite.keyword": dest
            }
          }
        ]
      }
    }
  }

  # print(str(q).replace("\'", "\""))
  fields = ['ipv6', 'src_netsite', 'dest_netsite', 'last_appearance_path', 'repaired_asn_path', 'anomalies', 'paths']
  result = scan(client=hp.es, index='ps_traces_changes', query=q, source=fields)

  data = []
  for item in result:
    temp = item['_source']
    for el in temp['paths']:
      temp_copy = temp.copy()
      temp_copy['last_appearance_path'] = el['last_appearance_path']
      temp_copy['repaired_asn_path'] = el['repaired_asn_path']
      temp_copy['path_len'] = len(el['repaired_asn_path'])
      data.append(temp_copy)

  ddf = pd.DataFrame(data)
  return ddf


def query4Avg(idx, dateFrom, dateTo):
  # TODO: stick to 1 date format
  # dateFrom = convertDate(dateFrom)
  # dateTo = convertDate(dateTo)
  val_fld = hp.getValueField(idx)
  src_field_name, dest_field_name = obtainFieldNames(dateFrom)
  query = {
            "size" : 0,
            "query" : {
              "bool" : {
                "must" : [
                  {
                    "range" : {
                      "timestamp" : {
                        "gt" : dateFrom,
                        "lte": dateTo,
                        "format": "strict_date_optional_time"
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
                    },
                    {
                      "src_site" : {
                        "terms" : {
                          "field" : src_field_name
                        }
                      }
                    },
                    {
                      "dest_site" : {
                        "terms" : {
                          "field" : dest_field_name
                        }
                      }
                    },
                    {
                      "src_host" : {
                        "terms" : {
                          "field" : "src_host"
                        }
                      }
                    },
                    {
                      "dest_host" : {
                        "terms" : {
                          "field" : "dest_host"
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

  aggdata = hp.es.search(index=idx, body=query, _source=False)
  for item in aggdata['aggregations']['groupby']['buckets']:
      aggrs.append({'pair': str(item['key']['src']+'-'+item['key']['dest']),
                    'src': item['key']['src'], 'dest': item['key']['dest'],
                    'src_host': item['key']['src_host'], 'dest_host': item['key']['dest_host'],
                    'src_site': item['key']['src_site'], 'dest_site': item['key']['dest_site'],
                    'value': item[val_fld]['value'],
                    'from': dateFrom, 'to': dateTo,
                    'doc_count': item['doc_count']
                    })

  return aggrs


def queryBandwidthIncreasedDecreased(dateFrom, dateTo, sips, dips, ipv6):
    q = {
          "query" : {  
              "bool" : {
              "must" : [
                    {
                      "range": {
                          "timestamp": {
                          "gte": dateFrom,
                          "lte": dateTo,
                          "format": "strict_date_optional_time"
                          }
                        }
                    },
                    {
                      "terms" : {
                        "src": sips
                      }
                    },
                    {
                      "terms" : {
                        "dest": dips
                      }
                    },
                    {
                      "term": {
                          "ipv6": {
                            "value": ipv6
                          }
                      }
                    }
                ]
              }
            }
          }
      # print(str(q).replace("\'", "\""))

    result = scan(client=hp.es,index='ps_throughput',query=q)
    data = []

    for item in result:
        data.append(item['_source'])

    df = pd.DataFrame(data)

    df['pair'] = df['src']+'->'+df['dest']
    df['dt'] = df['timestamp']
    return df

def getSiteMetadata(site, date_from=None, date_to=None, index='ps_meta'):
    """
    Fetch metadata using Elasticsearch scan with date range filtering,
    including cpu_cores, cpus
    """
    # Set default dates if not provided
    if date_to is None:
        date_to = datetime.utcnow()
    else:
        date_to = datetime.strptime(date_to, '%Y-%m-%d') if isinstance(date_to, str) else date_to
    
    if date_from is None:
        date_from = date_to - timedelta(days=365)
    else:
        date_from = datetime.strptime(date_from, '%Y-%m-%d') if isinstance(date_from, str) else date_from
    
    date_format = 'strict_date_optional_time'
    date_from_str = date_from.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    date_to_str = date_to.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"match_phrase": {"netsite": site}},
                    {"range": {
                        "timestamp": {
                            "format": date_format,
                            "gte": date_from_str,
                            "lte": date_to_str
                        }
                    }}
                ]
            }
        },
        "sort": [{"timestamp": {"order": "desc", "format": date_format}}],
        "_source": {
            "includes": ["*", "cpu_cores", "cpus", "wlcg-role"],  # Include all fields plus our specific ones
            "excludes": []  # No exclusions
        }
    }
    
    try:
        results = scan(
            hp.es,
            index=index,
            query=query,
            preserve_order=True
        )
        
        meta = []
        for hit in results:
            doc = hit['_source']
            # Ensure the fields exist in the document
            doc.setdefault('cpu_cores', None)
            doc.setdefault('cpus', None)
            meta.append(doc)
        
        if meta:
            return pd.DataFrame(meta)
        else:
            print(f"No metadata found for site '{site}' between {date_from_str} and {date_to_str}")
            return None
            
    except Exception as e:
        print(f"Error fetching metadata: {str(e)}")
        return None