import pandas as pd
from elasticsearch.helpers import scan
from datetime import datetime
import traceback

import utils.helpers as hp

import urllib3
urllib3.disable_warnings()


def queryPathChanged(dateFrom, dateTo):
    start = datetime.strptime(dateFrom, '%Y-%m-%d %H:%M')
    end = datetime.strptime(dateTo, '%Y-%m-%d %H:%M')
    if (end - start).days < 2:
      dateFrom, dateTo = hp.getPriorNhPeriod(dateTo)

    q = {
        "_source": [
            "from_date",
            "to_date",
            "src",
            "dest",
            "src_site",
            "dest_site",
            "diff"
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "from_date.keyword": {
                                "gte": dateFrom
                            }
                        }
                    },
                    {
                        "range": {
                            "to_date.keyword": {
                                "lte": dateTo
                            }
                        }
                    }
                ]
            }
        }
    }
    # print(str(q).replace("\'", "\""))
    result = scan(client=hp.es, index='ps_trace_changes', query=q)
    data = []

    for item in result:
      temp = item['_source']
      temp['tag'] = [temp['src_site'], temp['dest_site']]
      temp['from'] = temp['from_date']
      temp['to'] = temp['to_date']
      del temp['from_date']
      del temp['to_date']
      data.append(temp)

    return data



def queryAlarms(dateFrom, dateTo):
  period = hp.GetTimeRanges(dateFrom, dateTo)
  q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "created_at": {
                                "from": period[0],
                                "to": period[1]
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
                    }
                ]
            }
        }
        }

  try:
    result = scan(client=hp.es, index='aaas_alarms', query=q)
    data = {}

    for item in result:
        event = item['_source']['event']
        # if event != 'path changed':
        temp = []
        if event in data.keys():
            temp = data[event]

        if 'source' in item['_source'].keys():
          desc = item['_source']['source']
          if 'tags' in item['_source'].keys():
            tags = item['_source']['tags']
            desc['tag'] = tags

          if 'to' not in desc.keys():
            desc['to'] = datetime.fromtimestamp(
                item['_source']['created_at']/1000.0)

          if 'from' in desc.keys() and 'to' in desc.keys():
            desc['from'] = desc['from'].replace('T', ' ')
            desc['to'] = desc['to'].replace('T', ' ')
          temp.append(desc)

          data[event] = temp

    # path changed details resides in a separate index
    pathdf = queryPathChanged(dateFrom, dateTo)
    data['path changed between sites'] = pathdf
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
    data = scan(hp.es, index='ps_asns', query=query)
    if data:
      for item in data:
          asnDict[str(item['_id'])] = item['_source']['owner']
    else:
        print(ids, 'Not found')

    return asnDict


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

  print(len(data))
  print(data)
  for d in data: print(d)
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
    print(event)
    print(res['_source'])
    return res['_source']



def queryTraceChanges(fromDate, toDate):
  q = {
    "query": {
      "bool": {
        "must": [
          {
            "range": {
              "from_date.keyword": {
                "gte": fromDate
              }
            }
          },
          {
            "range": {
              "to_date.keyword": {
                "lte": toDate
              }
            }
          }
        ]
      }
    }
  }

  print(str(q).replace("\'", "\""))
  result = scan(client=hp.es,index='ps_trace_changes',query=q)
  data, positions, baseline, altPaths = [],[],[],[]
  positions = []
  for item in result:

      tempD = {}
      for k,v in item['_source'].items():
          if k not in ['positions', 'baseline', 'alt_paths', 'created_at']:
              tempD[k] = v
      data.append(tempD)

      src,dest,src_site,dest_site, = item['_source']['src'], item['_source']['dest'], item['_source']['src_site'], item['_source']['dest_site']
      from_date,to_date = item['_source']['from_date'], item['_source']['to_date']

      temp = item['_source']['positions']
      for p in temp:
          p['src'] = src
          p['dest'] = dest
          p['src_site'] = src_site
          p['dest_site'] = dest_site
          p['from_date'] = from_date
          p['to_date'] = to_date
      positions.extend(temp)
      
      temp = item['_source']['baseline']
      for p in temp:
          p['src'] = src
          p['dest'] = dest
          p['src_site'] = src_site
          p['dest_site'] = dest_site
          p['from_date'] = from_date
          p['to_date'] = to_date
      baseline.extend(temp)

      temp = item['_source']['alt_paths']
      for p in temp:
          p['src'] = src
          p['dest'] = dest
          p['src_site'] = src_site
          p['dest_site'] = dest_site
          p['from_date'] = from_date
          p['to_date'] = to_date
      altPaths.extend(temp)

  df = pd.DataFrame(data)
  posDf = pd.DataFrame(positions)
  baseline = pd.DataFrame(baseline)
  altPaths = pd.DataFrame(altPaths)
  
  posDf['pair'] = posDf['src']+' -> '+posDf['dest']
  df['pair'] = df['src']+' -> '+df['dest']
  baseline['pair'] = baseline['src']+' -> '+baseline['dest']
  altPaths['pair'] = altPaths['src']+' -> '+altPaths['dest']

  return df, posDf, baseline, altPaths


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