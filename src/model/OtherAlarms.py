
from datetime import datetime
import glob
import os
from elasticsearch.helpers import scan
import pandas as pd
import traceback
from datetime import datetime, timedelta

import utils.helpers as hp
from utils.helpers import timer
from utils.parquet import Parquet
import traceback

import urllib3
urllib3.disable_warnings()


class OtherAlarms(object):

  def __init__(self, currEvent=None, alarmEnd=None, site=None, src_site=None, dest_site=None, refresh=False):
    self.pq = Parquet()
    print(currEvent, alarmEnd, site, src_site, dest_site,'=========================')
    if not refresh and currEvent is not None:
      self.data = self.getOtherAlarms(currEvent, alarmEnd, site, src_site, dest_site)
      self.formatted = self.formatOtherAlarms(self.data)



  @timer
  def queryAlarms(self, dateFrom, dateTo):
      period = hp.GetTimeRanges(dateFrom, dateTo)
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
                  "category" : {
                    "value" : "Networking",
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
      
      sites = {}
      for item in result:
          event = item['_source']['event']
          temp = []
          if event in data.keys():
              temp = data[event]
          # else: print(event)

          desc = item['_source']['source']
          tags = item['_source']['tags']
          desc['tag'] = tags
          
          if 'from' in desc.keys() and 'to' in desc.keys():
            desc['from'] = desc['from'].replace('T',' ')
            desc['to'] = desc['to'].replace('T',' ')
          temp.append(desc)

          data[event] = temp
      
      # path changed details resides in a separate index
      pathdf = self.queryPathChanged(dateFrom, dateTo)
      data['path changed unfolded'] = pathdf
      return data


  @timer
  def queryPathChanged(self, dateFrom, dateTo):
    start = datetime.strptime(dateFrom, '%Y-%m-%d %H:%M')
    end = datetime.strptime(dateTo, '%Y-%m-%d %H:%M')
    if (end - start).days < 2:
      print((end - start).days)
      dateFrom, dateTo = self.getPriorNhPeriod(dateTo)

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
    result = scan(client=hp.es,index='ps_trace_changes',query=q)
    data = []

    for item in result:
      temp = item['_source']
      temp['tag'] = [temp['src_site'], temp['dest_site']]
      temp['from'] = temp['from_date']
      temp['to'] = temp['to_date']
      data.append(temp)
    
    return data


  def list2rows(self, df):
      s = df.apply(lambda x: pd.Series(x['tag']),axis=1).stack().reset_index(level=1, drop=True)
      s.name = 'tag'
      df = df.drop('tag', axis=1).join(s)
      return df



  def unpackAlarms(self, alarmsData):
    frames, pivotFrames = {},{}

    try:
      for event, alarms in alarmsData.items():
        print()
        print(event)
        df =  pd.DataFrame(alarms)
        df['id'] = df.index
        for col in df.columns:
          if col in ['tag', 'src_sites', 'dest_sites']:
            df[col] = df[col].apply(lambda vals: [x.upper() for x in vals])
          elif col in ['site','src_site','dest_sites']:
            df[col] = df[col].str.upper()

        frames[event] = df


        if event == 'destination cannot be reached from multiple':
          pivotFrames[event] = self.one2manyUnfold(odf = df,
                                                  fld = 'site',
                                                  fldNewName = 'dest_site',
                                                  listStites = 'cannotBeReachedFrom',
                                                  listedNewName = 'src_site')

        elif event == 'firewall issue':
          pivotFrames[event] = self.one2manyUnfold(odf = df,
                                                  fld = 'site',
                                                  fldNewName = 'dest_site',
                                                  listStites = 'sites',
                                                  listedNewName = 'src_site')

        elif event in ['high packet loss on multiple links', 'bandwidth increased from/to multiple sites', 'bandwidth decreased from/to multiple sites']:
          pivotFrames[event] = self.oneInBothWaysUnfold(df)

        else:
          frames[event] = df
          pivotFrames[event] = self.list2rows(df)

      return [frames, pivotFrames]
    except Exception as ex:
        print(ex)


  # code friendly event name
  def eventCF(self, event):
      return event.replace(' ','_').replace('/','-')


  # user friendly event name
  def eventUF(self, event):
      return event.replace('_',' ').replace('-','/')



  def one2manyUnfold(self, odf, fld, fldNewName, listStites, listedNewName):
      s = odf.apply(lambda x: pd.Series(x[listStites]),axis=1).stack().reset_index(level=1, drop=True)
      s.name = listedNewName
      odf = odf.join(s)
      odf[fldNewName] = odf[fld]
      return odf



  def oneInBothWaysUnfold(self, odf):
      data = []
      for r in odf.to_dict('records'):
          for dest_site in r['dest_sites']:
              rec = {
                  'from': r['from'],
                  'to': r['to'],
                  # 'ipv6': r['ipv6'],
                  'dest_site': dest_site,
                  # 'change': r['dest_change'][i],
                  'src_site': r['site'], 
                  # 'alarm_id': r['alarm_id'],
                  'id': r['id'], 
                  'tag': r['tag']
              }
              data.append(rec)

          for src_site in r['src_sites']:
              rec = {
                  'from': r['from'],
                  'to': r['to'],
                  # 'ipv6': r['ipv6'],
                  'src_site': src_site,
                  # 'change': r['src_change'][i],
                  'dest_site': r['site'], 
                  # 'alarm_id': r['alarm_id'],
                  'id': r['id'], 
                  'tag': r['tag']
              }
              data.append(rec)

      return pd.DataFrame(data)



  def getAlarms(self, dateFrom, dateTo):
    data = self.queryAlarms(dateFrom, dateTo)
    frames, pivotFrames = self.unpackAlarms(data)
    return [frames, pivotFrames]



  def loadData(self, alarmEnd):
      fmt = '%Y-%m-%d %H:%M'
      twoMonthsAgo = datetime.strptime(hp.defaultTimeRange(60)[0], fmt)
      alarmEndT = datetime.strptime(alarmEnd, fmt)
      dateFrom, dateTo = self.getPriorNhPeriod(alarmEnd)

      pivotFrames, frames = {},{}
      if twoMonthsAgo>alarmEndT:
          # print('query')
          frames, pivotFrames = self.getAlarms(dateFrom, dateTo)
      else:
          # print('read')
          folder = glob.glob("parquet/frames/*")
          
          for f in folder:
              try:
                  event = os.path.basename(f)
                  event = self.eventUF(event)
                  df = self.pq.readFile(f)

                  if 'from' in df.columns:
                      frames[event] = df
                      df = self.pq.readFile(f"parquet/pivot/{os.path.basename(f)}")
                      pivotFrames[event] = df
              except Exception as e:
                  print(e, df)

      return frames, pivotFrames



  def getPriorNhPeriod(self, end, days=1):
    fmt = '%Y-%m-%d %H:%M'
    endT = datetime.strptime(end, fmt)
    start = datetime.strftime(endT - timedelta(days), fmt)
    end = datetime.strftime(endT + timedelta(days), fmt)
    return start, end



  def formatOtherAlarms(self, otherAlarms):
    if not otherAlarms:
        cntAlarms = 'None found'
    else: 
        cntAlarms = '  |  '
        for event, cnt in otherAlarms.items():
            cntAlarms += (event).capitalize()+': '+str(cnt)+'  |   '

    return cntAlarms



  def getOtherAlarms(self, currEvent, alarmEnd, site=None, src_site=None, dest_site=None):
    frames, pivotFrames = self.loadData(alarmEnd)

    dateFrom, dateTo = self.getPriorNhPeriod(alarmEnd)

    alarmsListed = {}

    pivotFrames.pop('path changed')
    pivotFrames['path changed'] = pivotFrames['path changed unfolded']
    pivotFrames.pop('path changed unfolded')

    for event, pdf in pivotFrames.items():
      if not event==currEvent:
        try:
          subdf = pdf[(pdf['to']>=dateFrom) & (pdf['to']<=dateTo)]

          if src_site is not None and dest_site is not None and 'src_site' in pdf.columns and 'dest_site' in pdf.columns:
            src_site, dest_site = src_site.upper(), dest_site.upper()
            if len(subdf[(subdf['src_site']==src_site) & (subdf['dest_site']==dest_site)])>0:
                subdf = subdf[(subdf['src_site']==src_site) & (subdf['dest_site']==dest_site)]
                alarmsListed[event] = len(subdf)
      
          elif site is not None:
            site = site.upper()
            if len(subdf[subdf['tag']==site])>0:
              subdf = pdf[((pdf['tag']==site))]
              if len(subdf)>0:
                  alarmsListed[event] = len(subdf)

        except Exception as e:
            print(e, traceback.format_exc())

    return alarmsListed