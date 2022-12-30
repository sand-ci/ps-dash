import glob
import os
from elasticsearch.helpers import scan
import pandas as pd
import traceback
from datetime import datetime, timedelta

import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet

import urllib3
urllib3.disable_warnings()


class Alarms(object):

  @timer
  def queryAlarms(self, dateFrom, dateTo):
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
        pathdf = self.queryPathChanged(dateFrom, dateTo)
        data['path changed between sites'] = pathdf
        return data
      except Exception as e:
        print('Exception:', e)
        print(traceback.format_exc())


  @timer
  def queryPathChanged(self, dateFrom, dateTo):
    start = datetime.strptime(dateFrom, '%Y-%m-%d %H:%M')
    end = datetime.strptime(dateTo, '%Y-%m-%d %H:%M')
    if (end - start).days < 2:
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


  @staticmethod
  def list2rows(df):
      s = df.apply(lambda x: pd.Series(x['tag']), axis=1).stack().reset_index(level=1, drop=True)
      s.name = 'tag'
      df = df.drop('tag', axis=1).join(s)
      return df


  def unpackAlarms(self, alarmsData):
    frames, pivotFrames = {}, {}
    
    try:
      for event, alarms in alarmsData.items():
        if len(alarms)>0:
          df = pd.DataFrame(alarms)

          df['id'] = df.index

          for col in df.columns:
            df = df[~df[col].isnull()]
            if col in ['tag', 'src_sites', 'dest_sites', 'sites']:
              df[col] = df[col].apply(lambda vals: [x.upper() for x in vals])
            elif col in ['site', 'src_site', 'dest_site']:
              df[col] = df[col].str.upper()

          frames[event] = df

          if event == 'destination cannot be reached from multiple':
            pivotFrames[event] = self.one2manyUnfold(odf=df,
                                                      fld='site',
                                                      fldNewName='dest_site',
                                                      listSites='cannotBeReachedFrom',
                                                      listedNewName='src_site')

          elif event == 'firewall issue':
            df = self.one2manyUnfold(odf=df,
                                     fld='site',
                                     fldNewName='dest_site',
                                     listSites='sites',
                                     listedNewName='src_site')
            

          elif event in ['high packet loss on multiple links', 'bandwidth increased from/to multiple sites', 'bandwidth decreased from/to multiple sites']:
            pivotFrames[event] = self.oneInBothWaysUnfold(df)
          elif event == 'large clock correction':
            df['tags'] = df['tag']
            df['site'] = df['tag'].apply(lambda x: x[1] if len(x) > 1 else '')
            df['tag'] = df['site']
            # df['tags'] = df['tag']
            pivotFrames[event] = df
          # else:
          #   frames[event] = df
          
          pivotFrames[event] = self.list2rows(df)

    except Exception as e:
      print('Issue with', event, df.columns)
      print(e, traceback.format_exc())

    return [frames, pivotFrames]



  # code friendly event name
  @staticmethod
  def eventCF(event):
    return event.replace(' ', '_').replace('/', '-')


  # user friendly event name
  @staticmethod
  def eventUF(event):
    return event.replace('_', ' ').replace('-', '/')


  @staticmethod
  def one2manyUnfold(odf, fld, fldNewName, listSites, listedNewName):
      s = odf.apply(lambda x: pd.Series(x[listSites]), axis=1).stack(
      ).reset_index(level=1, drop=True)
      s.name = listedNewName
      odf = odf.join(s)
      odf[fldNewName] = odf[fld]
      return odf


  @staticmethod
  def oneInBothWaysUnfold(odf):
      data = []
      for r in odf.to_dict('records'):
          for dest_site in r['dest_sites']:
              rec = {
                  'from': r['from'],
                  'to': r['to'],
                  # 'ipv6': r['ipv6'],
                  'dest_site': dest_site,
                  'src_site': r['site'],
                  'id': r['id'],
                  'tag': r['tag'][0]
              }
              data.append(rec)

          for src_site in r['src_sites']:
              rec = {
                  'from': r['from'],
                  'to': r['to'],
                  # 'ipv6': r['ipv6'],
                  'src_site': src_site,
                  'dest_site': r['site'],
                  'id': r['id'],
                  'tag': r['tag'][0]
              }
              data.append(rec)

      return pd.DataFrame(data)


  def getAllAlarms(self, dateFrom, dateTo):
    data = self.queryAlarms(dateFrom, dateTo)
    frames, pivotFrames = self.unpackAlarms(data)
    return [frames, pivotFrames]


  # Check the requested period and either read the data
  # from the local files or read from ES
  def loadData(self, dateFrom, dateTo):
    print(f"loadData for {dateFrom}, {dateTo}")
    print('+++++++++++++++++++++')
    print()

    pq = Parquet()
    folder = glob.glob("parquet/frames/*")
    isTooOld = False
    frames, pivotFrames = {}, {}
    try:
      if folder:
        print("read stored data")
        print('+++++++++++++++++++++')
        for f in folder:
            event = os.path.basename(f)
            event = self.eventUF(event)
            df = pq.readFile(f)

            if 'from' in df.columns.tolist() and event != 'large clock correction':

              if dateFrom >= df[~df['from'].isnull()]['from'].min():
                  frames[event] = df
                  pdf = pq.readFile(f"parquet/pivot/{os.path.basename(f)}")
                  pivotFrames[event] = pdf

                  frames[event] = df[(df['to']>=dateFrom) & (df['to'] <= dateTo)]
                  pdf = pq.readFile(f"parquet/pivot/{os.path.basename(f)}")
                  pivotFrames[event] = pdf[(pdf['to'] >= dateFrom) & (pdf['to'] <= dateTo)]
              else:
                  isTooOld = True

      if not folder or isTooOld:
          print('query', event)
          print('+++++++++++++++++++++')
          frames, pivotFrames = self.getAllAlarms(dateFrom, dateTo)

    except Exception as e:
      print(e, traceback.format_exc())
    return frames, pivotFrames


  @staticmethod
  def formatOtherAlarms(otherAlarms):
    if not otherAlarms:
        cntAlarms = 'None found'
    else:
        cntAlarms = '  |  '
        for event, cnt in otherAlarms.items():
            cntAlarms += (event).capitalize()+': '+str(cnt)+'  |   '

    return cntAlarms


  @timer
  def getOtherAlarms(self, currEvent, alarmEnd, pivotFrames, site=None, src_site=None, dest_site=None):
    # for a given alarm, check if there were additional alarms
    # 24h prior and 24h after the current event
    dateFrom, dateTo = hp.getPriorNhPeriod(alarmEnd)
    # frames, pivotFrames = self.loadData(dateFrom, dateTo)
    print('getOtherAlarms')
    print('+++++++++++++++++++++')
    print(dateFrom, dateTo, currEvent, alarmEnd, '# alarms:', [len(d) for d in pivotFrames], site, src_site, dest_site)
    print()

    alarmsListed = {}

    if 'path changed' in pivotFrames.keys() and 'path changed between sites' in pivotFrames.keys():
      pivotFrames.pop('path changed')
      pivotFrames['path changed'] = pivotFrames['path changed between sites']
      pivotFrames.pop('path changed between sites')

    for event, pdf in pivotFrames.items():
      if not event == currEvent:
        try:
          subdf = pdf[(pdf['to'] >= dateFrom) & (pdf['to'] <= dateTo)]

          if src_site is not None and dest_site is not None and 'src_site' in pdf.columns and 'dest_site' in pdf.columns:
            src_site, dest_site = src_site.upper(), dest_site.upper()
            if len(subdf[(subdf['src_site'] == src_site) & (subdf['dest_site'] == dest_site)]) > 0:
                subdf = subdf[(subdf['src_site'] == src_site) & (subdf['dest_site'] == dest_site)]
                alarmsListed[event] = len(subdf['id'].unique())

          elif site is not None:
            site = site.upper()
            if len(subdf[subdf['tag'] == site]) > 0:
              subdf = subdf[((subdf['tag'] == site))]

              if len(subdf) > 0:
                  alarmsListed[event] = len(subdf['id'].unique())

        except Exception as e:
            print(f'Issue with {event}')
            print(e, traceback.format_exc())

    return alarmsListed


  @staticmethod
  def list2str(vals, sign):
    values = vals.values
    temp = ''
    for i, s in enumerate(values[0]):
        temp += f'{s}: {sign}{values[1][i]}% \n'

    return temp


  @staticmethod
  def replaceCol(colName, df, sep=','):
      dd = df.copy()
      dd['temp'] = [sep.join(map(str, l)) for l in df[colName]]
      dd = dd.drop(columns=[colName]).rename(columns={'temp': colName})
      return dd


  def formatDfValues(self, df, event):
    try:
        columns = df.columns.tolist()

        if event in ['bandwidth decreased from/to multiple sites', 'bandwidth increased from/to multiple sites']:
            columns = [el for el in columns if el not in ['src_sites', 'dest_sites']]

        sign = {'bandwidth increased from/to multiple sites': '+',
                'bandwidth decreased from/to multiple sites': ''}

        df = self.replaceCol('tag', df)
        if 'sites' in df.columns:
          df = self.replaceCol('sites', df, '\n')
        if 'diff' in df.columns:
            df = self.replaceCol('diff', df)
        if 'hosts' in df.columns:
            df = self.replaceCol('hosts', df, '\n')
        if 'cannotBeReachedFrom' in df.columns:
          df = self.replaceCol('cannotBeReachedFrom', df, '\n')

        if 'dest_change' in df.columns:
            df['dest_change'] = df[['dest_sites', 'dest_change']].apply(
                lambda x: self.list2str(x, sign[event]), axis=1)
        if 'src_change' in df.columns:
            df['src_change'] = df[['src_sites', 'src_change']].apply(
                lambda x: self.list2str(x, sign[event]), axis=1)

        if 'dest_loss%' in df.columns:
            df['to_dest_loss'] = df[['dest_sites', 'dest_loss%']].apply(
                lambda x: self.list2str(x, ''), axis=1)
        if 'src_loss%' in df.columns:
            df['from_src_loss'] = df[['src_sites', 'src_loss%']].apply(
                lambda x: self.list2str(x, ''), axis=1)

        if 'src_sites' in df.columns:
            df = self.replaceCol('src_sites', df, '\n')
        if 'dest_sites' in df.columns:
            df = self.replaceCol('dest_sites', df, '\n')

    except Exception as e:
        print('Exception ------- ', event)
        print(e, traceback.format_exc())

    return df[columns]

  
  @staticmethod
  @timer
  # The code uses the description from ES and replaces the variables with the values
  def buildSummary(alarm):
    description = qrs.getCategory(alarm['event'])['template']
    description = description.split('More')[0]
    words = description.split()

    try:
      for k, v in alarm['source'].items():
          field = '%{'+k+'}' if not k == 'avg_value' else 'p{'+k+'}'
          if k == 'dest_loss%':
            field = '%{dest_loss}'
          elif k == 'src_loss%':
            field = '%{src_loss}'
          if k == '%change':
            field = '%{%change}%'

          if field in words or field+',' in words or field+'.' in words or field+';' in words:
            if isinstance(v, list):
              if len(v) == 0:
                v = ' - '
              else:
                v = '  |  '.join(str(l) for l in v)
              v = "\n" + v

            if k == 'avg_value':
              v = str(v*100)+'%'
            elif k == '%change':
              v = str(v)+'%'
            
            if v is None:
              v = ' - '

            description = description.replace(field, str(v))

    except Exception as e:
      print(e)

    return description
