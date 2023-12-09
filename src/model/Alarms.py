import glob
import os
from elasticsearch.helpers import scan
import pandas as pd
import traceback
from flask import request

import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet

import urllib3
urllib3.disable_warnings()


class Alarms(object):

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
          frames[event] = df

          if event == 'destination cannot be reached from multiple':
            df = self.one2manyUnfold(odf=df,
                                    fld='site',
                                    fldNewName='dest_site',
                                    listSites='cannotBeReachedFrom',
                                    listedNewName='src_site')
            df['tag'] = df['site']
          elif event == 'firewall issue':
            df = self.one2manyUnfold(odf=df,
                                     fld='site',
                                     fldNewName='dest_site',
                                     listSites='sites',
                                     listedNewName='src_site')
            df['tag'] = df['site']

          elif event in ['high packet loss on multiple links', 'bandwidth increased from/to multiple sites', 'bandwidth decreased from/to multiple sites']:
            df = self.oneInBothWaysUnfold(df)

          elif event == 'large clock correction':
            # df = df.drop('tags', axis=1)
            df['site'] = df['tag'].apply(lambda x: x[1] if len(x) > 1 else x[0])
            df['tag'] = df['site']
            df = df.round(2)
            # df['tags'] = df['tag']

          elif event in ['high packet loss',
                         'path changed',
                         'destination cannot be reached from any',
                         'source cannot reach any',
                         'bandwidth decreased',
                         'bandwidth increased',
                         'complete packet loss',
                         'path changed between sites']:
            
            
            df = self.list2rows(df)

    
          pivotFrames[event] = df

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
    # the field name changed on the DB side
    if 'dest_loss%' in odf.columns and 'src_loss%' in odf.columns:
      odf['dest_loss%'] = odf['dest_loss%'].fillna(odf['dest_loss'])
      odf['src_loss%'] = odf['src_loss%'].fillna(odf['src_loss'])
      odf.drop(columns=['dest_loss', 'src_loss'], inplace=True)

    for r in odf.to_dict('records'):
      for i, dest_site in enumerate(r['dest_sites']):
        rec = {
          'from': r['from'],
          'to': r['to'],
          'dest_site': dest_site,
          'src_site': r['site'],
          'id': r['id'],
          'tag': r['tag'][0]
        }
        if 'dest_loss%' in r.keys():
          rec['dest_loss%'] = r['dest_loss%'][i]
        elif 'dest_change' in r.keys():
          rec['dest_change'] = r['dest_change'][i]

        if 'ipv6' in r.keys():
          rec['ipv6'] = r['ipv6']
          
        data.append(rec)

      for i, src_site in enumerate(r['src_sites']):
        rec = {
          'from': r['from'],
          'to': r['to'],
          'src_site': src_site,
          'dest_site': r['site'],
          'id': r['id'],
          'tag': r['tag'][0]
        }
        if 'src_loss%' in r.keys():
          rec['src_loss%'] = r['src_loss%'][i]
        elif 'src_change' in r.keys():
          rec['src_change'] = r['src_change'][i]

        if 'ipv6' in r.keys():
          rec['ipv6'] = r['ipv6']

        data.append(rec)

    df = pd.DataFrame(data)

    return df


  def getAllAlarms(self, dateFrom, dateTo):
    data = qrs.queryAlarms(dateFrom, dateTo)
    if 'indexing' in data.keys(): del data['indexing']
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

            if 'from' in df.columns.tolist():

              if dateFrom >= df[~df['from'].isnull()]['from'].min():
                  frames[event] = df[(df['to']>=dateFrom) & (df['to'] <= dateTo)]
                  pdf = pq.readFile(f"parquet/pivot/{os.path.basename(f)}")

                  pdf = pdf[(pdf['to'] >= dateFrom) & (pdf['to'] <= dateTo)]
                  pivotFrames[event] = pdf

              else:
                  isTooOld = True

      if not folder or isTooOld:
          # print('query', event)
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
    # print(dateFrom, dateTo, currEvent, alarmEnd, '# alarms:', [len(d) for d in pivotFrames], site, src_site, dest_site)
    print()

    alarmsListed = {}

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


  # Format, hide or edit anything displayed in the datatables
  def formatDfValues(self, df, event):
    try:
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
            
            df['dest_change'] = df[['dest_sites', 'dest_change']].apply(lambda x: self.list2str(x, sign[event]), axis=1)
            # df.drop('dest_change', axis=1, inplace=True)
            df.drop('dest_sites', axis=1, inplace=True)
        if 'src_change' in df.columns:
            df['src_change'] = df[['src_sites', 'src_change']].apply(lambda x: self.list2str(x, sign[event]), axis=1)
            # df.drop('src_change', axis=1, inplace=True)
            df.drop('src_sites', axis=1, inplace=True)

        if 'dest_loss%' in df.columns:
            df['to_dest_loss'] = df[['dest_sites', 'dest_loss%']].apply(lambda x: self.list2str(x, ''), axis=1)
            df.drop('dest_loss%', axis=1, inplace=True)
            df.drop('dest_sites', axis=1, inplace=True)
        if 'src_loss%' in df.columns:
            df['from_src_loss'] = df[['src_sites', 'src_loss%']].apply(lambda x: self.list2str(x, ''), axis=1)
            df.drop('src_loss%', axis=1, inplace=True)
            df.drop('src_sites', axis=1, inplace=True)

        if 'src_sites' in df.columns:
            df = self.replaceCol('src_sites', df, '\n')
        if 'dest_sites' in df.columns:
            df = self.replaceCol('dest_sites', df, '\n')


        if 'alarms_id' in df.columns:
            df.drop('alarms_id', axis=1, inplace=True)
        if 'tag' in df.columns:
            df.drop('tag', axis=1, inplace=True)
        if '%change' in df.columns:
           df.drop('%change', axis=1, inplace=True)
        if 'id' in df.columns:
            df.drop('id', axis=1, inplace=True)
        if 'avg_value' in df.columns:
            df['avg_value'] = df['avg_value'].apply(lambda x: f'{x}%')
        if 'alarm_id' in df.columns:
          df['alarm_link'] = df['alarm_id']
          df.drop('alarm_id', axis=1, inplace=True)
        df = df[['from','to'] + [col for col in df.columns if not col in ['from', 'to']]]
        
        df = self.createAlarmURL(df, event)
    except Exception as e:
        print('Exception ------- ', event)
        print(df.head())
        print(e, traceback.format_exc())

    return df

  @staticmethod
  # Create dynamically the URLs leading to a page for a specific alarm
  def createAlarmURL(df, event):
    if event.startswith('bandwidth'):
          page = 'throughput/'
    elif event == 'path changed':
        page = 'paths/'
    elif event in ['firewall issue', 'complete packet loss', 'bandwidth decreased from/to multiple sites',
                    'high packet loss on multiple links', 'high packet loss']:
        page = 'loss-delay/'

    # create clickable cells leading to alarm pages
    if 'alarm_link' in df.columns:
        url = f'{request.host_url}{page}'
        df['alarm_link'] = df['alarm_link'].apply(
            lambda id: f"<a class='btn btn-secondary' role='button' href='{url}{id}' target='_blank'>VIEW IN A NEW TAB</a>" if id else '-')
    
    # if event == 'path changed between sites':
    #     df['alarm_link'] = df['path'].apply(
    #         lambda site: f"<a href='{request.host_url}paths-site/{site}?dateFrom={df['from'].min()}&dateTo={df['to'].max()}' target='_blank'>VIEW</a>" if site else '-')
            
    return df


  
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
          if k == 'change':
            field = '%{change}%'

          if field in words or field+',' in words or field+'.' in words or field+';' in words:
            if isinstance(v, list):
              if len(v) == 0:
                v = ' - '
              else:
                v = '  |  '.join(str(l) for l in v)
              v = "\n" + v

            if k == 'avg_value':
              v = str(v)+'%'
            elif k == '%change':
              v = str(v)+'%'
            elif k == 'change':
              v = str(v)+'%'
            
            if v is None:
              v = ' - '

            description = description.replace(field, str(v))

    except Exception as e:
      print(e)

    return description
