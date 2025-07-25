import glob
import os
import time
from elasticsearch.helpers import scan
import numpy as np
import pandas as pd
import traceback
from flask import request

import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet
import dash_bootstrap_components as dbc

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

          elif event in ['large clock correction']:
            df['site'] = df['tag'].apply(lambda x: x[1] if len(x) > 1 else x[0])
            df['tag'] = df['site']
            df = df.round(3)

          elif event in ['high packet loss',
                         'ASN path anomalies',
                         'ASN path anomalies per site',
                         'destination cannot be reached from any',
                         'source cannot reach any',
                         'bandwidth decreased',
                         'bandwidth increased',
                         'complete packet loss',
                         'hosts not found',
                         'unresolvable host']:
            df = self.list2rows(df)

          pivotFrames[event] = df

      return [frames, pivotFrames]

    except Exception as e:
      print('Issue with', event, df.columns)
      print(e, traceback.format_exc())


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
    print(odf.columns)

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
        if 'dest_loss' in r.keys():
          rec['dest_loss'] = r['dest_loss'][i]
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
        if 'src_loss' in r.keys():
          rec['src_loss'] = r['src_loss'][i]
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
    current_time = time.time()
    pq = Parquet()
    folder = glob.glob("parquet/frames/*")
    isTooOld = False
    frames, pivotFrames = {}, {}
    try:
      if folder:
        for f in folder:
            event = os.path.basename(f)
            # remove the extension (.parquet)
            event = os.path.splitext(event)[0]
            event = self.eventUF(event)

            df = pq.readFile(f)
            df['to'] = pd.to_datetime(df['to'], utc=True)
            modification_time = os.path.getmtime(f)

            # Calculate the time difference in seconds
            time_difference = current_time - modification_time
            time_difference_hours = time_difference / (60 * 60)

            # Check if the file was modified more than 1 hour ago
            if time_difference_hours <  1:
              # print('>>>>>>>', df['to'].min(), df['to'].max() , dateFrom, dateTo)
              # print("The file was modified within the last hour.")
              frames[event] = df[(df['to']>=dateFrom) & (df['to'] <= dateTo)]
              pdf = pq.readFile(f"parquet/pivot/{os.path.basename(f)}")
              pdf = pdf[(pdf['to'] >= dateFrom) & (pdf['to'] <= dateTo)]
              pivotFrames[event] = pdf

            else:
              print("\n\n The file was modified more than 1 hour ago.", f)
              isTooOld = True
      
      
      if len(folder)==0 or isTooOld == True:
          print('Query ES')
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
    
    # print(dateFrom, dateTo, currEvent, alarmEnd, '# alarms:', [len(d) for d in pivotFrames], site, src_site, dest_site)


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
      dd['temp'] = [sep.join(map(str, list(set(l)))) for l in df[colName]]
      dd = dd.drop(columns=[colName]).rename(columns={'temp': colName})
      return dd


  @staticmethod
  def convertListOfDict(column_name, df, event=False):
    def convert_to_string(value):
# I could have broken smth here
            if isinstance(value, dict):
                result = []
                for key, val in value.items():
                    if val is not None:
                        if isinstance(val, np.ndarray):
                            val_str = ', '.join(val)
                        else:
                            val_str = str(val)
                        result.append(f"<b>{key}</b>: {val_str}")
                return '\n'.join(result)
            if isinstance(value, list):
                val_str = ' || '.join(value)
                return val_str
            if column_name == "hosts_not_found":
                print(type(value))
            return str(value)
    if event:
      df['hosts_failed'] = df['hosts_failed'].apply(convert_to_string)
      df['tests_types_failed'] = df['tests_types_failed'].apply(convert_to_string)
    else:
      df[column_name] = df[column_name].apply(convert_to_string)
    return df


  @staticmethod
  def reorder_columns(df, columns):
      existing_columns = [col for col in columns if col in df.columns]
      remaining_columns = [col for col in df.columns if col not in existing_columns]
      return df[existing_columns + remaining_columns]


  # Format, hide or edit anything displayed in the datatables
  def formatDfValues(self, df, event, generate_button=False, site_report=False):
    try:
        sign = {'bandwidth increased from/to multiple sites': '+',
                'bandwidth decreased from/to multiple sites': ''}

        df = self.replaceCol('tag', df)
        if 'as_source_to' and 'as_destination_from' in df.columns:
            df['sites'] = df.apply(
                                      lambda row: set(
                                          (row['as_source_to'] if isinstance(row['as_source_to'], (list, tuple)) else []) +
                                          (row['as_destination_from'] if isinstance(row['as_destination_from'], (list, tuple)) else [])
                                      ),
                                      axis=1
                                  )
            df = self.replaceCol('as_source_to', df, '\n')
            df = self.replaceCol('as_destination_from', df, '\n')
        if 'sites' in df.columns:
            df = self.replaceCol('sites', df, '\n')
        if 'diff' in df.columns:
            df = self.replaceCol('diff', df)
            df.rename(columns={'diff': 'ASN-diff'}, inplace=True)
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

        if 'dest_loss' in df.columns:
            df['to_dest_loss'] = df[['dest_sites', 'dest_loss']].apply(lambda x: self.list2str(x, ''), axis=1)
            df.drop('dest_loss', axis=1, inplace=True)
            df.drop('dest_sites', axis=1, inplace=True)
        if 'src_loss' in df.columns:
            df['from_src_loss'] = df[['src_sites', 'src_loss']].apply(lambda x: self.list2str(x, ''), axis=1)
            df.drop('src_loss', axis=1, inplace=True)
            df.drop('src_sites', axis=1, inplace=True)
        format_sites_list = ['src_sites', 'dest_sites']
        for col in format_sites_list:
            if col in df.columns:
                  df = self.replaceCol(col, df, '\n')
        if 'anomalies' in df.columns:
            df['anomalies'] = df['anomalies'].apply(lambda x: ', '.join(map(str, x)))
            df.rename(columns={'anomalies': 'new ASN(s)'}, inplace=True)
        if 'to_date' in df.columns:
            df['to'] = pd.to_datetime(df['to_date'], format='mixed', utc=True)
            df['to'] = df['to'].dt.strftime('%Y-%m-%d')
            df.drop('to_date', axis=1, inplace=True)
        if 'asn_list' in df.columns:
            df.drop('asn_list', axis=1, inplace=True)
        if 'ipv' in df.columns:
            df['ipv'] = df['ipv'].apply(lambda x: x.lower() if x is not None else x)
            df.rename(columns={'ipv': 'IP version'}, inplace=True)

        # if 'alarms_id' in df.columns:
        #     df.drop('alarms_id', axis=1, inplace=True)
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

        if 'configurations' in df.columns:
            df = self.replaceCol('configurations', df, '\n')
        if 'hosts_not_found' in df.columns or event == 'hosts not found':
            additionalTable = False
            if 'hosts_not_found' not in df.columns:
              additionalTable = True
            df = self.convertListOfDict('hosts_not_found', df, additionalTable)
            
        drop_columns = {'complete packet loss': ['avg_value'], 'ASN path anomalies': ['asn_count'], 'ASN path anomalies per site': ['all_alarm_ids_src', 'all_alarm_ids_dest']}

        

        # TODO: create pages/visualizatios for the following events then remove the df.drop('alarm_link') below
        if event != "unresolvable host":
            df = self.createAlarmURL(df, event, site_report)
          
        if generate_button:
            self.createGraphButton(df)
        # df.drop('alarm_link', axis=1, inplace=True)
        if 'site' in df.columns:
            df['site'] = df['site'].fillna("Unknown site")

        # Reorder 'from' and 'to' columns to be the first two columns if they exist
        df = self.reorder_columns(df, ['from', 'to'])
        if event in drop_columns:
          df.drop(columns=drop_columns[event], inplace=True)
        if 'alarm_id' in df.columns:
            df.drop('alarm_id', axis=1, inplace=True)
        return df
    except Exception as e:

        print('Exception ------- ', event)
        print(df.info())
        print(df.head())
        print(e, traceback.format_exc())


  
  # Create dynamically the URLs leading to a page for a specific alarm

  

  @staticmethod
  def createAlarmURL(df, event, site_report=False):
      def generate_url(row, event, page, site_report):
          host_url = request.host_url
          if site_report:
              if 'ASN path anomalies' in event:
                  if event == 'ASN path anomalies per site':
                      idx = f"{row['site']}*{row['to']}*{row['alarm_id']}" if row['site'] and row['alarm_id'] else '-'
                      button = '-site'
                  else:
                      idx = f"{row['src_netsite']}*{row['dest_netsite']}*{row['to']}" if row['src_netsite'] and row['dest_netsite'] else '-'
                      button = ''
                  return dbc.Button(
                      "VIEW DETAILS",
                      id={'type': 'path-anomaly-btn'+button, 'index': idx},
                      className="btn btn-secondary",
                      n_clicks=0
                  ) if idx!='-' else '-'
            
              elif event == 'hosts not found':
                  return dbc.Button(
                      "VIEW DETAILS",
                      id={'type': 'hosts-not-found-btn', 'index': f"{row['site']}, {row['alarm_id']}"},
                      className="btn btn-secondary",
                      n_clicks=0
                  ) if row['site'] else '-'

              elif 'alarm_link' in row and row['alarm_link']:
                  return dbc.Button(
                      "VIEW DETAILS",
                      id={'type': 'alarm-link-btn', 'index': f"{row['alarm_link']}, {event}"},
                      className="btn btn-secondary",
                      n_clicks=0
                  )
              else:
                  return '-'

          else:
              if 'ASN path anomalies' in event:
                  if event == 'ASN path anomalies per site':
                      details = f'site={row['site']}&date={row['to']}&id={row['alarm_id']}' if row['site'] and row['alarm_id'] else '-'
                  else:
                      details = f'src_netsite={row['src_netsite']}&dest_netsite={row['dest_netsite']}&dt={row['to']}' if row['src_netsite'] and row['dest_netsite'] else '-'               
                  return (
                      f"<a class='btn btn-secondary' role='button' href='{host_url}{page}{details}' target='_blank'>VIEW IN A NEW TAB</a>"
                      if details!='-' else '-'
                  )

              elif event == 'hosts not found':
                  return (
                      f"<a class='btn btn-secondary' role='button' href='{host_url}{page}{row['site']}' target='_blank'>VIEW IN A NEW TAB</a>"
                      if row['site'] else '-'
                  )

              elif 'alarm_link' in row and row['alarm_link']:
                  return (
                      f"<a class='btn btn-secondary' role='button' href='{host_url}{page}{row['alarm_link']}' target='_blank'>VIEW IN A NEW TAB</a>"
                  )
              else:
                  return '-'
      
      page_map = {
          'ASN path anomalies': 'anomalous_paths/',
          'ASN path anomalies per site': 'anomalous_paths/',
          'firewall issue': 'loss-delay/',
          'complete packet loss': 'loss-delay/',
          'bandwidth decreased from/to multiple sites': 'loss-delay/',
          'high packet loss on multiple links': 'loss-delay/',
          'high packet loss': 'loss-delay/',
          'hosts not found': 'hosts_not_found/'
      }

      page = 'throughput/' if event.startswith('bandwidth') else page_map.get(event, '')
      df['alarm_link'] = df.apply(lambda row: generate_url(row, event, page, site_report), axis=1)
      return df
  
  @staticmethod
  def createGraphButton(df):
    df['alarm_button'] = df['site'].apply(
        lambda site: dbc.Button(
            "GENERATE GRAPH",
            id={'type': 'generate-graph-button', 'index': site},  # Unique ID for each button
            color="primary",
            className="me-1",
            style={"width": "100%", "font-size": "1.0em"}
        )
    )
    return df


  
  @staticmethod
  @timer
  # The code uses the description from ES and replaces the variables with the values
  def buildSummary(alarm):
    description = qrs.getCategory(alarm['event'])['template']
    description = description.split('More')[0]
    words = description.split()
    print(words)
    print(description)

    try:
      for k, v in alarm['source'].items():
          field = '%{'+k+'}' if not k == 'avg_value' else '%{'+k+'}.'
          print(k, v)
          print(field)
          if k == 'dest_loss':
            field = '{dest_loss}'
          elif k == 'src_loss':
            field = '{src_loss}'
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