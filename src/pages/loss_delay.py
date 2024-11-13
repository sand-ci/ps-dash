import dash
from dash import html
import dash_bootstrap_components as dbc

from datetime import datetime
import urllib3

import utils.helpers as hp
from utils.helpers import DATE_FORMAT
from model.Alarms import Alarms
import model.queries as qrs

from utils.parquet import Parquet
urllib3.disable_warnings()



def title(q=None):
    return f"Latency alarm {q}"



def description(q=None):
    return f"Visual represention on a selected packetloss alarms {q}"



dash.register_page(
    __name__,
    path_template="/loss-delay/<q>",
    title=title,
    description=description,
)


def obtainFieldNames(dateFrom):
  # Date when all latency alarms started netsites instead of sites
  target_date = datetime(2023, 9, 21)
  
  date_from = datetime.strptime(dateFrom, DATE_FORMAT)
  
  if date_from > target_date:
    return {'src': 'src_netsite', 'dest': 'dest_netsite'}
  else:
    return {'src': 'src_site', 'dest': 'dest_site'}


def layout(q=None, **other_unknown_query_strings):
    if q:
      alarm = qrs.getAlarm(q)
      print('URL query:', q)
      print()
      print('Alarm content:', alarm)

      alrmContent = alarm['source']
      event = alarm['event']

      query = ''
      if 'src' in alrmContent.keys() and 'dest' in alrmContent.keys():
        query = f'src:{alrmContent["src"]} and dest:{alrmContent["dest"]}'
      if 'src_host' in alrmContent.keys() and 'dest_host' in alrmContent.keys():
        query = f'src_host:{alrmContent["src_host"]} and dest_host:{alrmContent["dest_host"]}'
      elif 'host' in alrmContent.keys() and event == 'high packet loss on multiple links':
        query = f'src_host: {alrmContent["host"]} or dest_host: {alrmContent["host"]}'
      elif 'host' in alrmContent.keys():
        query = f'dest_host: {alrmContent["host"]}'


      dates = hp.getPriorNhPeriod(alrmContent['to'], daysBefore=3, midPoint=False)
      timeRange = f"(from:'{dates[0]}',to:'{dates[1]}')"
      fieldName = obtainFieldNames(dates[0]) 

      pq = Parquet()
      metaDf = pq.readFile('parquet/raw/metaDf.parquet')

      alarmsInst = Alarms()
      url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/e015c210-65e2-11ed-afcf-d91dad577662?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&_a=(query:(language:kuery,query:\'{query}\'))'
      
      kibanaIframe = []
      if alarm['event'] == 'high packet loss on multiple links':
        if len(alrmContent["dest_sites"])>0:
          original_names = metaDf[metaDf['netsite'].isin(alrmContent["dest_sites"])]['netsite_original'].unique()
          dest_sites = str(list(s for s in set(original_names))).replace('\'', '"').replace('[','').replace(']','').replace(',', ' OR')
          query = f'src_host: {alrmContent["host"]} and {fieldName["dest"]}:({dest_sites})'
          url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/ee5a6310-8c40-11ed-8156-b9b28813464d?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&hide-filter-bar=true&_a=(query:(language:kuery,query:\'{query}\'))'
          # print('\n \n',url)
          kibanaIframe.append(dbc.Row([
              dbc.Row(html.H3(f"Issues from {alrmContent['site']}")),
              dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
            ], className="boxwithshadow pair-details g-0 mb-1 p-3"))
        
        if len(alrmContent["src_sites"]) > 0:
          original_names = metaDf[metaDf['netsite'].isin(alrmContent["src_sites"])]['netsite_original'].unique()
          src_sites = str(list(s for s in set(original_names))).replace('\'', '"').replace('[','').replace(']','').replace(',', ' OR')
          query = f'dest_host: {alrmContent["host"]} and {fieldName["src"]}:({src_sites})'
          url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/920cd1f0-8c41-11ed-8156-b9b28813464d?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&_a=(query:(language:kuery,query:\'{query}\'))'
          # print('\n \n',url)
          kibanaIframe.append(dbc.Row([
              dbc.Row(html.H3(f"Issues to {alrmContent['site']}")),
              dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
            ], className="boxwithshadow pair-details g-0 mb-1 p-3"))
      else:
        kibanaIframe = dbc.Row([html.Iframe(src=url, style={"height": "1000px"})], className="boxwithshadow pair-details g-0")



      return html.Div([
              dbc.Row([
                dbc.Row([
                  dbc.Col([
                    html.H3(event.upper(), className="text-center bold"),
                    html.H5(alrmContent['to'], className="text-center bold"),
                  ], lg=2, md=12, className="p-3"),
                  dbc.Col(
                      html.Div(
                        [
                          dbc.Row([
                            dbc.Row([
                              html.H1(f"Summary", className="text-left"),
                              html.Hr(className="my-2")]),
                            dbc.Row([
                                html.P(alarmsInst.buildSummary(alarm), className='subtitle'),
                              ], justify="start"),
                            ])
                        ],
                      ),
                  lg=10, md=12, className="p-3")
                ], className="boxwithshadow alarm-header pair-details g-0", justify="between", align="center")
              ], style={"padding": "0.5% 1.5%"}, className='g-0'),
            dbc.Row(
                kibanaIframe, style={"padding": "0.5% 1.5%"}, className='g-0 p-3')
      ], className='mb-5')



