import dash
from dash import html
import dash_bootstrap_components as dbc

from datetime import datetime

import utils.helpers as hp
from model.Alarms import Alarms
import model.queries as qrs

import urllib3
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


def convertTime(ts):
    stripped = datetime.strptime(ts, '%Y-%m-%d %H:%M')
    return int((stripped - datetime(1970, 1, 1)).total_seconds()*1000)


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


      dates = hp.getPriorNhPeriod(alrmContent['to'], before=3, midPoint=False)
      dates = [dt.replace(' ', 'T')+':00.000Z' for dt in dates]
      timeRange = f"(from:'{dates[0]}',to:'{dates[1]}')"


      alarmsInst = Alarms()
      url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/e015c210-65e2-11ed-afcf-d91dad577662?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&_a=(query:(language:kuery,query:\'{query}\'))'
      
      kibanaIframe = []
      if alarm['event'] == 'high packet loss on multiple links':
        if len(alrmContent["dest_sites"])>0:
          dest_sites = str(list(s for s in set(alrmContent["dest_sites"]))).replace('\'', '"').replace('[','').replace(']','').replace(',', ' OR')
          query = f'src_host: {alrmContent["host"]} and dest_site:({dest_sites})'
          url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/ee5a6310-8c40-11ed-8156-b9b28813464d?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&hide-filter-bar=true&_a=(query:(language:kuery,query:\'{query}\'))'
          # print(url)
          kibanaIframe.append(dbc.Row([
              dbc.Row(html.H3(f"Issues from {alrmContent['site']}")),
              dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
            ], className="boxwithshadow pair-details g-0 mb-1"))
        
        if len(alrmContent["src_sites"]) > 0:
          src_sites = str(list(s for s in set(alrmContent["src_sites"]))).replace('\'', '"').replace('[', '').replace(']', '').replace(',', ' OR')
          query = f'dest_host: {alrmContent["host"]} and src_site:({src_sites})'
          url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/920cd1f0-8c41-11ed-8156-b9b28813464d?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&_a=(query:(language:kuery,query:\'{query}\'))'
          kibanaIframe.append(dbc.Row([
              dbc.Row(html.H3(f"Issues to {alrmContent['site']}")),
              dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
            ], className="boxwithshadow pair-details g-0 mb-1"))
      else:
        kibanaIframe = dbc.Row([html.Iframe(src=url, style={"height": "1000px"})], className="boxwithshadow pair-details g-0")



      return html.Div([
              dbc.Row([
                dbc.Row([
                  dbc.Col([
                    html.H3(event.upper(), className="text-center bold p-4"),
                    html.H5(alrmContent['to'], className="text-center bold"),
                  ], width=2),
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
                  width=10)
                ], className="boxwithshadow alarm-header pair-details g-0", justify="between", align="center")
              ], style={"padding": "0.5% 1.5%"}, className='g-0'),
            dbc.Row(
                kibanaIframe, style={"padding": "0.5% 1.5%"}, className='g-0')
            ])



