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
    return f"Throughput alarm {q}"



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

      for k, v in alrmContent.items():
        print(k, v)

      query = ''
      
      if 'src' in alrmContent.keys() and 'dest' in alrmContent.keys():
        query = f'src:{alrmContent["src"]} and dest:{alrmContent["dest"]}'
      if 'src_host' in alrmContent.keys() and 'dest_host' in alrmContent.keys():
        query = f'src_host:{alrmContent["src_host"]} and dest_host:{alrmContent["dest_host"]}'
      elif 'host' in alrmContent.keys() and event == 'high packet loss on multiple links':
        query = f'src_host: {alrmContent["host"]} or dest_host: {alrmContent["host"]}'
      elif 'host' in alrmContent.keys():
        query = f'dest_host: {alrmContent["host"]}'
      # elif 'host' in alrmContent.keys() and event == 'firewall issue':
      #   query = f'dest_host: {alrmContent["host"]}'
      

      dates = hp.getPriorNhPeriod(alrmContent['to'], 3)
      dates = [dt.replace(' ', 'T')+':00.000Z' for dt in dates]
      timeRange = f"(from:'{dates[0]}',to:'{dates[1]}')"


      alarmsInst = Alarms()
      url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/e015c210-65e2-11ed-afcf-d91dad577662?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&_a=(query:(language:kuery,query:\'{query}\'))'

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
            dbc.Row([
                dbc.Row([
                  html.Iframe(src=url,
                  style={"height": "1000px"})
                ], className="boxwithshadow pair-details g-0"),
            ], style={"padding": "0.5% 1.5%"}, justify="between", align="center")
            ])



