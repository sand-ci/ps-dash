import dash
from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, MATCH, State

from elasticsearch.helpers import scan

import plotly.graph_objects as go
import plotly.express as px


import pandas as pd
from datetime import datetime

from utils.helpers import timer
from elasticsearch.helpers import scan

import utils.helpers as hp
from model.OtherAlarms import OtherAlarms

import urllib3
urllib3.disable_warnings()



def title(q=None):
    return f"Throughput alarm {q}"



def description(q=None):
    return f"Visual represention on a selected througput alarm {q}"



dash.register_page(
    __name__,
    path_template="/throughput/<q>",
    title=title,
    description=description,
)



def convertTime(ts):
    stripped = datetime.strptime(ts, '%Y-%m-%d %H:%M')
    return int((stripped - datetime(1970, 1, 1)).total_seconds()*1000)



@timer
def getRawDataFromES(src, dest, ipv6, dateFrom, dateTo):
    q = {
        "query" : {  
            "bool" : {
            "must" : [
                  {
                    "range": {
                        "timestamp": {
                        "gte": convertTime(dateFrom),
                        "lte": convertTime(dateTo)
                        }
                      }
                  },
                  {
                    "term" : {
                        "src_site": {
                        "value": src,
                        "case_insensitive": True
                        }
                    }
                  },
                  {
                    "term" : {
                        "dest_site": {
                        "value":dest,
                        "case_insensitive": True
                        }
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
    # print(idx, str(query).replace("\'", "\""))

    result = scan(client=hp.es,index='ps_throughput',query=q)
    data = []

    for item in result:
        data.append(item['_source'])

    df = pd.DataFrame(data)
    df['pair'] = df['src']+'->'+df['dest']
    df['dt'] = pd.to_datetime(df['timestamp'], unit='ms')

    return df


@timer
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
  
  print(id, data)
  if len(data) == 1:
    return data[0]


@timer
def buildPlot(df):
  fig = go.Figure(data=px.scatter(
      df,
      y = 'MBps',
      x = 'dt',
      color='pair'
      
  ))

  fig.update_layout(showlegend=False)
  fig.update_traces(marker=dict(
                      color='#00245a',
                      size=12
                  ))
  fig.layout.template = 'plotly_white'
  # fig.show()
  return fig


@timer
def buildSummary(alarm):
  desc = 'Bandwidth decreased' if alarm['event'].startswith('bandwidth decreased') else 'Bandwidth increased'

  if alarm['event'] in ['bandwidth decreased', 'bandwidth increased']:
    return f"{desc} for the {alarm['source']['ipv']} links between sites {alarm['source']['src_site']} and {alarm['source']['dest_site']}.\
          Current throughput is {alarm['source']['last3days_avg']} MB, dropped by {alarm['source']['%change']}% with respect to the 21-day-average. "

  elif alarm['event'] in ['bandwidth decreased from/to multiple sites', 'bandwidth increased from/to multiple sites']:
    temp = f"{desc} for the {alarm['source']['ipv']} links between site {alarm['source']['site']}"
    firstIn = False
    if alarm['source']['dest_sites']:
      firstIn = True
      temp+=f" to sites: {'  |  '.join(alarm['source']['dest_sites'])} change in percentages: {('  |  '.join([str(l) for l in alarm['source']['dest_change']]))}"
    if alarm['source']['src_sites']:
      if firstIn:
        temp+= ' and '
      temp += f"from sites: {'  |  '.join(alarm['source']['src_sites'])}, change in percentages: {('  |  '.join([str(l) for l in alarm['source']['src_change']]))}"

    temp += " with respect to the 21-day average."
    return temp


@timer
def getSitePairs(alarm):
  sitePairs = []
  event = alarm['event']

  if event in ['bandwidth decreased', 'bandwidth increased']:
    sitePairs = [{'src_site': alarm['source']['src_site'],
                'dest_site': alarm['source']['dest_site'],
                'ipv6': alarm['source']['ipv6'],
                'change':  alarm['source']['%change']}]

  elif event in ['bandwidth decreased from/to multiple sites', 'bandwidth increased from/to multiple sites']:
    for i,s in enumerate(alarm['source']['dest_sites']):
      temp = {'src_site': alarm['source']['site'],
              'dest_site': s,
              'ipv6': alarm['source']['ipv6'],
              'change':  alarm['source']['dest_change'][i]}
      sitePairs.append(temp)

    for i,s in enumerate(alarm['source']['src_sites']):
      temp = {'src_site': s,
              'dest_site': alarm['source']['site'],
              'ipv6': alarm['source']['ipv6'],
              'change':  alarm['source']['src_change'][i]}
      sitePairs.append(temp)
  
  return sitePairs


def layout(q=None, **other_unknown_query_strings):
    if q:
      alarm = getAlarm(q)
      print(q)
      sitePairs = getSitePairs(alarm)
      alarmData = alarm['source']

      cntAlarms = OtherAlarms(currEvent=alarm['event'],
                        alarmEnd=alarmData['to'],
                        site=alarmData['site'] if 'site' in alarmData.keys() else None,
                        src_site=alarmData['src_site'] if 'src_site' in alarmData.keys() else None,
                        dest_site=alarmData['dest_site'] if 'dest_site' in alarmData.keys() else None).formatted

      expand = True
      alarmsIn48h = ''
      if alarm['event'] not in ['bandwidth decreased', 'bandwidth increased']:
        expand = False
        alarmsIn48h = dbc.Row([
                        dbc.Row([
                              html.P(f'Site {alarmData["site"]} takes part in the following alarms in the period 24h prior and up to 24h after the current alarm end ({alarmData["to"]})', className='subtitle'),
                              html.B(cntAlarms, className='subtitle')
                          ], className="boxwithshadow alarm-header pair-details g-0", justify="between", align="center"),
                      ], style={"padding": "0.5% 1.5%"}, className='g-0')


      return html.Div([
              dbc.Row([
                dbc.Row([
                  dbc.Col([
                    html.H3(f"{alarm['event'].upper()}", className="text-center bold p-4"),
                  ], width=2),
                  dbc.Col(
                      html.Div(
                        [
                          dbc.Row([
                            dbc.Row([
                              html.H1(f"Summary", className="text-left"),
                              html.Hr(className="my-2")]),
                            dbc.Row([
                              html.P(buildSummary(alarm), className='subtitle'),
                              ], justify="start"),
                            ])
                        ],
                      ),
                  width=10)
                ], className="boxwithshadow alarm-header pair-details g-0", justify="between", align="center")                
              ], style={"padding": "0.5% 1.5%"}, className='g-0'),
            alarmsIn48h,
            dcc.Store(id='alarm-store', data=alarm),
            dbc.Row([
              html.Div(id=f'site-section-throughput{i}',
                children=[
                  dbc.Button(
                      alarmData['src_site']+' to '+alarmData['dest_site'],
                      value = alarmData,
                      id={
                          'type': 'tp-collapse-button',
                          'index': f'throughput{i}'
                      },
                      className="collapse-button",
                      color="white",
                      n_clicks=0,
                  ),
                  dcc.Loading(
                    dbc.Collapse(
                        id={
                            'type': 'tp-collapse',
                            'index': f'throughput{i}'
                        },
                        is_open=expand, className="collaps-container rounded-border-1"
                  ), color='#e9e9e9', style={"top":0}),
                ]) for i, alarmData in enumerate(sitePairs)
              ], className="rounded-border-1 g-0", align="start", style={"padding": "0.5% 1.5%"})
            ])



@dash.callback(
    [
      Output({'type': 'tp-collapse', 'index': MATCH},  "is_open"),
      Output({'type': 'tp-collapse', 'index': MATCH},  "children")
    ],
    [
      Input({'type': 'tp-collapse-button', 'index': MATCH}, "n_clicks"),
      Input({'type': 'tp-collapse-button', 'index': MATCH}, "value"),
      Input('alarm-store', 'data')
    ],
    [State({'type': 'tp-collapse', 'index': MATCH},  "is_open")],
)
def toggle_collapse(n, alarmData, alarm, is_open):
  data = ''
  if n:
    if is_open==False:
      data = buildGraphComponents(alarmData, alarm['source']['from'], alarm['source']['to'], alarm['event'])
    return [not is_open, data]
  if is_open==True:
    data = buildGraphComponents(alarmData, alarm['source']['from'], alarm['source']['to'], alarm['event'])
    return [is_open, data]
  return [is_open, data]



@timer
def buildGraphComponents(alarmData, dateFrom, dateTo, event):
  df = getRawDataFromES(alarmData['src_site'], alarmData['dest_site'], alarmData['ipv6'], dateFrom, dateTo)
  df.loc[:, 'MBps'] = df['throughput'].apply(lambda x: round(x/1e+6, 2))
  
  cntAlarms = OtherAlarms(currEvent=event, alarmEnd=dateTo, src_site=alarmData['src_site'], dest_site=alarmData['dest_site']).formatted

  return html.Div(children=[
          dbc.Row([
            dbc.Col([
              dbc.Row([
                dbc.Col([
                  dbc.Row([html.P('Source', className="text-center")]),
                  dbc.Row([html.B(alarmData['src_site'], className="text-center")], align="center", justify="start"),
                ]),
              ], className="more-alarms change-section rounded-border-1 mb-1", align="center"),
              dbc.Row([
                dbc.Col([
                  dbc.Row([html.P('Destination', className="text-center")]),
                  dbc.Row([html.B(alarmData['dest_site'], className="text-center")], align="center", justify="start",),
                ])
              ], className="more-alarms change-section rounded-border-1 mb-1", align="center"),
              dbc.Row([
                dbc.Col([
                  dbc.Row([html.B(f"Change: {alarmData['change']}%", className="text-center site-details")]),
                ])
              ], className="more-alarms change-section rounded-border-1 mb-1", align="center"),

              dbc.Row([
                html.H4( f"Total number of throughput measures: {len(df)}", className="text-center"),
                html.H4(f"Other networking alarms", className="text-center"),
                html.H4(cntAlarms, className="text-center"),
              ], align="left", justify="center", className="more-alarms change-section rounded-border-1 mb-1 pb-4")
            ], width={"size":"4"}, align="center"),
            dbc.Col([
              dcc.Loading([
                html.Div(dcc.Graph(className="site-plots site-inner-cont", figure=buildPlot(df))),
              ], className='loader-tp', color='#00245A', style = {"position": "relative", "top": "10rem"})
            ], width=7)
          ], className="", justify="evenly"),

          dbc.Row(
            html.Div(buildDataTable(df), className='single-table p-4'),
          justify="evenly")

        ], className='boxwithshadow')



def buildDataTable(df):
    return html.Div(dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{"name": i, "id": i} for i in df.columns],
            id='tbl-raw',
            style_table={
                'overflowX': 'auto'
            },
            page_action="native",
            page_current= 0,
            page_size= 10,
            style_cell={
              'padding': '3px'
            },
            style_header={
                'fontWeight': 'bold'
            },
            style_data={
                'height': 'auto',
                'lineHeight': '15px',
            },
            filter_action="native",
            sort_action="native",
        ))