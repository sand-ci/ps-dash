import dash
from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, MATCH, State
import plotly.graph_objects as go
import plotly.express as px

from elasticsearch.helpers import scan

import pandas as pd
from datetime import datetime
import requests
from urllib3.exceptions import InsecureRequestWarning
import urllib3


import utils.helpers as hp
from utils.helpers import timer
from model.Alarms import Alarms
import model.queries as qrs
from utils.parquet import Parquet


urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

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

# TODO: move that query to queries.py
@timer
def getRawDataFromES(src, dest, ipv6, dateFrom, dateTo):
    pq = Parquet()
    metaDf = pq.readFile('parquet/raw/metaDf.parquet')
    sips = metaDf[(metaDf['site'] == src) | (metaDf['netsite'] == src)]['ip'].values.tolist()
    sips = [ip.upper() for ip in sips] + [ip.lower() for ip in sips]
    dips = metaDf[(metaDf['site'] == dest) | (metaDf['netsite'] == dest)]['ip'].values.tolist()
    dips = [ip.upper() for ip in dips] + [ip.lower() for ip in dips]

    if len(sips) > 0 or len(dips) > 0:
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

    else: print(f'No IPs found for the selected sites {src} and {dest} {ipv6}')
    


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
def getSitePairs(alarm):
  sitePairs = []
  event = alarm['event']

  if event in ['bandwidth decreased', 'bandwidth increased']:
    sitePairs = [{'src_site': alarm['source']['src_site'],
                'dest_site': alarm['source']['dest_site'],
                'ipv6': alarm['source']['ipv6'],
                'change':  alarm['source']['change']}]

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


alarmsInst = Alarms()

def layout(q=None, **other_unknown_query_strings):
  if q:
    alarm = qrs.getAlarm(q)
    print('URL query:', q)
    print()

    sitePairs = getSitePairs(alarm)
    alarmData = alarm['source']
    dateFrom, dateTo = hp.getPriorNhPeriod(alarmData['to'])
    print('Alarm\'s content:', alarmData)
    pivotFrames = alarmsInst.loadData(dateFrom, dateTo)[1]

    data = alarmsInst.getOtherAlarms(
                                    currEvent=alarm['event'],
                                    alarmEnd=alarmData['to'],
                                    pivotFrames=pivotFrames,
                                    site=alarmData['site'] if 'site' in alarmData.keys() else None,
                                    src_site=alarmData['src_site'] if 'src_site' in alarmData.keys() else None,
                                    dest_site=alarmData['dest_site'] if 'dest_site' in alarmData.keys() else None
                                    )

    otherAlarms = alarmsInst.formatOtherAlarms(data)

    expand = True
    alarmsIn48h = ''
    if alarm['event'] not in ['bandwidth decreased', 'bandwidth increased']:
      expand = False
      alarmsIn48h = dbc.Row([
                      dbc.Row([
                            html.P(f'Site {alarmData["site"]} takes part in the following alarms in the period 24h prior \
                              and up to 24h after the current alarm end ({alarmData["to"]})', className='subtitle'),
                            html.B(otherAlarms, className='subtitle')
                        ], className="boxwithshadow alarm-header pair-details g-0 p-3", justify="between", align="center"),
                    ], style={"padding": "0.5% 1.5%"}, className='g-0')


    return html.Div([
            dbc.Row([
              dbc.Row([
                dbc.Col([
                  html.H3(alarm['event'].upper(), className="text-center bold p-3"),
                  html.H5(alarmData['to'], className="text-center bold"),
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

  dateFrom, dateTo = hp.getPriorNhPeriod(alarm['source']['to'])
  pivotFrames = alarmsInst.loadData(dateFrom, dateTo)[1]
  if n:
    if is_open==False:
      data = buildGraphComponents(alarmData, alarm['source']['from'], alarm['source']['to'], alarm['event'], pivotFrames)
    return [not is_open, data]
  if is_open==True:
    data = buildGraphComponents(alarmData, alarm['source']['from'], alarm['source']['to'], alarm['event'], pivotFrames)
    return [is_open, data]
  return [is_open, data]



@timer
def buildGraphComponents(alarmData, dateFrom, dateTo, event, pivotFrames):

  df = getRawDataFromES(alarmData['src_site'], alarmData['dest_site'], alarmData['ipv6'], dateFrom, dateTo)
  df.loc[:, 'MBps'] = df['throughput'].apply(lambda x: round(x/1e+6, 2))

  
  data = alarmsInst.getOtherAlarms(currEvent=event, alarmEnd=dateTo, pivotFrames=pivotFrames,
                                   src_site=alarmData['src_site'], dest_site=alarmData['dest_site'])

  otherAlarms = alarmsInst.formatOtherAlarms(data)

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
                html.H4(otherAlarms, className="text-center"),
              ], align="left", justify="center", className="more-alarms change-section rounded-border-1 mb-1 pb-4")
            ], width={"size":"4"}, align="center"),
            dbc.Col([
              dcc.Loading([
                html.Div(dcc.Graph(className="site-plots site-inner-cont", figure=buildPlot(df))),
              ], className='loader-tp', color='#00245A', style = {"position": "relative", "top": "10rem"})
            ], width=7)
          ], className="", justify="evenly"),

          dbc.Row(
            html.Div(buildDataTable(df), className='single-table p-2'),
          justify="evenly")

        ], className='boxwithshadow')



def buildDataTable(df):
    columns = ['dt', 'pair', 'throughput', 'src_host', 'dest_host',
              'retransmits', 'src_site', 'src_netsite', 'src_rcsite', 'dest_site',
              'dest_netsite', 'dest_rcsite', 'src_production', 'dest_production']

    df = df[columns]
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
            filter_options={"case": "insensitive"},
            sort_action="native",
        ))