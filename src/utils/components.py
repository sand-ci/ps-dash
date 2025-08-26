"""
This file contains functions which generates and returns the components which are reused in different pages.
"""

from dash import dcc, html
import dash_bootstrap_components as dbc
from utils.utils import SitesOverviewPlots, descChange, buildPlot, buildDataTable
from model.Alarms import Alarms
import plotly.graph_objects as go
from datetime import datetime
import utils.helpers as hp
from utils.helpers import DATE_FORMAT
from utils.parquet import Parquet
from utils.utils import asnAnomaliesGroupedAlarmVisualisation, generate_graphs
import model.queries as qrs
import pandas as pd
###############################################
            #ASN path anomalies.py
###############################################
def asnAnomalesPerSiteVisualisation(query_params):
    alarm_id = query_params['id']
    src = query_params['src']
    dest = query_params['dest']
    dt = query_params['dt']
    if alarm_id:
        site = query_params['site']
        dt = query_params['date']
        print("Alarm per site")
        print(site, dt, id)
        alarm = qrs.getAlarm(alarm_id)
        print('URL query:', alarm)
        print("HERE")
        asn_alarms_src = alarm['source']['all_alarm_ids_src']
        asn_alarms_dest = alarm['source']['all_alarm_ids_dest']
        asn_alarms_src_component = asnAnomaliesGroupedAlarmVisualisation(alarm, site, asn_alarms_src, asn_alarms_dest, dt, '-site')
        return asn_alarms_src_component, dict()
    if src and dest and dt:
        data = qrs.query_ASN_anomalies(src, dest, dt)
        if len(data) == 0:
            return html.Div([
                html.H1(f"No data found for alarm {src} to {dest}"),
                html.P('No data was found for the alarm selected. Please try another alarm.',
                    className="plot-sub")
            ], className="l-h-3 p-2 boxwithshadow page-cont ml-1 p-1"), dict()

        anomalies = data['anomalies'].values[0]
        title = html.H1(children=html.Div([
            dbc.Row([
                html.Span(f"{src} → {dest}", className="sites-anomalies-title")]),
            dbc.Row([
                html.Span("", style={"margin": "0 10px", "color": "#C50000", "fontSize": "40px"}),
                html.Span(f"Anomalies: {anomalies}", className="anomalies-title", style={"color": "#910000"}),
            ])     
        ], style={"textAlign": "center", "marginBottom": "20px"})
        )

        figures = generate_graphs(data, src, dest, dt)
        return html.Div([
            title,
            figures]), data.to_dict()
        
    return html.Div('No data available for these parameters.'), dict()


###############################################
            #site.py
###############################################

def siteMeasurements(q, pq):
  return dbc.Row(
                    dbc.Card([
                        dbc.CardHeader(html.H3(f'{q.upper()} network measurements',
                                                className="card-title text-center", style={'color': 'white'}), style={'background-color': '#00245a'}),
                        dbc.CardBody([
                            html.Div(
                                dcc.Graph(id="site-plots-in-out", 
                                figure=SitesOverviewPlots(q, pq),
                                config={'displayModeBar': False},# remove Plotly buttons so that they don't ovelap with the legend
                                className="site-plots site-inner-cont p-05")
                            )
                        ], className="text-dark p-1")
                    ], className="boxwithshadow"),
                    className="mb-4 site-alarms-tables page-cont mb-1 g-0"
                )
  
def getColor(asn, diffs):
  if asn in diffs:
    return '#ff5f5f'
  return 'rgb(0 35 90)'

def singlePlotPositions(dd):
    dd.replace(-1, 'OFF', inplace=True)
    fig = go.Figure(go.Heatmap(
            y = dd['asn'].astype(str).values,
            x = dd['pos'].astype(str).values,
            z = dd['P'].values,
            zmin=0, zmax=1,
            text=dd['asn'].values,
            texttemplate="%{text}", colorscale='deep'
        )
    )

    fig.update_annotations(font_size=12)
    fig.update_layout(template='plotly_white',
                      yaxis={'visible': False, 'showticklabels': False},
                      xaxis={'title': 'Position on the path'},
                      title_font_size=12
                     )

    return go.Figure(data=fig)
  
def pairDetails(pair, alarm, chdf, baseline, altpaths, hopPositions, pivotFrames, alarmsInst=Alarms()):
  print("baseline2")
  print(baseline)
  sites = baseline[['src_site','dest_site']].values.tolist()[0]
  howPathChanged = descChange(pair, chdf, hopPositions)
  diffs = chdf[chdf["pair"]==pair]['diff'].to_list()[0]

  print()
  print("otherAlarms", alarm['to'], sites)
  data = alarmsInst.getOtherAlarms(currEvent='path changed', alarmEnd=alarm['to'], pivotFrames=pivotFrames, src_site=sites[0], dest_site=sites[1])
  print(data)
  otherAlarms = alarmsInst.formatOtherAlarms(data)

  return   html.Div([
            dbc.Row([
              dbc.Col([
                dbc.Row([
                  dbc.Col([
                    dbc.Row([html.P('Source', className="text-center")]),
                    dbc.Row([html.B(sites[0], className="text-center")], align="start", justify="start"),
                    dbc.Row([html.P(baseline['src'].values[0], className="text-center")], justify="start",)
                  ]),
                ], className="more-alarms change-section rounded-border-1 mb-1", align="start"),
                dbc.Row([
                  dbc.Col([
                    dbc.Row([html.P('Destination', className="text-center")]),
                    dbc.Row([html.B(sites[1], className="text-center")], align="start", justify="start",),
                    dbc.Row([html.P(baseline['dest'].values[0], className="text-center")], justify="start",)
                  ])
                ], className="more-alarms change-section rounded-border-1 mb-1", align="start", ),


                dbc.Row([
                  html.H4(f"Total number of traceroute measures: {baseline['cnt_total_measures'].values[0]}", className="text-center"),
                  html.H4(f"Other networking alarms: {otherAlarms}", className="text-center"),
                ], align="left", justify="start", className="more-alarms change-section rounded-border-1 mb-1 pb-4"
                ),

                dbc.Row([
                  dbc.Row([
                    dbc.Row([
                      dbc.Col([
                        html.H2(f"BASELINE PATH", className="label text-center mb-1")
                      ])
                    ]),
                    dbc.Row([
                      dbc.Col([
                        dbc.Badge(f"Taken in {str(round(baseline['hash_freq'].values.tolist()[0]*100))}% of time", text_color="dark", color="#e9e9e9",  className="w-100")
                        ], lg=6, md=12),
                      dbc.Col([
                        dbc.Badge(f"Always reaches destination: {['YES' if baseline['path_always_reaches_dest'].values[0] else 'NO'][0]}", text_color="dark", color="#e9e9e9",  className="w-100")
                        ], lg=6, md=12),
                    ], justify="center", align="center", className="bg-gray rounded-border-1"),
                    dbc.Row([
                        html.Div(children=[
                          dbc.Badge(asn, text_color="light", color='rgb(0 35 90)', className="me-2") for asn in baseline["asns_updated"].values.tolist()[0]
                        ], className="text-center"),
                      ], className='mb-1'),
                  ], className='baseline-section'),
                  dbc.Row([
                    html.H2(f"ALTERNATIVE PATHS", className="label text-center mb-1"),
                    dbc.Row(children=[
                        html.Div(children=[
                          dbc.Row([
                            dbc.Col([
                              dbc.Badge(f"Taken in {str(round(hash_freq*100,3))}% of time", text_color="dark", color="rgb(255, 255, 255)",  className="w-100")
                              ]),
                            dbc.Col([
                              dbc.Badge(f"Always reaches destination: {['YES' if path_always_reaches_dest else 'NO'][0]}", text_color="dark", color="rgb(255, 255, 255))",  className="w-100")
                              ]),
                          ], className="bg-white"),
                          
                          html.Div(children=[
                            dbc.Badge(asn, text_color="light", color=getColor(asn, diffs), className="me-2") for asn in path])
                        ], className="text-center mb-1") for path, hash_freq, path_always_reaches_dest in altpaths[["asns_updated","hash_freq","path_always_reaches_dest"]].values.tolist()
                    ])
                    ], justify="center", align="center")
                ], className="bordered-box p-1")
              ], className="pl-1 pr-1"),

              dbc.Col([
                dbc.Row([
                  dcc.Graph(id="graph-hops", figure=singlePlotPositions(hopPositions), className="p-1 bordered-box mb-0"),
                  html.P(
                      f"The plot shows the AS numbers for every hop and the frequency of their occurences at each position (source and destination not included).",
                      className="text-center mb-0 fs-5"),
                  html.P(
                      f"The dark blue values of 1 mean the ASN was always used at that position; \
                        Close to 0, means the ASN rarely appeared; \
                        OFF indicates the device did not respond at the time of the traceroute test;  \
                        0 is when there was a responce, but the ASN was unknown.",
                      className="text-center mb-0 fs-5"),
                ], align="start", className="mb-1"),
                dbc.Row([
                  html.Div(children=[

                      dbc.Row([
                        dbc.Col(html.P('At position'), className=' text-right'),
                        dbc.Col(html.P('Typically goes through'), className='text-center'),
                        dbc.Col(html.P('Changed to'), className=' text-center')
                      ]),
                      dbc.Row([
                        dbc.Col(html.B(str(round(item['atPos']))), className='text-right'),
                        dbc.Col(html.B(item['jumpedFrom']), className=' text-center'),
                        dbc.Col(html.B(item['diff']), className=' text-center')
                      ]),
                      dbc.Row([
                        dbc.Col(html.P(''), className=''),
                        dbc.Col(html.P(item['jumpedFromOwner']), className=' text-center'),
                        dbc.Col(html.P(item['diffOwner']), className=' text-center')
                      ]),
                    ], className='mb-1 change-section rounded-border-1')
                  for item in howPathChanged.to_dict('records') ], className="rounded-border-1", align="start")
                    ], align="start", className=''),

            ], justify="start", align="start"),
          ])

###############################################
            #loss_delay.py
###############################################

def obtainFieldNames(dateFrom):
  # Date when all latency alarms started netsites instead of sites
  target_date = datetime(2023, 9, 21)
  
  date_from = datetime.strptime(dateFrom, DATE_FORMAT)
  
  if date_from > target_date:
    return {'src': 'src_netsite', 'dest': 'dest_netsite'}
  else:
    return {'src': 'src_site', 'dest': 'dest_site'}


def loss_delay_kibana(alarmContent, event):
    query = ''
    if 'src' in alarmContent.keys() and 'dest' in alarmContent.keys():
        query = f'src:{alarmContent["src"]} and dest:{alarmContent["dest"]}'
    if 'src_host' in alarmContent.keys() and 'dest_host' in alarmContent.keys():
        query = f'src_host:{alarmContent["src_host"]} and dest_host:{alarmContent["dest_host"]}'
    elif 'host' in alarmContent.keys() and event == 'high packet loss on multiple links':
        query = f'src_host: {alarmContent["host"]} or dest_host: {alarmContent["host"]}'
    elif 'host' in alarmContent.keys():
        query = f'dest_host: {alarmContent["host"]}'
    dates = hp.getPriorNhPeriod(alarmContent['to'], daysBefore=3, midPoint=False)
    timeRange = f"(from:'{dates[0]}',to:'{dates[1]}')"
    fieldName = obtainFieldNames(dates[0]) 

    pq = Parquet()
    metaDf = pq.readFile('parquet/raw/metaDf.parquet')

    # Default (generic) link
    url = (
        "https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards"
        "?auth_provider_hint=anonymous1#/view/e015c210-65e2-11ed-afcf-d91dad577662"
        f"?embed=true&_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:{timeRange})"
        f"&show-query-input=true&show-time-filter=true&_a=(query:(language:kuery,query:'{query}'))"
    )
    special_urls = {'high packet loss on multiple links': ["https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/ee5a6310-8c40-11ed-8156-b9b28813464d", "https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/920cd1f0-8c41-11ed-8156-b9b28813464d", "host", "host"],
                    'high delay from/to multiple sites': ["https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/ecf72311-a68f-4882-bbd3-e9ceb5f20c9f", "https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/faa80c6e-b12d-4772-a187-056b79f5e1f0", 'netsite', 'site'],
                    'high one-way delay': ["https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/faa80c6e-b12d-4772-a187-056b79f5e1f0", 'netsite', 'site']
                  }
    
    kibanaIframe = []

    if event in special_urls:
        url_base = f"?embed=true&_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:{timeRange})&show-query-input=true&show-time-filter=true"
        if len(alarmContent.get("dest_sites", [])) > 0:
            original_names = metaDf[metaDf['netsite'].isin(alarmContent["dest_sites"])]['netsite'].unique()
            dest_sites = str(list(s for s in set(original_names))).replace("'", '"').replace('[','').replace(']','').replace(',', ' OR')
            query = f'src_{special_urls[event][2]}: {alarmContent[special_urls[event][3]]} and {fieldName["dest"]}:({dest_sites})'
            query = f"&_a=(query:(language:kuery,query:'{query}'))"
            url = f"{special_urls[event][0]}{url_base}{query}"
            kibanaIframe.append(
                dbc.Row([
                    dbc.Row(html.H3(f"Issues from {alarmContent['site']}")),
                    dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
                ], className="boxwithshadow pair-details g-0 mb-1 p-3")
            )

        if len(alarmContent.get("src_sites", [])) > 0:
            original_names = metaDf[metaDf['netsite'].isin(alarmContent["src_sites"])]['netsite'].unique()
            src_sites = str(list(s for s in set(original_names))).replace("'", '"').replace('[','').replace(']','').replace(',', ' OR')
            query = f'dest_{special_urls[event][2]}: {alarmContent[special_urls[event][3]]} and {fieldName["src"]}:({src_sites})'
            query = f"&_a=(query:(language:kuery,query:'{query}'))"
            url = f"{special_urls[event][1]}{url_base}{query}"
            
            kibanaIframe.append(
                dbc.Row([
                    dbc.Row(html.H3(f"Issues to {alarmContent['site']}")),
                    dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
                ], className="boxwithshadow pair-details g-0 mb-1 p-3")
            )
        if event == "high one-way delay":
            query = f"&_a=(query:(language:kuery,query:'{query}'))"
            url = f"{special_urls[event][0]}{url_base}{query}"
            kibanaIframe.append(
                dbc.Row([
                    dbc.Row(html.H3(f"Issues between {alarmContent['src_site']} and {alarmContent['dest_site']}")),
                    dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
                ], className="boxwithshadow pair-details g-0 mb-1 p-3")
            )
    else:
        kibanaIframe = dbc.Row(
            [html.Iframe(src=url, style={"height": "1000px"})],
            className="boxwithshadow pair-details g-0"
        )

    return dbc.Row(kibanaIframe, style={"padding": "0.5% 1.5%"}, className='g-0 p-3')

###############################################
            #throughput.py
###############################################

def bandwidth_increased_decreased(alarm, alarmData, alarmsInst, alarmsIn48h, sitePairs, expand, index_extension=""):
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
                        'type': 'tp-collapse-button'+index_extension,
                        'index': f'throughput{i}'+index_extension
                    },
                    className="collapse-button",
                    color="white",
                    n_clicks=0,
                ),
                dcc.Loading(
                  dbc.Collapse(
                      id={
                          'type': 'tp-collapse'+index_extension,
                          'index': f'throughput{i}'+index_extension
                      },
                      is_open=expand, className="collaps-container rounded-border-1"
                ), color='#e9e9e9', style={"top":0}),
              ]) for i, alarmData in enumerate(sitePairs)
            ], className="rounded-border-1 g-0", align="start", style={"padding": "0.5% 1.5%"})
          ])

def throughput_graph_components(alrmData, df, otherAlarms):
    
    return html.Div(children=[
          dbc.Row([
            dbc.Col([
              dbc.Row([
                dbc.Col([
                  dbc.Row([html.P('Source', className="text-center")]),
                  dbc.Row([html.B(alrmData['src_site'], className="text-center")], align="center", justify="start"),
                ]),
              ], className="more-alarms change-section rounded-border-1 mb-1", align="center"),
              dbc.Row([
                dbc.Col([
                  dbc.Row([html.P('Destination', className="text-center")]),
                  dbc.Row([html.B(alrmData['dest_site'], className="text-center")], align="center", justify="start",),
                ])
              ], className="more-alarms change-section rounded-border-1 mb-1", align="center"),
              dbc.Row([
                dbc.Col([
                  dbc.Row([html.B(f"Change: {alrmData['change']}%", className="text-center site-details")]),
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
    
