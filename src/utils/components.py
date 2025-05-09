"""
This file contains functions which generates and returns the components which are reused in different pages.
"""

from dash import dcc, html
import dash_bootstrap_components as dbc
from flask import request
from utils.utils import extractRelatedOnly, SitesOverviewPlots, descChange, getRawDataFromES, buildPlot, buildDataTable
from model.Alarms import Alarms
import plotly.graph_objects as go


###############################################
            #path_changed.py
###############################################

def siteBoxPathChanged(site, pairCount, chdf, posDf, baseline, altPaths, alarm, pivotFrames, alarmsInst=Alarms(), button_name=""):
    cnt = pairCount[pairCount['site']==site]['pair'].values[0]
    chdf = chdf[(chdf['src_site']==site) | (chdf['dest_site']==site)]
    posDf = posDf[(posDf['src_site']==site) | (posDf['dest_site']==site)]
    baseline = baseline[(baseline['src_site']==site) | (baseline['dest_site']==site)]
    altPaths = altPaths[(altPaths['src_site']==site) | (altPaths['dest_site']==site)]
    if site:
      baseline['spair'] = baseline['src_site']+' -> '+baseline['dest_site']

      showSample = ''
      related2FlaggedASN = extractRelatedOnly(chdf, alarm['asn'])
      cntRelated = len(related2FlaggedASN)
      if len(related2FlaggedASN)>10:
        showSample = "Bellow is a subset of 5."
        related2FlaggedASN = related2FlaggedASN.sample(5)
      sitePairs = related2FlaggedASN['spair'].values
      nodePairs = related2FlaggedASN['pair'].values
      
      
      diffs = sorted(list(set([str(el) for v in chdf['diff'].values.tolist() for el in v])))
      diffs_str = '  |  '.join(diffs)
      
      data = alarmsInst.getOtherAlarms(currEvent='path changed', alarmEnd=alarm["to"], pivotFrames=pivotFrames, site=site)
      otherAlarms = alarmsInst.formatOtherAlarms(data)

      url = f'{request.host_url}paths-site/{site}?dateFrom={alarm["from"]}&dateTo={alarm["to"]}'

      return html.Div([
              dbc.Row([
                dbc.Row([
                  dbc.Col([
                    dbc.Row([
                       dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.P(f"{cnt} is the total number of traceroute alarms which involve site {site}", className='card-text'),
                                    html.P(f"{cntRelated} (out of {cnt}) concern a path change to ASN {alarm['asn']}. {showSample}", className='card-text'),
                                    html.P(f"Other flagged AS numbers:  {diffs_str}", className='card-text')
                                ])
                            ], className='mb-3 h-100'),
                       ], lg=6, md=12),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.P(f'Site {site} takes part in the following alarms in the period 24h prior and up to 24h after the current alarm end ({alarm["to"]})', className='card-text'),
                                    html.B(otherAlarms, className='card-text')
                                ])
                            ], className='mb-3 h-100')
                        ], lg=6, md=12)
                      ], 
                      className='site-header-line p-2 d-flex',
                      justify="center", align="stretch"),

                    dbc.Row(
                    [
                      html.Div(id=f'pair-section{site+str(i)}',
                        children=[
                          dbc.Button(
                              sitePairs[i],
                              value=pair,
                              id={
                                  'type': 'collapse-button'+button_name,
                                  'index': site+str(i)+button_name
                              },
                              className="collapse-button",
                              color="white",
                              n_clicks=0,
                          ),
                          dcc.Loading(
                            dbc.Collapse(
                                id={
                                    'type': 'collapse'+button_name,
                                    'index': site+str(i)+button_name
                                },
                                is_open=False, className="collaps-container rounded-border-1"
                          ), style={'height':'0.5rem'}, color='#e9e9e9'),
                        ]
                      ) for i, pair in enumerate(nodePairs)
                    ],),
                    dbc.Row(
                        html.Div(
                              html.A('Show all "Path changed" alarms for that site in a new page', href=url, target='_blank',
                                   className='btn btn-secondary site-btn mb-2',
                                   id={
                                       'type': 'site-new-page-btn',
                                       'index': site
                                   }
                              ),
                        ), align='center'
                      )
                  ]),
                ]),
              ])
            ])


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

  
def toggleCollapse(chdf, posDf, baseline, altPaths, n, pair, alarm, is_open, pivotFrames, alarmsInst=Alarms()):
    data = ''
    if n:
      if is_open==False:
        # chdf, posDf, baseline, altPaths = pq.readFile(f'{location}chdf.parquet'), pq.readFile(f'{location}posDf.parquet'), pq.readFile(f'{location}baseline.parquet'), pq.readFile(f'{location}altPaths.parquet')
        data = pairDetails(pair, alarm,
                        chdf[(chdf['pair']==pair) & (chdf['from_date']==alarm['from']) & (chdf['to_date']==alarm['to'])],
                        baseline[(baseline['pair']==pair) & (baseline['from_date']==alarm['from']) & (baseline['to_date']==alarm['to'])],
                        altPaths[(altPaths['pair']==pair) & (altPaths['from_date']==alarm['from']) & (altPaths['to_date']==alarm['to'])],
                        posDf[(posDf['pair']==pair) & (posDf['from_date']==alarm['from']) & (posDf['to_date']==alarm['to'])],
                        pivotFrames,
                        alarmsInst)
      return [not is_open, data]
    return [is_open, data]
  

###############################################
            #loss_delay.py
###############################################

from datetime import datetime
import utils.helpers as hp
from utils.helpers import DATE_FORMAT
from model.Alarms import Alarms
from utils.parquet import Parquet

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

    alarmsInst = Alarms()
    url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/e015c210-65e2-11ed-afcf-d91dad577662?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&_a=(query:(language:kuery,query:\'{query}\'))'
    
    kibanaIframe = []
    if event == 'high packet loss on multiple links':
      if len(alarmContent["dest_sites"])>0:
        original_names = metaDf[metaDf['netsite'].isin(alarmContent["dest_sites"])]['netsite'].unique()
        dest_sites = str(list(s for s in set(original_names))).replace('\'', '"').replace('[','').replace(']','').replace(',', ' OR')
        query = f'src_host: {alarmContent["host"]} and {fieldName["dest"]}:({dest_sites})'
        url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/ee5a6310-8c40-11ed-8156-b9b28813464d?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&hide-filter-bar=true&_a=(query:(language:kuery,query:\'{query}\'))'
        # print('\n \n',url)
        kibanaIframe.append(dbc.Row([
            dbc.Row(html.H3(f"Issues from {alarmContent['site']}")),
            dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
          ], className="boxwithshadow pair-details g-0 mb-1 p-3"))
        
      if len(alarmContent["src_sites"]) > 0:
        original_names = metaDf[metaDf['netsite'].isin(alarmContent["src_sites"])]['netsite'].unique()
        src_sites = str(list(s for s in set(original_names))).replace('\'', '"').replace('[','').replace(']','').replace(',', ' OR')
        query = f'dest_host: {alarmContent["host"]} and {fieldName["src"]}:({src_sites})'
        url = f'https://atlas-kibana.mwt2.org:5601/s/networking/app/dashboards?auth_provider_hint=anonymous1#/view/920cd1f0-8c41-11ed-8156-b9b28813464d?embed=true&_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A{timeRange})&show-query-input=true&show-time-filter=true&_a=(query:(language:kuery,query:\'{query}\'))'
        # print('\n \n',url)
        kibanaIframe.append(dbc.Row([
            dbc.Row(html.H3(f"Issues to {alarmContent['site']}")),
            dbc.Row(html.Iframe(src=url, style={"height": "600px"}))
          ], className="boxwithshadow pair-details g-0 mb-1 p-3"))
    else:
      kibanaIframe = dbc.Row([html.Iframe(src=url, style={"height": "1000px"})], className="boxwithshadow pair-details g-0")

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