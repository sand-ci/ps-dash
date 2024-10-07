import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, MATCH, ALL
from flask import request

from elasticsearch.helpers import scan

import plotly.graph_objects as go

import pandas as pd

from utils.helpers import timer

import utils.helpers as hp
import model.queries as qrs
from model.Alarms import Alarms

import urllib3
urllib3.disable_warnings()



def title(q=None):
    return f"Alarm ID: {q}"



def description(q=None):
    return f"Visual represention on a selected path-changed alarm {q}"



dash.register_page(
    __name__,
    path_template="/paths/<q>",
    title=title,
    description=description,
)



chdf, posDf, baseline, altPaths = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
dateFrom, dateTo, frames, pivotFrames =  None, None, None, None
alarmsInst = Alarms()


@timer
def layout(q=None, **other_unknown_query_strings):
    global chdf
    global posDf
    global baseline
    global altPaths
    global dateFrom
    global dateTo
    global frames
    global pivotFrames

    if q:
        alarm = qrs.getAlarm(q)['source']
        print('URL query:', q, other_unknown_query_strings)
        print()
        print('Alarm content:', alarm)

        chdf, posDf, baseline, altPaths = qrs.queryTraceChanges(alarm['from'], alarm['to'], alarm['asn'])
        dateFrom, dateTo = hp.getPriorNhPeriod(alarm["to"])
        frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
        posDf['asn'] = posDf['asn'].astype(int)

        dsts = chdf.groupby('dest_site')[['pair']].count().reset_index().rename(columns={'dest_site':'site'})
        srcs = chdf.groupby('src_site')[['pair']].count().reset_index().rename(columns={'src_site':'site'})
        pairCount = pd.concat([dsts, srcs]).groupby(['site']).sum().reset_index().sort_values('pair', ascending=False)
        topDownList = pairCount[pairCount['site'].isin(alarm['sites'])]['site'].values.tolist()

        # affected = '  |  '.join(alarm["sites"])

        selectedTab = topDownList[0]
        if 'site' in other_unknown_query_strings.keys():
          if other_unknown_query_strings['site'] in pairCount['site'].values:
            selectedTab = other_unknown_query_strings['site']

        return html.Div([
            dcc.Store(id='local-store', data=alarm),
            dcc.Location(id='url', refresh=True),
            html.P(id='test'),
            dbc.Row([
              dbc.Row([
                dbc.Col([
                    html.H3(f"ASN {alarm['asn']}", className="text-center bold"),
                    html.H3(alarm['owner'], className="text-center")
                  ], lg=2, md=12, className="p-3"
                ),
                dbc.Col(
                    html.Div(
                        [
                          dbc.Row([
                            dbc.Row([
                              html.H1(f"Summary", className="text-left"),
                              html.Hr(className="my-2")]),
                            dbc.Row([
                              html.P(f"During the period {alarm['from']} - {alarm['to']}, path between {alarm['num_pairs']} source-destination pairs diverged through ASN {alarm['asn']}.", className='subtitle'),
                              ], justify="start"),
                              dbc.Row([
                                dbc.Col(html.P(f"The change affected the sites bellow ", className='subtitle'), width='auto', align="center"),
                                # dbc.Col(html.B(affected, className='subtitle'), align="center"),
                              ], justify="start")
                            ], className="pair-details")
                        ],
                    ), lg=10, md=12, className="p-3"
                  ),
              ], justify="between", align="center", className="boxwithshadow alarm-header pair-details")
            ], style={"padding": "0.5% 1.5%"}, className='g-0 mb-1'),
            dcc.Store(id='window-width', storage_type='session'),
            dbc.Row([
              dbc.Row([
                  dcc.Tabs(id="asn-site-tabs", value=selectedTab, parent_className='custom-tabs', className='custom-tabs-container h-100',
                      children=[
                        dcc.Tab(label=site.upper(), value=site, 
                        id=site,
                        className='custom-tab text-center ', selected_className='custom-tab--selected rounded-border-1 p-1',
                        children=buildSiteBox(site,
                                              pairCount[pairCount['site']==site]['pair'].values[0], 
                                              chdf[(chdf['src_site']==site) | (chdf['dest_site']==site)],
                                              posDf[(posDf['src_site']==site) | (posDf['dest_site']==site)],
                                              baseline[(baseline['src_site']==site) | (baseline['dest_site']==site)],
                                              altPaths[(altPaths['src_site']==site) | (altPaths['dest_site']==site)],
                                              alarm)
                                              ) for site in topDownList
                        ], vertical=False, 
                        style={
                          # "width": "99%",
                          # "display": "inline-block"
                          }
                        )
              ], className="boxwithshadow")
            ], style={"padding": "0.5% 1.5%"}, className='g-0'),
          ])


def extractRelatedOnly(chdf, asn):
  chdf.loc[:, 'spair'] = chdf[['src_site', 'dest_site']].apply(lambda x: ' -> '.join(x), axis=1)

  diffData = []
  for el in chdf[['pair','spair','diff']].to_dict('records'):
      for d in el['diff']:
          diffData.append([el['spair'], el['pair'],d])

  diffDf = pd.DataFrame(diffData, columns=['spair', 'pair', 'diff'])
  return diffDf[diffDf['diff']==asn]



@timer
def buildSiteBox(site, cnt, chdf, posDf, baseline, altPaths, alarm):
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
                                  'type': 'collapse-button',
                                  'index': site+str(i)
                              },
                              className="collapse-button",
                              color="white",
                              n_clicks=0,
                          ),
                          dcc.Loading(
                            dbc.Collapse(
                                id={
                                    'type': 'collapse',
                                    'index': site+str(i)
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



@timer
@dash.callback(
    [
      Output({'type': 'collapse', 'index': MATCH},  "is_open"),
      Output({'type': 'collapse', 'index': MATCH},  "children")
    ],
    [
      Input({'type': 'collapse-button', 'index': MATCH}, "n_clicks"),
      Input({'type': 'collapse-button', 'index': MATCH}, "value"),
      Input('local-store', 'data')
    ],
    [State({'type': 'collapse', 'index': MATCH},  "is_open")],
)
def toggle_collapse(n, pair, alarm, is_open):
    data = ''
    global chdf
    global posDf
    global baseline
    global altPaths

    if n:
      if is_open==False:
        # chdf, posDf, baseline, altPaths = pq.readFile(f'{location}chdf.parquet'), pq.readFile(f'{location}posDf.parquet'), pq.readFile(f'{location}baseline.parquet'), pq.readFile(f'{location}altPaths.parquet')
        data = pairDetails(pair, alarm,
                        chdf[(chdf['pair']==pair) & (chdf['from_date']==alarm['from']) & (chdf['to_date']==alarm['to'])],
                        baseline[(baseline['pair']==pair) & (baseline['from_date']==alarm['from']) & (baseline['to_date']==alarm['to'])],
                        altPaths[(altPaths['pair']==pair) & (altPaths['from_date']==alarm['from']) & (altPaths['to_date']==alarm['to'])],
                        posDf[(posDf['pair']==pair) & (posDf['from_date']==alarm['from']) & (posDf['to_date']==alarm['to'])])
      return [not is_open, data]
    return [is_open, data]



def getColor(asn, diffs):
  if asn in diffs:
    return '#ff5f5f'
  return 'rgb(0 35 90)'



@timer
def pairDetails(pair, alarm, chdf, baseline, altpaths, hopPositions):

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



@timer
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


@timer
def descChange(pair, chdf, posDf):

  owners = qrs.getASNInfo(posDf[(posDf['pair']==pair)]['asn'].values.tolist())
  owners['-1'] = 'OFF/Unavailable'
  howPathChanged = []
  for diff in chdf[(chdf['pair']==pair)]['diff'].values.tolist()[0]:

    for pos, P in posDf[(posDf['pair']==pair)&(posDf['asn']==diff)][['pos','P']].values:
        atPos = posDf[(posDf['pair']==pair) & (posDf['pos']==pos)]['asn'].values.tolist()

        if len(atPos)>0:
          atPos.remove(diff)
          for newASN in atPos:
            if newASN not in [0, -1]:
              if P < 1:
                howPathChanged.append({'diff': diff,
                                      'diffOwner': owners[str(diff)], 'atPos': pos,
                                      'jumpedFrom': newASN, 'jumpedFromOwner': owners[str(newASN)]})
          # the following check is covering the cases when the change happened at the very end of the path
          # i.e. the only ASN that appears at that position is the diff detected
          if len(atPos) == 0:
            howPathChanged.append({'diff': diff,
                                  'diffOwner': owners[str(diff)], 'atPos': pos,
                                  'jumpedFrom': "No data", 'jumpedFromOwner': ''})

  if len(howPathChanged)>0:
    return pd.DataFrame(howPathChanged).sort_values('atPos')
  
  return pd.DataFrame()

