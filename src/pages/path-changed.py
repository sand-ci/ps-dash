from utils.parquet import Parquet
import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, MATCH

from elasticsearch.helpers import scan

import plotly.graph_objects as go

import pandas as pd

from utils.helpers import timer
from elasticsearch.helpers import scan

import utils.helpers as hp

import urllib3
urllib3.disable_warnings()



def title(q=None):
    return f"Asset Analysis: {q}"


def description(q=None):
    return f"This is the AVN Industries A   sset Analysis: {q}"


dash.register_page(
    __name__,
    path_template="/paths/<q>",
    title=title,
    description=description,
)

def plotRaw(vals):
    print(vals)



def getAlarm(id):
    q = {
        "query": {
          "term": {
            "_id": id
          }
        }
      }

    result = scan(client=hp.es,index='aaas_alarms',query=q)
    data = []

    for item in result:
        data.append(item['_source']['source'])
        
    if len(data) == 1:
      return data[0]



def getStats(fromDate, toDate, affectedSites):
    q = {
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "from_date.keyword": {
                      "value": fromDate
                    }
                  }
                },
                {
                  "term": {
                    "to_date.keyword": {
                      "value": toDate
                    }
                  }
                },
                {
                  "bool": {
                    "should": [
                      {
                        "terms": {
                          "src_site": affectedSites
                        }
                      },
                      {
                        "terms": {
                          "dest_site": affectedSites
                        }
                      }
                    ]
                  }
                }
              ]
            }
          }
        }
    print(str(q).replace("\'", "\""))
    result = scan(client=hp.es,index='ps_trace_changes',query=q)
    data, positions, baseline, altPaths = [],[],[],[]
    positions = []
    for item in result:
        
        tempD = {}
        for k,v in item['_source'].items():
            if k not in ['positions', 'baseline', 'alt_paths', 'created_at']:
                tempD[k] = v
        data.append(tempD)
        
        src,dest,src_site,dest_site = item['_source']['src'], item['_source']['dest'], item['_source']['src_site'], item['_source']['dest_site']
        
        temp = item['_source']['positions']
        for p in temp:
            p['src'] = src
            p['dest'] = dest
            p['src_site'] = src_site
            p['dest_site'] = dest_site
        positions.extend(temp)
        
        temp = item['_source']['baseline']
        for p in temp:
            p['src'] = src
            p['dest'] = dest
            p['src_site'] = src_site
            p['dest_site'] = dest_site
        baseline.extend(temp)
        
        altPaths = item['_source']['alt_paths']
        for p in temp:
            p['src'] = src
            p['dest'] = dest
            p['src_site'] = src_site
            p['dest_site'] = dest_site
        altPaths.extend(temp)
        
    df = pd.DataFrame(data)
    posDf = pd.DataFrame(positions)
    baseline = pd.DataFrame(baseline)
    altPaths = pd.DataFrame(altPaths)
    
    posDf['pair'] = posDf['src']+'  ->   '+posDf['dest']
    df['pair'] = df['src']+'  ->   '+df['dest']
    baseline['pair'] = baseline['src']+'  ->   '+baseline['dest']
    altPaths['pair'] = altPaths['src']+'  ->   '+altPaths['dest']
    
    return df, posDf, baseline, altPaths



def getOtherAlarmsCount(dateFrom, dateTo, src_site, dest_site):
  query = {
      "bool" : {
        "must" : [
          {
            "range" : {
              "source.from.keyword": {
                "gte" : dateFrom
              }
            }
          },
          {
            "range" : {
              "source.to.keyword": {
                "lte" : dateTo
              }
            }
          },
          {
            "term" : {
              "source.src_site.keyword": {
                "value": src_site,
                "case_insensitive": True
              }
            }
          },
          {
            "term" : {
              "source.dest_site.keyword": {
                "value": dest_site,
                "case_insensitive": True
              }
            }
          }
          ]
      }
  }

  aggs = {
      "alarms": {
        "terms": {
          "field": "event"
        }
      }
    }


  res = []

  # print(str(query).replace("\'", "\""))
  # print(str(aggs).replace("\'", "\""))
  print()
  aggdata = hp.es.search(index='aaas_alarms', query=query, size=0, aggs=aggs)

  for item in aggdata['aggregations']['alarms']['buckets']:
    if item:
      print(src_site, dest_site, item)
      res.append({
                  'event': item['key'],
                  'count': item['doc_count']
                })

  return res


def getASNInfo(ids):

    query = {
        "query": {
            "terms": {
                "_id": ids
            }
        }
    }

    print(str(query).replace("\'", "\""))
    asnDict = {}
    data = scan(hp.es, index='ps_asns', query=query)
    if data:
      for item in data:
          asnDict[str(item['_id'])] = item['_source']['owner']
    else: print(ids, 'Not found')

    return asnDict

@timer
def layout(q=None, **other_unknown_query_strings):
    pq = Parquet()
    location = 'parquet/raw/'
    print(q, other_unknown_query_strings)
    if q:
        alarm = getAlarm(q)

        chdf, posDf, baseline, altPaths = pq.readFile(f'{location}chdf.parquet'), pq.readFile(f'{location}posDf.parquet'), pq.readFile(f'{location}baseline.parquet'), pq.readFile(f'{location}altPaths.parquet')
        chdf = chdf[(chdf['from_date']==alarm['from']) & (chdf['to_date']==alarm['to'])]
        baseline = baseline[(baseline['from_date']==alarm['from']) & (baseline['to_date']==alarm['to'])]
        altPaths = altPaths[(altPaths['from_date']==alarm['from']) & (altPaths['to_date']==alarm['to'])]
        posDf = posDf[(posDf['from_date']==alarm['from']) & (posDf['to_date']==alarm['to'])]

        dsts = chdf.groupby('dest_site')[['pair']].count().reset_index().rename(columns={'dest_site':'site'})
        srcs = chdf.groupby('src_site')[['pair']].count().reset_index().rename(columns={'src_site':'site'})
        pairCount = pd.concat([dsts, srcs]).groupby(['site']).sum().reset_index().sort_values('pair', ascending=False)
        print(alarm)


        alarmDesc = {
            "description": "Code running every 12 hours at UC k8s cluster, checks in ps_trace for changes on the lists of AS numbers. Alarm is generated if an ASN is flagged between > 10 source-destination pairs."}

        affected = '  |  '.join(alarm["sites"])

        selectedTab = pairCount['site'].values[0]
        if 'site' in other_unknown_query_strings.keys():
          if other_unknown_query_strings['site'] in pairCount['site'].values:
            selectedTab = other_unknown_query_strings['site']


        return html.Div([
            dcc.Store(id='local-store', data=alarm),
            dbc.Row([
                dbc.Col([
                  html.H3(f"ASN {alarm['asn']}", className="text-center bold"),
                  # html.Hr(className="my-2"),
                  html.H3(alarm['owner'], className="text-center")
                ], width=2),
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
                                dbc.Col(html.P(f"The change affected the following sites: ", className='subtitle'), width='auto', align="center"),
                                dbc.Col(html.B(affected, className='subtitle'), align="center"),
                              ], justify="start")
                            ], className="pair-details")
                        ],
                    ), width=10
            ),
            ], justify="between", align="center", className="asn-header"),
            dbc.Row(
                dcc.Tabs(id="tabs-example-graph", value=selectedTab, parent_className='custom-tabs', className='custom-tabs-container col-2',
                    children=[
                      dcc.Tab(label=site, value=site,
                      id=site,
                      className='custom-tab text-center ', selected_className='custom-tab--selected rounded-border-1 mr-2',
                      children=buildSiteBox(site,
                                            pairCount[pairCount['site']==site]['pair'].values[0], 
                                            chdf[(chdf['src_site']==site) | (chdf['dest_site']==site)],
                                            posDf[(posDf['src_site']==site) | (posDf['dest_site']==site)],
                                            baseline[(baseline['src_site']==site) | (baseline['dest_site']==site)],
                                            altPaths[(altPaths['src_site']==site) | (altPaths['dest_site']==site)])
                                            ) for site in alarm['sites']
                      ], vertical=True),
            )
          ])



@timer
def buildSiteBox(site, cnt, chdf, posDf, baseline, altPaths):
    if site:
      print(site)
      baseline['spair'] = baseline['src_site']+' -> '+baseline['dest_site']

      bline = baseline[(baseline['src_site']==site)].sort_values(['src_site'])
      allPairs = pd.concat([bline, baseline[(baseline['dest_site']==site)].sort_values(['dest_site'])])[['spair', 'pair']]

      if len(allPairs)>10:
        allPairs = allPairs.sample(10)

      sitePairs = allPairs['spair'].values
      nodePairs = allPairs['pair'].values
      diffs = sorted(list(set([str(el) for v in chdf['diff'].values.tolist() for el in v])))
      diffs_str = '  |  '.join(diffs)

      print(site, len(sitePairs))

      return html.Div([
              dbc.Row([
                dbc.Row([
                  dbc.Col([
                    dbc.Row([
                      dbc.Col(
                        html.P(f"{cnt} traceroute alarms related to that site"), align='left', width=12, className='mt-1 site-details'),
                      dbc.Col(
                        html.P(f"Flagged AS numbers:  {diffs_str}"), align='left', width=12, className='site-details'),
                      ], className='mb-1 site-header-line rounded-border-1 p-2'),
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
                          )
                          , type='dot', style={'height':'0.5rem'}, color='#e9e9e9'),
                        ]
                      ) for i, pair in enumerate(nodePairs)
                    ]
                    ) ,
                  ], width={"size": '12'}),
                  
                ], justify="between", align="center"),
              ], justify="center", align="center")
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
    pq = Parquet()
    location = 'parquet/raw/'

    if n:
      if is_open==False:
        
        chdf, posDf, baseline, altPaths = pq.readFile(f'{location}chdf.parquet'), pq.readFile(f'{location}posDf.parquet'), pq.readFile(f'{location}baseline.parquet'), pq.readFile(f'{location}altPaths.parquet')
        data = pairDetails(pair, alarm,
                        chdf[(chdf['pair']==pair) & (chdf['from_date']==alarm['from']) & (chdf['to_date']==alarm['to'])],
                        baseline[(baseline['pair']==pair) & (baseline['from_date']==alarm['from']) & (baseline['to_date']==alarm['to'])],
                        altPaths[(altPaths['pair']==pair) & (altPaths['from_date']==alarm['from']) & (altPaths['to_date']==alarm['to'])],
                        posDf[(posDf['pair']==pair) & (posDf['from_date']==alarm['from']) & (posDf['to_date']==alarm['to'])])
      return [not is_open, data]
    return [is_open, data]

def getColor(asn, diffs):
  if asn in diffs:
    # print('red!')
    return '#ff5f5f'
  # print('grey!')
  return 'rgb(0 35 90)'

@timer
def pairDetails(pair, alarm, chdf, baseline, altpaths, hopPositions):
  
  
  # print('                 3. pairDetails')
  # print(alarm)
  sites = baseline[['src_site','dest_site']].values.tolist()[0]
  howPathChanged = descChange(pair, chdf, hopPositions)
  diffs = chdf[chdf["pair"]==pair]['diff'].to_list()[0]

  otherAlarms = getOtherAlarmsCount(alarm['from'], alarm['to'], sites[0], sites[1])
  # print(otherAlarms, alarm['from'], alarm['to'], sites[0], sites[1])

  if not otherAlarms:
    cntAlarms = 'None found'
  else: 
    cntAlarms = ''
    for res in otherAlarms:
      print(res)
      cntAlarms += (res['event']).capitalize()+': '+str(res['count'])+'  |   '
  
  
  
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
                # align="left", justify="start", className="text-right mt-10"
                # ),
                # dbc.Row([
                  html.H4(f"Other networking alarms: {cntAlarms}", className="text-center"),
                  # html.P(cntAlarms, className="text-left")
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
                        dbc.Badge(f"Taken in {str(round(baseline['hash_freq'].values.tolist()[0]*100))}% of time", text_color="dark", color="#e9e9e9",  className="label w-100")
                        ], width=6),
                      dbc.Col([
                        dbc.Badge(f"Always reaches destination: {['YES' if baseline['all_dests_reached'].values[0] else 'NO'][0]}", text_color="dark", color="#e9e9e9",  className="label w-100")
                        ], width=6),
                    ], justify="center", align="center", className="bg-gray rounded-border-1"),
                    dbc.Row([
                        html.Div(children=[
                          dbc.Badge(asn, text_color="light", color='rgb(0 35 90)', className="me-2") for asn in baseline["asns_updated"].values.tolist()[0]
                        ], className="text-center"),
                      ], className='mb-2'),
                  ], className='baseline-section'),
                  dbc.Row([
                    html.H2(f"ALTERNATIVE PATHS", className="label text-center mb-1"),
                    dbc.Row(children=[
                        html.Div(children=[
                          dbc.Row([
                            dbc.Col([
                              dbc.Badge(f"Taken in {str(round(hash_freq*100,2))}% of time", text_color="dark", color="rgb(255, 255, 255)",  className="w-100")
                              ]),
                            dbc.Col([
                              dbc.Badge(f"Always reaches destination: {['YES' if all_dests_reached else 'NO'][0]}", text_color="dark", color="rgb(255, 255, 255))",  className="w-100")
                              ]),
                          ], className="bg-white"),
                          
                          html.Div(children=[
                            dbc.Badge(asn, text_color="light", color=getColor(asn, diffs), className="me-2") for asn in path])
                        ], className="text-center mb-2") for path, hash_freq, all_dests_reached in altpaths[["asns_updated","hash_freq","all_dests_reached"]].values.tolist()
                    ])
                    ], justify="center", align="center")
                ], className="bordered-box mt-1")
              ], className="mlr-1"),

              dbc.Col([
                dbc.Row(
                  dcc.Graph(id="graph-hops", figure=singlePlotPositions(hopPositions), className="p-1 bordered-box")
                , align="start",),
                dbc.Row([
                  html.Div(children=[

                      dbc.Row([
                        dbc.Col(html.P('At position'), className=' text-right'),
                        dbc.Col(html.P('Hopped from'), className='text-center'),
                        dbc.Col(html.P('Hopped to'), className=' text-center')
                      ]),
                      dbc.Row([
                        dbc.Col(html.B(str(round(item['atPos']))), className='text-right'),
                        dbc.Col(html.B(item['jumped2']), className=' text-center'),
                        dbc.Col(html.B(item['diff']), className=' text-center')
                      ]),
                      dbc.Row([
                        dbc.Col(html.P(''), className=''),
                        dbc.Col(html.P(item['jumpOwner']), className=' text-center'),
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
            text=dd['asn'].values,
            texttemplate="%{text}", colorscale='deep'
        )
    )

    fig.update_annotations(font_size=12)
    fig.update_layout(template='plotly_white', title='AS numbers for every hop and the frequency of their occurences at each position',
                      yaxis={'visible': False, 'showticklabels': False},
                      xaxis={'title': 'Position on the path'},
                      title_font_size=12
                     )

    return go.Figure(data=fig)




@timer
def descChange(pair, chdf, posDf):
  print(pair, getASNInfo(chdf[(chdf['pair']==pair)]['diff'].values.tolist()[0]))
  owners = getASNInfo(posDf[(posDf['pair']==pair)]['asn'].values.tolist())
  owners['-1'] = 'OFF/Unavailable'
  howPathChanged = []
  for diff in chdf[(chdf['pair']==pair)]['diff'].values.tolist()[0]:
      for pos, P in posDf[(posDf['pair']==pair)&(posDf['asn']==diff)][['pos','P']].values:
          atPos = posDf[(posDf['pair']==pair) & (posDf['pos']==pos)]['asn'].values.tolist()

          if len(atPos)>0 and P<1:
            atPos.remove(diff)
            # print(getASNInfo(atPos))
            for newASN in atPos:
              if newASN not in [0, -1]:
                howPathChanged.append({'diff': diff,
                                      'diffOwner': owners[str(diff)],'atPos': pos, 
                                      'jumped2': newASN, 'jumpOwner': owners[str(newASN)]})

  if len(howPathChanged)>0:
    return pd.DataFrame(howPathChanged).sort_values('atPos')


