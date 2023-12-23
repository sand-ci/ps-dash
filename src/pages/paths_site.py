import dash
from dash import Dash, dcc, html, Input, Output, State, Patch, MATCH
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from elasticsearch.helpers import scan
import pandas as pd

from utils.helpers import timer
import utils.helpers as hp
from model.Alarms import Alarms
import model.queries as qrs

import urllib3
urllib3.disable_warnings()



def title(q=None):
    return f"Site: {q}"



def description(q=None):
    return f"List traceroute 'path changed' alarms for site {q}"



dash.register_page(
    __name__,
    path_template="/paths-site/<q>",
    title=title,
    description=description,
)



@timer
def getStats(dateFrom, dateTo, site):
    q = {
        "query": {
            "bool": {
            "must": [
                {
                "range": {
                    "to_date": {
                    "gte": dateFrom,
                    "lte": dateTo,
                    "format": "strict_date_optional_time"
                    }
                }
                },
                {
                    "bool": {
                        "should": [
                        {
                            "term": {
                            "src_site": site
                            }
                        },
                        {
                            "term": {
                            "dest_site": site
                            }
                        }
                        ]
                    }
                    }
            ]
            }
        }
        }

    # print(str(q).replace("\'", "\""))
    result = scan(client=hp.es,index='ps_traces_changes',query=q)
    data, positions, baseline, altPaths = [],[],[],[]
    positions = []
    for item in result:

        tempD = {}
        for k,v in item['_source'].items():
            if k not in ['positions', 'baseline', 'alt_paths', 'created_at']:
                tempD[k] = v
        data.append(tempD)

        src,dest,src_site,dest_site, = item['_source']['src'], item['_source']['dest'], item['_source']['src_site'], item['_source']['dest_site']
        from_date,to_date = item['_source']['from_date'], item['_source']['to_date']

        temp = item['_source']['positions']
        for p in temp:
            p['src'] = src
            p['dest'] = dest
            p['src_site'] = src_site
            p['dest_site'] = dest_site
            p['from_date'] = from_date
            p['to_date'] = to_date
        positions.extend(temp)
        
        temp = item['_source']['baseline']
        for p in temp:
            p['src'] = src
            p['dest'] = dest
            p['src_site'] = src_site
            p['dest_site'] = dest_site
            p['from_date'] = from_date
            p['to_date'] = to_date
        baseline.extend(temp)

        temp = item['_source']['alt_paths']
        for p in temp:
            p['src'] = src
            p['dest'] = dest
            p['src_site'] = src_site
            p['dest_site'] = dest_site
            p['from_date'] = from_date
            p['to_date'] = to_date
        altPaths.extend(temp)

    df = pd.DataFrame(data)
    posDf = pd.DataFrame(positions)
    baseline = pd.DataFrame(baseline)
    altPaths = pd.DataFrame(altPaths)
    
    if len(df)>0:
      posDf['pair'] = posDf['src']+' -> '+posDf['dest']
      df['pair'] = df['src']+' -> '+df['dest']
      baseline['pair'] = baseline['src']+' -> '+baseline['dest']
      altPaths['pair'] = altPaths['src']+' -> '+altPaths['dest']

    return df, posDf, baseline, altPaths




@timer
def layout(q=None, **other_unknown_query_strings):

    print(q, other_unknown_query_strings)
    if q:
      print('URL query:', q)
      print()
    
      site  = q
      dateFrom = other_unknown_query_strings['dateFrom']
      dateTo = other_unknown_query_strings['dateTo']
      
      alarmsInst = Alarms()
      chdf, posDf, baseline, altPaths = getStats(dateFrom, dateTo, site)
      dateFrom, dateTo = hp.getPriorNhPeriod(dateTo)
      pivotFrames = alarmsInst.loadData(dateFrom, dateTo)[1]

      posDf['asn'] = posDf['asn'].astype(int)

      if len(chdf) > 0:
        dsts = chdf.groupby('dest_site')[['pair']].count().reset_index().rename(columns={'dest_site':'site'})
        srcs = chdf.groupby('src_site')[['pair']].count().reset_index().rename(columns={'src_site':'site'})
        pairCount = pd.concat([dsts, srcs]).groupby(['site']).sum().reset_index().sort_values('pair', ascending=False)

        baseline['spair'] = baseline['src_site']+' -> '+baseline['dest_site']

        bline = baseline[(baseline['src_site']==site)].sort_values(['src_site'])
        allPairs = pd.concat([bline, baseline[(baseline['dest_site']==site)].sort_values(['dest_site'])])[['spair', 'pair']]

        cnt = pairCount[pairCount['site']==site]['pair'].values[0]
        
        diffs = sorted(list(set([str(el) for v in chdf['diff'].values.tolist() for el in v])))
        diffs_str = '  |  '.join(diffs)

        data = alarmsInst.getOtherAlarms(
            currEvent='path changed', alarmEnd=dateTo, pivotFrames=pivotFrames, site=site)
        otherAlarms = alarmsInst.formatOtherAlarms(data)

        return html.Div([
            dcc.Store(id='site-local-store', data=[{'from': dateFrom, 'to': dateTo}, 
                      chdf.to_dict(), posDf.to_dict(), baseline.to_dict(), altPaths.to_dict(),
                ]),
            dcc.Store(id='site-local-store-pairs', data=[site, allPairs.to_dict()]),

            dbc.Row([
              dbc.Row([
                dbc.Col([
                  html.H3('SITE', className="text-center bold"),
                  html.H3(site.upper(),className="text-center")
                ], lg=2, md=12, className="p-3"),
                dbc.Col(
                    html.Div(
                        [
                          dbc.Row([
                            dbc.Row([
                              html.H1(f"Summary", className="text-left"),
                              html.Hr(className="my-2")]),
                            dbc.Row(
                              html.P(f"{cnt} traceroute alarms involve site {site} in the period between {dateFrom} and {dateTo}"),
                              align='left', className='site-details'
                              ),
                            dbc.Row(
                              html.P(f"Flagged AS numbers:  {diffs_str}", className='subtitle'),
                              align='left', className='mb-1'
                            ),
                            dbc.Row([
                                html.P(f'Site {site} takes part in the following alarms in the period 24h prior and up to 24h after the current alarm end ({dateTo})', className='subtitle'),
                                html.B(otherAlarms, className='subtitle')
                            ], align='left')
                            ], className="pair-details"),
                            
                        ],
                    ), lg=10, md=12, className="p-3"
                  ),
              ], justify="between", align="center", className="boxwithshadow alarm-header pair-details")
            ], style={"padding": "0.5% 1.5%"}, className='g-0 mb-1'),

            dbc.Row([
              dbc.Row([
                dbc.Row([
                  dbc.Col(
                     html.P('Below is a list of site pairs which reported a changed path for the specified period', className='subtitle'),
                     lg=4, md=12, className="mb-1", align="center"
                  ),
                  dbc.Col(
                    html.Button('Get next 10 pairs', id='next-10-btn',
                                n_clicks=0, className="next-10-btn load-pairs-button w-100 mb-1",),
                    lg=4, md=6, 
                  ),
                  dbc.Col(
                      html.Button('Get all', id='all-btn',
                                  n_clicks=0, className="all-btn load-pairs-button w-100 mb-1",),
                  lg=4, md=6),

                ], className="pl-1 mt-1", justify="end"),
          
                html.Div(id="site-pairs-div"),

              ], className="boxwithshadow")
            ], style={"padding": "0.5% 1.5%"}, className='g-0'),
          ])
      else:
        return dbc.Row([
            dbc.Row([
                  html.P(f'Site {site} has no alarms relaated to changed path in the period between {dateFrom} and {dateTo}', className='subtitle')
              ], className="boxwithshadow alarm-header pair-details g-0", justify="between", align="center"),
            ], style={"padding": "0.5% 1.5%"}, className='g-0')


@dash.callback(
    [Output('site-pairs-div', 'children'),
     Output('next-10-btn', 'disabled')],
    [Input('next-10-btn', 'n_clicks'),
     Input('all-btn', 'n_clicks'),
     Input('site-local-store-pairs', 'data')])
def load_site_pairs(next10_clicks, load_all_clicks, data):
    page_limit = 10
    allPairs = pd.DataFrame(data[1])
    site = data[0]
    pages = [allPairs.iloc[i:i+page_limit] for i in range(0, len(allPairs), page_limit)]

    if load_all_clicks == 0 and len(pages)>next10_clicks+1:
      # adds dynamically new set of pairs on click
      patched_children = Patch()
      patched_children.append(html.Div([
          html.Div(id={
              'type': 'output-div-pairs',
              'index': next10_clicks
          }, children=[buildSiteBox(site, pages[next10_clicks])]
          )
      ]))
      return patched_children, False

    else:
       return html.Div([
           html.Div(id={
               'type': 'output-div-pairs',
               'index': next10_clicks
           }, children=[buildSiteBox(site, allPairs)]
           )
       ]), True
       

@timer
def buildSiteBox(site, allPairs):
    if site:
      nodePairs = allPairs['pair'].to_dict()

      return html.Div([
                dbc.Row(
                [
                    html.Div(id=f'site-pair-section{site+str(i)}',
                    children=[
                        dbc.Button(
                            allPairs[allPairs.index==i]['spair'].values[0],
                            value=pair,
                            id={
                                'type': 'site-collapse-button',
                                'index': site+str(i)
                            },
                            className="collapse-button",
                            color="white",
                            n_clicks=0,
                        ),
                        dcc.Loading(
                            dbc.Collapse(
                                id={
                                    'type': 'site-collapse',
                                    'index': site+str(i)
                                },
                                is_open=False, className="collaps-container rounded-border-1"
                        ), style={'height':'0.5rem'}, color='#e9e9e9'),
                    ]
                    ) for i, pair in nodePairs.items()
                ]),
            ], className="p-0")



@timer
@dash.callback(
    [
      Output({'type': 'site-collapse', 'index': MATCH},  "is_open"),
      Output({'type': 'site-collapse', 'index': MATCH},  "children")
    ],
    [
      Input({'type': 'site-collapse-button', 'index': MATCH}, "n_clicks"),
      Input({'type': 'site-collapse-button', 'index': MATCH}, "value"),
      Input('site-local-store', 'data')
    ],
    [State({'type': 'site-collapse', 'index': MATCH},  "is_open")],
)
def toggle_collapse(n, pair, store, is_open):
    data = ''
    period, chdf, posDf, baseline, altPaths, pivotFrames = store[0], pd.DataFrame(store[1]), pd.DataFrame(store[2]), pd.DataFrame(store[3]), pd.DataFrame(store[4]), store[4]

    if n:
      if is_open==False:
        data = pairDetails(pair, period,
                           chdf[(chdf['pair']==pair)],
                           baseline[(baseline['pair']==pair)],
                           altPaths[(altPaths['pair']==pair)],
                           posDf[(posDf['pair']==pair)],
                           pivotFrames)
      return [not is_open, data]
    return [is_open, data]



def getColor(asn, diffs):
  if asn in diffs:
    return '#ff5f5f'
  return 'rgb(0 35 90)'



@timer
def pairDetails(pair, period, chdf, baseline, altpaths, hopPositions, pivotFrames):
  sites = baseline[['src_site','dest_site']].values.tolist()[0]

  howPathChanged = descChange(pair, chdf, hopPositions)
  diffs = chdf[chdf["pair"]==pair]['diff'].to_list()[0]
  alarmsInst = Alarms()
  data = alarmsInst.getOtherAlarms(
      currEvent='path changed', alarmEnd=period['to'], pivotFrames=pivotFrames, src_site=sites[0], dest_site=sites[1])
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
                              dbc.Badge(f"Taken in {str(round(hash_freq*100,2))}% of time", text_color="dark", color="rgb(255, 255, 255)",  className="w-100")
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
                ], className="bordered-box mt-1")
              ], className="mr-1"),

              dbc.Col([
                dbc.Row([
                  dcc.Graph(id="site-graph-hops", figure=singlePlotPositions(hopPositions), className="p-1 bordered-box"),
                  html.P(
                    f"The plot shows the AS numbers for every hop and the frequency of their occurences at each position (source and destination not included).",
                    className="text-center mb-0 fs-5"),
                  html.P(
                    f"The dark blue values of 1 mean the ASN was always used at that position; \
                      Close to 0, means the ASN rarely appeared; \
                      OFF indicates the device did not respond at the time of the traceroute test;  \
                      0 is when there was a responce, but the ASN was unknown.",
                    className="text-center mb-0 fs-5"),
              ], align="start",),
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

  owners = qrs.getASNInfo(posDf[(posDf['pair'] == pair)]['asn'].values.tolist())
  owners['-1'] = 'OFF/Unavailable'
  howPathChanged = []
  for diff in chdf[(chdf['pair'] == pair)]['diff'].values.tolist()[0]:

    for pos, P in posDf[(posDf['pair'] == pair) & (posDf['asn'] == diff)][['pos', 'P']].values:
        atPos = posDf[(posDf['pair'] == pair) & (
            posDf['pos'] == pos)]['asn'].values.tolist()

        if len(atPos) > 0:
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

  if len(howPathChanged) > 0:
    return pd.DataFrame(howPathChanged).sort_values('atPos')
  return pd.DataFrame()


