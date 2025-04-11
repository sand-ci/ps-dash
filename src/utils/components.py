
from dash import dcc, html
import dash_bootstrap_components as dbc
from flask import request
from utils.utils import extractRelatedOnly, SitesOverviewPlots
from model.Alarms import Alarms
###############################################
            #path_changed.py
###############################################
def siteBoxPathChanged(site, pairCount, chdf, posDf, baseline, altPaths, alarm, pivotFrames, alarmsInst=Alarms()):
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
