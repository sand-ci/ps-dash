print('starting')
import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate
import dash_html_components as html
import pandas as pd
import urllib.parse as urlparse
from urllib.parse import parse_qs
from flask_caching import Cache
import os
print('finish 1st section')

from model.DataLoader import Updater
print('updater')
from model.DataLoader import GeneralDataLoader
print('generaldataloader')
from view.site_report import SiteReport
from view.sites_page import SitesPage
from view.problematic_pairs import ProblematicPairsPage
from view.pair_plots import PairPlotsPage
print('Finish Normal views')
# from model.parquet_creation import main
print('Finish parquet')
import utils.helpers as hp
print('finish 2nd section')

import threading
import time
import datetime
import pandas as pd
from functools import reduce, wraps
from datetime import datetime, timedelta
import numpy as np
from  scipy.stats import zscore
print('finish 3rd section')

import model.queries as qrs
from model.NodesMetaData import NodesMetaData
import utils.helpers as hp
from utils.helpers import timer
print('finish 4th section')

import asyncio
import parquet_creation as pcr

from model.DataLoader import ParquetUpdater
ParquetUpdater()


# Start a thread which will update the data every hour
# Updater()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP]


app = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/brPBPO.css"})

# CACHE_CONFIG = {
#     'CACHE_TYPE': 'redis',
#     'CACHE_REDIS_URL': 'redis://redis-master.perfsonar-platform.svc.cluster.local'
# }
# cache = Cache()
# cache.init_app(app.server, config=CACHE_CONFIG)

# On start of application create the neccessary objects
%run parquet_creation.py
print('limt from the pcr file',pcr.limit)

gdl = GeneralDataLoader()
spage = SitesPage()
sreport = SiteReport()
ppage = ProblematicPairsPage()
pplotpage = PairPlotsPage()

def serve_layout():
    # on reload get the latest data
    global gdl, spage, sreport, ppage, pplotpage
    gdl = GeneralDataLoader()
    spage = SitesPage()
    sreport = SiteReport()
    ppage = ProblematicPairsPage()
    pplotpage = PairPlotsPage()

    
    return html.Div([
                dcc.Location(id='change-url', refresh=False),
                dcc.Store(id='store-dropdown'),
                dbc.Row([
                     dbc.Col(dbc.Button(
                        "perfSONAR Toolkit Information",
                        className="external-button",
                        href='https://toolkitinfo.opensciencegrid.org/toolkitinfo/'
                    )),
                    dbc.Col(dbc.Button(
                        "Kibana: Packet Loss in OSG/WLCG",
                        className="external-button",
                        href='https://atlas-kibana.mwt2.org/s/networking/app/kibana#/dashboard/07a03a80-beda-11e9-96c8-d543436ab024?_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A(from%3Anow-3d%2Cto%3Anow))'
                    )),
                    dbc.Col(dbc.Button(
                        "Kibana: Packet Loss Tracking",
                        className="external-button",
                        href='https://atlas-kibana.mwt2.org/s/networking/app/dashboards#/view/ab7c4950-5cfa-11ea-bad0-ff3d06e7229e?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-3d,to:now))'
                    )),
                    dbc.Col(dbc.Button(
                        "MEPHi Tracer: Traceroute explorer",
                        className="external-button",
                        href='https://perfsonar.uc.ssl-hep.org'
                    ))
                ], className="external-links", justify='center', align="center", no_gutters=True),
                dbc.Nav(
                    [
                        dbc.NavItem(dbc.NavLink(
                            html.Img(src=app.get_asset_url('ps-dash.png'), height="35px"
                                    ), disabled=True, href="/sites", className="logo")),
                        dbc.NavItem(dbc.NavLink("SITES", href="/sites", id='sites-tab')),
                        dbc.NavItem(dbc.NavLink("LINKS", href="/nodes", id='nodes-tab')),
                        dbc.NavItem(dbc.NavLink("PLOTS", href="/pairs", id='pairs-tab')),
                    ], fill=True, justified=True, id='navbar'
                ),
                dcc.Loading(html.Div(id='page-content'), className='loader-cont', color='#00245A'),

                html.Div(id='page-content-noloading'),
            ], className='main-cont')


app.layout = serve_layout


layout_nodes = html.Div(
                    ppage.createLayout(), className='tab-element'
                    )

layout_sites = html.Div(
                 spage.buildLayout()
                , className='tab-element', id='main-tabs')

layout_notfound = dbc.Jumbotron(
                    [
                        dbc.Row([
                            dbc.Col([
                                html.H1("404: Not found", className="text-danger"),
                                html.Hr(),
                                html.P("Try another path...")
                            ], width=8)
                        ], justify='center')
                    ]
                )

'''Takes selected site from the Geo map and displays the relevant information'''
@app.callback([Output('site-details', 'children'),
               Output('site-plots-out', 'figure'),
               Output('site-plots-in', 'figure')],
              [Input('site-map', 'clickData')])
def update_site_elements(value):
    if value is not None:
        site = value['points'][0]['hovertext']
    else: site = sreport.sites[0]
    return (sreport.createCard(site), spage.SitesOverviewPlots(site, 'src'), spage.SitesOverviewPlots(site, 'dest'))



'''Get the relevant dataframe based on the type of problem. Page loading is much faster this way'''
@app.callback([Output('tabs-content', 'children'),
               Output('last-updated', 'children')],
              [Input('tabs-indeces', 'value'),
               Input('tabs-prob-types', 'value')])
def renderProblems(idx, problem):
    if (problem == 'threshold_reached'):
        df = ppage.threshold_reached
    elif (problem == 'has_bursts'):
        df = ppage.has_bursts
    elif (problem == 'all_packets_lost'):
        df = ppage.all_packets_lost

    return [ppage.showProblems(idx, df), dbc.Col([
                                            html.Div(f'{ppage.obj.dateFrom} - {ppage.obj.dateTo}', className='period-times'),
                                            html.Div('(Queried period in UTC time)', className='period-times')], className='period-element'
                                         )]


'''Store the data from the clicked row in another element'''
@app.callback(
        [Output({'type': 'input-src','index': MATCH}, 'value'),
         Output({'type': 'input-dest','index': MATCH}, 'value'),
         Output({'type': 'memory-output','index': MATCH}, 'value'),],
        [Input({'type': 'problem-table','index': MATCH}, 'active_cell'),
         Input({'type': 'problem-table','index': MATCH}, 'data'),
         Input({'type': 'problem-table','index': MATCH}, 'page_current'),
         Input({'type': 'problem-table','index': MATCH}, 'page_size'),
         Input({'type': 'problem-table','index': MATCH}, 'id')])
def fillInput(active_cell, data, page, page_size, elem_id):
    if active_cell is not None:
        df = pd.DataFrame(data)
#         active_cell is printing {'row': 0, 'column': 0, 'column_id': 'src_host'}
        loc = page*page_size+active_cell['row'] if page is not None else active_cell['row']
        active_row_id = df.loc[loc].to_dict()
        active_row_id['host_src'] = active_row_id['host_src'] if active_row_id['host_src'] != 'N/A' else active_row_id['src']
        active_row_id['host_dest'] = active_row_id['host_dest'] if active_row_id['host_dest'] != 'N/A' else active_row_id['dest']
        active_row_id['idx'] = elem_id['index']
        return [active_row_id['host_src'], active_row_id['host_dest'], active_row_id]
    raise PreventUpdate


'''Simulate opening of a new page to show the relevant plots'''
@app.callback([Output('change-url', 'href')],
              [Input({'type': 'plot', 'index': ALL}, 'n_clicks'),
               Input({'type': 'memory-output','index': ALL}, 'value'),
               Input("store-dropdown", "data")
              ])
def changePath(clicked, stored, dddata):
    idx, data = None, None

    # If there is only 1 value for the plot button, that means a user is on page Pairs
    # and the data comes from the dropdown fields or from the vertical tab is selected
    # a value != "ALL"
    if (len(clicked) == 1):
        if (clicked[0] == 1) and (dddata is None):
            data = stored[0]
        elif (clicked[0] == 1) and (dddata is not None) and not any(v is None for v in dddata.values()):
            data = dddata
    else:
        # If there is > 1 value for the plot button, that means a user is on page Nodes
        # and the data comes from the datatables
        # get the index of the button click and use it to get the data from the corresponding table
        for i in range(len(clicked)):
            if clicked[i] > 0:
                idx = hp.INDECES[i]
                data = stored[i]

    if data is not None:
        return [f'/plot?idx={data["idx"]}&src_host={data["host_src"]}&src={data["src"]}&dest_host={data["host_dest"]}&dest={data["dest"]}']
    raise PreventUpdate


@app.callback([Output("src-dropdown", "options"),
               Output("dest-dropdown", "options"),
               Output("store-dropdown", "data"),
               Output("total-pairs", "children"),
               Output("total-srcs", "children"),
               Output("total-dests", "children")],
              [Input("idx-dropdown", "value"),
               Input("src-dropdown", "value"),
               Input("dest-dropdown", "value")],
               State("store-dropdown", "data"))
def fill_dropdowns(idx, src, dest, stored):
    idxChanged = False
    if stored is not None:
        if idx != stored['idx']:
            idxChanged = True
    srcPresent = ''
    ip_src, host_src, ip_dest, host_dest = None, None, None, None

    if idx is not None:
        # Get all values for the selected index
        if (dest is None and src is None) or (idxChanged):
            slist = gdl.all_tested_pairs[gdl.all_tested_pairs['idx'] == idx]['source'].unique()
            dlist = gdl.all_tested_pairs[gdl.all_tested_pairs['idx'] == idx]['destination'].unique()
        # This is the case when the values come from the URL
        elif (src is not None) and (dest is not None):
            s = src.split(': ')
            d = dest.split(': ')
            ip_src = s[1]
            ip_dest = d[1]
            host_src = s[0]
            host_dest = d[0]
            slist = gdl.all_tested_pairs[gdl.all_tested_pairs['idx'] == idx]['source'].unique()
            dlist = gdl.all_tested_pairs[(gdl.all_tested_pairs['source'] == src) & (gdl.all_tested_pairs['idx'] == idx)]['destination'].unique()
        # Get all destinations for the selected source and index
        elif (src is not None) or (src != stored['src']):
            srcPresent = f' for selected source'
            ip_src = gdl.all_tested_pairs[(gdl.all_tested_pairs['source'] == src)]['src'].values[0]
            ip_dest = gdl.all_tested_pairs[(gdl.all_tested_pairs['destination'] == dest)]['dest'].values[0] if dest is not None else None
            host_src = gdl.all_tested_pairs[(gdl.all_tested_pairs['source'] == src)]['host_src'].values[0]
            host_dest = gdl.all_tested_pairs[(gdl.all_tested_pairs['destination'] == dest)]['host_dest'].values[0] if dest is not None else None
            slist = gdl.all_tested_pairs[gdl.all_tested_pairs['idx'] == idx]['source'].unique()
            dlist = gdl.all_tested_pairs[(gdl.all_tested_pairs['source'] == src) & (gdl.all_tested_pairs['idx'] == idx)]['destination'].unique()

        data = {'idx': idx, 'source': src, 'destination': dest, 'src': ip_src, 'dest': ip_dest, 'host_src': host_src, 'host_dest': host_dest}

        return [[{'label':v, 'value':v} for v in slist],
                [{'label':v, 'value':v} for v in dlist],
                data,
                f"Total number of pairs for selected index: {len(gdl.all_tested_pairs[gdl.all_tested_pairs['idx'] == idx])}",
                f"Number of sources for selected index: {len(slist)}",
                f"Number of destinations{srcPresent}: {len(dlist)}"]

    raise PreventUpdate

'''Show different layouts depending on the URL'''
@app.callback([Output("idx-dropdown", "value"),
               Output("src-dropdown", "value"),
               Output("dest-dropdown", "value")],
              [Input('change-url', 'href')])
def update_dropdowns_from_url(url_data):
    parsed = urlparse.urlparse(url_data)
    data = parse_qs(parsed.query)

    if bool(data):
        return [data['idx'][0], f"{data['src_host'][0]}: {data['src'][0]}", f"{data['dest_host'][0]}: {data['dest'][0]}"]
    raise PreventUpdate


'''Show different layouts depending on the URL'''
@app.callback([Output('page-content-noloading', 'children'),
               Output('page-content', 'children'),
               Output('sites-tab', 'active'),
               Output('nodes-tab', 'active'),
               Output('pairs-tab', 'active')],
              [Input('change-url', 'pathname'),
               Input('change-url', 'href')])
def displayPage(pathname, url):
    if pathname == '/' or pathname == '/sites':
        return [layout_sites, None, True, False, False]
    elif url.endswith('/nodes'):
        return [layout_nodes, None, False, True, False]
    elif url.endswith('/pairs') :
        return [None, pplotpage.defaultLayout(), False, False, True]
    elif url.startswith('/plot') or pathname.startswith('/plot'):
        return [dcc.Loading([pplotpage.defaultLayout(), pplotpage.specificPairLayout(url)],fullscreen=True), None, False, False, True]
    else: return [None, layout_notfound, False, False, False]

# @app.callback(Output('tabs-content', 'style'),
#               [Input('tabs-indeces', 'value'),
#                Input('tabs-prob-types', 'value')])
# def triggerspinner(value):
#     return {'display':'none'}


app.run_server(debug=False, port=8050, host='localhost')
print("Done")