import dash

from dash import html
from dash import Dash, html, dcc, Input, Output, dcc, html, callback
import dash_bootstrap_components as dbc
import urllib3

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import dash_bootstrap_components as dbc

import model.queries as qrs
from utils.utils import generate_graphs

urllib3.disable_warnings()



dash.register_page(
    __name__,
    path_template="/anomalous_paths/<q>",
    title="ASN-path anomalies",
    description="Visual representation of an ASN-path anomaly",
)

def layout(q=None, **other_unknown_query_strings):
    return html.Div([
        dcc.Location(id='url', refresh=False),
        dcc.Store(id='alarm-data-store'),
        html.Div(id='asn-anomalies-content'),
        html.Div([
            html.Div([
                html.H1(id="page-title"),
            ], className="l-h-3 p-2"),
            dcc.Loading(id='loading-spinner', type='default', children=[
                html.Div(id='asn-anomalies-graphs')
            ], color='#00245A'),
        ], className="l-h-3 p-2 boxwithshadow page-cont ml-1 p-1")
    ], style={"padding": "0.5% 1.5%"})


@callback(
    Output('alarm-data-store', 'data'),
    Input('url', 'pathname')
)
def update_store(pathname):
    if pathname:
        path_parts = pathname.split('/')
        if len(path_parts) > 2 and '=' in path_parts[2]:
            params = path_parts[2].split('&')
            query_params = {param.split('=')[0]: param.split('=')[1] for param in params}
            return query_params
    return {}


@callback(
    [Output('asn-anomalies-graphs', 'children'),
     Output('page-title', 'children'),
     ],
    Input('alarm-data-store', 'data')
)
def update_graphs_and_title(query_params):
    if not query_params:
        return html.Div(), "ASN-path anomalies"

    src = query_params.get('src_netsite')
    dest = query_params.get('dest_netsite')
    dt = query_params.get('dt')
    if not (src and dest):
        return html.Div(), "ASN-path anomalies"
    data = qrs.query_ASN_anomalies(src, dest, dt)
    if len(data) == 0:
        return html.Div([
            html.H1(f"No data found for alarm {src} to {dest}"),
            html.P('No data was found for the alarm selected. Please try another alarm.',
                   className="plot-subtitle")
        ], className="l-h-3 p-2 boxwithshadow page-cont ml-1 p-1"), "ASN-path anomalies"
    anomalies = data['anomalies'].values[0]
    title = html.Div([
        html.Span(f"{src} → {dest}", className="sites-anomalies-title"),
        html.Span(" ||| ", style={"margin": "0 10px", "color": "#6c757d", "fontSize": "20px"}),
        html.Span(f"Anomalies: {anomalies}", className="anomalies-title"),
    ], style={"textAlign": "center", "marginBottom": "20px"})

    figures = generate_graphs(data, src, dest, dt)

    return figures, title