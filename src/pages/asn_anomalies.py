import dash
from dash import html
from dash import Dash, html, dcc, Input, Output, Patch, callback, State, ctx, dash_table, dcc, html

import urllib3

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

import model.queries as qrs
from utils.utils import generate_plotly_heatmap_with_anomalies


urllib3.disable_warnings()



def title(q=None):
    return f"ASN-path anomalies"



def description(q=None):
    return f"Visual represention of an ASN-path anomaly"



dash.register_page(
    __name__,
    path_template="/anomalous_paths/<q>",
    title=title,
    description=description,
)


def layout(q=None, **other_unknown_query_strings):
    return html.Div([
        dcc.Location(id='url', refresh=False),
        dcc.Store(id='alarm-data-store'),
        html.Div(id='asn-anomalies-content'),
        html.Div([
          html.Div([
            html.H1(f"The most recent ASN paths"),
            html.P('The plot shows new ASNs framed in white. The data is based on the alarms of type "ASN path anomalies"', style={"font-size": "1.2rem"})
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
        # Extract the parameters from the URL path
        path_parts = pathname.split('/')
        if len(path_parts) > 2 and '=' in path_parts[2]:
            params = path_parts[2].split('&')
            query_params = {param.split('=')[0]: param.split('=')[1] for param in params}
            return query_params
    return {}


@callback(
    Output('asn-anomalies-graphs', 'children'),
    Input('alarm-data-store', 'data')
)
def update_graphs(query_params):
    if query_params:
        src = query_params.get('src_netsite')
        dest = query_params.get('dest_netsite')
        print(src, dest)
        if src and dest:
            data = qrs.query_ASN_anomalies(src, dest)

            if len(data) > 0:
                if len(data['ipv6'].unique()) == 2:
                    ipv6_figure = generate_plotly_heatmap_with_anomalies(data[data['ipv6'] == True])
                    ipv4_figure = generate_plotly_heatmap_with_anomalies(data[data['ipv6'] == False])
                    figures = [
                        dcc.Graph(figure=ipv4_figure, id="asn-sankey-ipv4"),
                        dcc.Graph(figure=ipv6_figure, id="asn-sankey-ipv6")
                    ]
                else:
                    figure = generate_plotly_heatmap_with_anomalies(data)
                    figures = [dcc.Graph(figure=figure, id="asn-sankey-ipv4")]

                return html.Div(figures)
            else:
                return html.Div([
                    html.H1(f"No data found for alarm {src} to {dest}"),
                    html.P('No data was found for the alarm selected. Please try another alarm.', style={"font-size": "1.2rem"})
                ], className="l-h-3 p-2 boxwithshadow page-cont ml-1 p-1")
    return html.Div()


