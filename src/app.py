import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import dash_html_components as html
import plotly.express as px
import pandas as pd

import view.host_map as host_map
import view.site_report as site_report

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
                dcc.Tabs([
                    dcc.Tab(label='Sites', children=[
                        html.Div([
                            dcc.Location(id='url', refresh=False),
                            dcc.Store(id='memory-output'),
                            dcc.Interval(
                                    id='interval-component',
                                    interval=1*450, # in milliseconds
                                    n_intervals=0,
                                    max_intervals=len(site_report.sites),
                                ),
                            html.Div(id='cards')
                        ], className='tab-element')
                    ]),
                    dcc.Tab(label='Hosts', children=[
                                                html.Div(
                                                    host_map.layout_all, className='tab-element'
                                                    )
                                                ]
                           )
                ])
            ])



elem_list = []
@app.callback(Output('cards', 'children'),
              [Input('interval-component', 'n_intervals')])
def siteTables(interval):
    global elem_list
    if (interval == 0):
        elem_list = []

    if (interval%3 == 0):
        elem_list.append(dbc.Row([dbc.Col(site_report.createCard(val))
                                  for val in site_report.sites[interval:interval+3]],
                                 id=f"card-{interval}", className='site-card'))

    return elem_list

app.run_server(debug=False, port=8050, host='0.0.0.0')