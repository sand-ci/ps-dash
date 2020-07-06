import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import dash_html_components as html
import plotly.express as px
import pandas as pd

import host_map
import site_report as sr

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
                                    interval=1*250, # in milliseconds
                                    n_intervals=0,
                                    max_intervals=len(sr.sites),
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
        elem_list.append(dbc.Row([dbc.Col(sr.createCard(val))
                                  for val in sr.sites[(interval):interval+3]],
                                 id=f"card-{interval}", className='site-card'))

    return elem_list

if __name__ == '__main__':
    app.run_server(debug=False)