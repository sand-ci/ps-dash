import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import dash_html_components as html
import plotly.express as px
import pandas as pd

from model.DataLoader import Updater
# import view.host_map as host_map
import view.site_report as site_report
from view.problematic_pairs import ProblematicPairsPage


# Start a thread which will update the data every hour
Updater()
ppage = ProblematicPairsPage()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
                dcc.Tabs([
                    dcc.Tab(label='Sites', id='sites-tab', children=[
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
                        ], className='tab-element', id='main-tabs')
                    ]),
                    dcc.Tab(label='Nodes', id='hosts-tab', children=[
                            html.Div(
                                ppage.createLayout(), className='tab-element'
                                )
#                             html.Div(
#                                 host_map.layout_all, className='tab-element'
#                                 )
                            ]
                    )
                ])
            ])



@app.callback(Output('cards', 'children'),
              [Input('interval-component', 'n_intervals')],
              [State('cards', 'children')])
def siteTables(interval, current_elements):
    elem_list = []

    if (interval%3 == 0):
        elem_list.append(dbc.Row([dbc.Col(site_report.createCard(val))
                                  for val in site_report.sites[interval:interval+3]],
                                 id=f"card-{interval}", className='site-card'))
    if current_elements is not None:
        return current_elements + elem_list
    return elem_list

@app.callback([Output('tabs-content', 'children'),
               Output('last-updated', 'children')],
              [Input('tabs-indeces', 'value'),
               Input('tabs-prob-types', 'value'),
               Input('update-interval-component', 'n_intervals')])
def render_problems(idx, problem, intv):
    print('*** Reload content ***')
    ppage = ProblematicPairsPage()
    if (problem == 'high_sigma'):
        df = ppage.high_sigma
    elif (problem == 'has_bursts'):
        df = ppage.has_bursts
    elif (problem == 'all_packets_lost'):
        df = ppage.all_packets_lost

    return [ppage.showProblems(idx, df), dbc.Col([
                                            html.Div(f'{ppage.obj.dateFrom} - {ppage.obj.dateTo}', className='period-times'),
                                            html.Div('(Queried period in UTC time)', className='period-times')], className='period-element'
                                         )]



app.run_server(debug=False, port=8050, host='0.0.0.0')