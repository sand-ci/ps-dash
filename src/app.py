import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate
import dash_html_components as html
import plotly.express as px
import pandas as pd

from model.DataLoader import Updater
# import view.host_map as host_map
import view.site_report as site_report
from view.problematic_pairs import ProblematicPairsPage
from view.pair_plots import PairPlotsPage
import utils.helpers as hp

# Start a thread which will update the data every hour
Updater()
ppage = ProblematicPairsPage()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)

app.layout = html.Div([
                dcc.Location(id='change-url', refresh=False),
                html.Div(id='page-content'),
                dcc.Loading(id="loader", type="default", fullscreen=True,
                    children=[
                        html.Div(id='page-content1')
                    ]
                ),
            ])


layout_tabs =  dcc.Tabs([
                    dcc.Tab(label='Sites', id='sites-tab', children=[
                        html.Div([
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


'''Build the site summaries as a set of smaller elements wrapped up in a bigger one'''
@app.callback([Output('cards', 'children')],
              [Input('interval-component', 'n_intervals')],
              [State('cards', 'children')])
def showsSiteTables(interval, current_elements):
    elem_list = []
    if (interval%3 == 0):
        elem_list.append(dbc.Row([dbc.Col(site_report.createCard(val))
                                  for val in site_report.sites[interval:interval+3]],
                                 id=f"card-{interval}", className='site-card'))
    if current_elements is not None:
        return [current_elements + elem_list]
    return [elem_list]


'''Get the relevant dataframe based on the type of problem. Page loading is much faster this way'''
@app.callback([Output('tabs-content', 'children'),
               Output('last-updated', 'children')],
              [Input('tabs-indeces', 'value'),
               Input('tabs-prob-types', 'value')])
def renderProblems(idx, problem):
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
@app.callback([Output('change-url', 'pathname')],
              [Input({'type': 'plot', 'index': ALL}, 'n_clicks'),
               Input({'type': 'memory-output','index': ALL}, 'value')])
def changePath(clicked, stored):
    idx = None
    # get the index of the button click and use it to get the data from the corresponding table
    for i in range(len(clicked)):
        if clicked[i] > 0:
            idx = hp.INDECES[i]
            data = stored[i]
    if idx is not None:
        return [f'/plot?idx={data["idx"]}&src_host={data["host_src"]}&src={data["src"]}&dest_host={data["host_dest"]}&dest={data["dest"]}']
    else: raise PreventUpdate



# TODO: remove the long sites page and replace it woth a gep map. The the container without the loader can be removed as well
'''Show different layouts depending on the URL'''
@app.callback([Output('page-content', 'children'), Output('page-content1', 'children'),],[Input('change-url', 'pathname'), Input('change-url', 'href')])
def displayPage(pathname, url):
    if pathname == '/':
        return [layout_tabs, None]
    elif pathname.startswith('/plot'):
        path = pathname if len(pathname) > 5 else url
        o = PairPlotsPage(path)
        return [None, o.createLayout()]


app.run_server(debug=False, port=8050, host='0.0.0.0')