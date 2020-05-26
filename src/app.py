import plotly.express as px
import dash
import dash_table
import dash_daq as daq
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State

import pandas as pd
from datetime import datetime, timedelta

import templates as tmpl
import DatasetBuilder as build

external_stylesheets = [dbc.themes.BOOTSTRAP]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True, prevent_initial_callbacks=True)

build.StartCron()

def hosts_table(df_tab):
    return dash_table.DataTable(
                id='datatable-row-ids',
                columns=[{"name": i, "id": i} for i in df_tab.columns],
                data=df_tab.to_dict('records'), 
                style_data_conditional= tmpl.host_table_cond,
                style_cell_conditional=tmpl.host_table_cell,
                style_header=tmpl.host_table_header,
                style_cell={'font-family':'sans-serif'},
                sort_action="native",
                sort_mode="multi",
                page_action="native",
                page_current= 0,
                page_size= 20
               )

app.layout = html.Div(
    [
        dbc.Row([
            dbc.Col(html.H4(id='count')),
            dbc.Col( dbc.Row([html.P('Read from preprocessed dataset'),
                    daq.ToggleSwitch(size=40, id='read_from_db', className="read-db-switch"),
                    html.P('Read from ElasticSearch')],justify="center",)),
            dbc.Col(
                dbc.FormGroup(
                    [
                        dbc.RadioItems(
                            options=[
                                {"label": "1H", "value": 1},
                                {"label": "12H", "value": 12},
                                {"label": "1D", "value": 24},
                                {"label": "3D", "value": 72},
                                {"label": "7D", "value": 168},
                            ],
                            value=1,
                            id="radioitems-period",
                            className="period-btns",
                            inline=True,
                        ),
                    ]
                )
                )
        ], no_gutters = True, justify="around", className="header"),
        dbc.Row([
            dbc.Col(dcc.Loading(className='loading-component1', id="loader1", color="#b3aaaa", fullscreen=True), width=12),
            dbc.Col(html.Div(id='hosts-table', className="data-table"), width=12)
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col(html.H5("Selected host: "), width='auto', id="selected", className='selected-text'),
                    dbc.Col(html.H5(html.Div(id='selected-host')), width='auto')
                    ]),
                dbc.Row(html.Div(html.P('Tests in the past 24 hours '), className='desc-text', id="description"))
            ])
        ], justify="around", className="header"),
        dbc.Row([
            dbc.Col(dcc.Loading(className='loading-component', id="loader", color="#b3aaaa"))
        ]),
        dbc.Row([
            dbc.Col([
                html.H5("Host as source", id="as_src_txt",className="data-table"),
                html.Div(
                        dash_table.DataTable(
                            id='datatable-src', 
                            style_data_conditional= tmpl.host_table_cond,
                            style_cell_conditional=tmpl.host_table_cell,
                            style_header=tmpl.host_table_header,
                            style_cell={'font-family':'sans-serif'},
                            sort_action="native",
                            sort_mode="multi",
                            page_action="native",
                            page_current= 0,
                            page_size= 10
                        ),
                        className="data-table")
                ]),
            dbc.Col([
                html.H5("Host as destination", id="as_dest_txt", className="data-table"),
                html.Div(
                        dash_table.DataTable(
                            id='datatable-dest', 
                            style_data_conditional= tmpl.host_table_cond,
                            style_cell_conditional=tmpl.host_table_cell,
                            style_header=tmpl.host_table_header,
                            style_cell={'font-family':'sans-serif'},
                            sort_action="native",
                            sort_mode="multi",
                            page_action="native",
                            page_current= 0,
                            page_size= 10
                        ),
                        className="data-table")
            ])
        ]),
    ]
)


@app.callback([Output('count', 'children'),
               Output('hosts-table', 'children'),
               Output('loader1', 'loading_state')],
              [Input('radioitems-period', 'value'),
               Input('read_from_db', 'value')],
             prevent_initial_call=False)
def update_hostsTable(period, read_from_db):
    if (read_from_db == True):
        dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
        dateFrom = datetime.strftime(datetime.now() - timedelta(hours = period), '%Y-%m-%d %H:%M')
        df_tab = build.LossDelayTestCountGroupedbyHost(dateFrom, dateTo)
    else:
        df_tab = pd.read_csv("data/LossDelayTestCountGroupedbyHost-"+str(period)+".csv")
    df_tab['id'] = df_tab['host']
    df_tab.set_index('host', inplace=True, drop=False)
    return u'''Number of hosts for the period: {}'''.format(len(df_tab)), hosts_table(df_tab), False

@app.callback(
    [Output('selected-host', 'children'),
     Output('selected', 'style'),
     Output('description', 'style'),
     Output('as_src_txt', 'style'),
     Output('as_dest_txt', 'style'),],
    [Input('datatable-row-ids', 'active_cell')])
def get_host(active_cell):
    active_row_id = active_cell['row_id'] if active_cell else None
    if active_row_id:
        show_css = {'display': 'block', 'padding': '0 5px'} 
        return active_row_id, show_css, show_css, show_css, show_css
    else: raise dash.exceptions.PreventUpdate

@app.callback(
     [Output('datatable-src', 'data'),
      Output('datatable-src', 'columns'),
      Output('datatable-dest', 'data'),
      Output('datatable-dest', 'columns'),
      Output('loader', 'loading_state')],
     [Input('selected-host', 'children')])
def get_details(host):
    if host is not None:
        data = build.SrcDestTables(host)
        as_source = data[0]
        as_destination = data[1]
        columns=[{"name": i, "id": i} for i in as_source.columns]
        return as_source.to_dict('records'), columns, as_destination.to_dict('records'), columns, False
    else:
        raise dash.exceptions.PreventUpdate


app.run_server(debug=False)
