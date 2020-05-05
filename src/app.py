import plotly.express as px
import dash
import dash_table
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State

import pandas as pd
import datetime as dt

import getpass
import templates as tmpl
import DatasetBuilder as build


username = getpass.getuser()
isDev = True if username == 'petya' else False

external_stylesheets = [dbc.themes.BOOTSTRAP]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


toDT  = dt.datetime.now()
fromDT= toDT + dt.timedelta(days=-1)


app.layout = html.Div(
    [
        dbc.Row([
            dbc.Col(html.H4(id='count'), width=7),
            dbc.Col([
                dbc.Row([
                    dbc.Col(html.P("From:"), className="dt-name"),
                    dbc.Col(dcc.Input(id='fromDate', type='text', value=fromDT.strftime('%Y-%m-%d %H:%M'), debounce=True, className="dt-input"))
                ])
            ], width=1.5),
            dbc.Col([
                dbc.Row([
                     dbc.Col(html.P("To:"), className="dt-name"),
                     dbc.Col(dcc.Input(id='toDate', type='text', value=toDT.strftime('%Y-%m-%d %H:%M'), debounce=True, className="dt-input"))
                ])
            ], width=1.5)
        ], no_gutters = True, justify="around", className="header"),
        html.Div(id='hosts-table',  className="data-table"),
    ]
)

@app.callback([Output('count', 'children'), Output('hosts-table', 'children')],
              [Input(component_id='fromDate', component_property='value'), Input(component_id='toDate', component_property='value')])
def update_hostsTable(dateFrom, dateTo):
    df_tab = build.CountTestsGroupedByHost(isDev, dateFrom, dateTo)
    return u'''Number of hosts for the period "{}"'''.format(len(df_tab)), dash_table.DataTable(
                                                                                columns=[{"name": i, "id": i} for i in df_tab.columns],
                                                                                data=df_tab.to_dict('records'),
                                                                                style_data_conditional= tmpl.host_table_cond,
                                                                                style_header=tmpl.host_table_header,
                                                                                style_cell={'font-family':'sans-serif'},
                                                                                sort_action="native",
                                                                                sort_mode="multi",
                                                                                page_action="native",
                                                                                page_current= 0,
                                                                                page_size= 10
                                                                            )
# app.config.suppress_callback_exceptions = True
app.enable_dev_tools(debug=True, dev_tools_props_check=False)

app.run_server(port=80, debug=True)
