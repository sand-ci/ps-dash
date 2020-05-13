import plotly.express as px
import dash
import dash_table
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State

import pandas as pd
from datetime import datetime, timedelta

import templates as tmpl
import DatasetBuilder as build

external_stylesheets = [dbc.themes.BOOTSTRAP, "style.css"]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

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
            dbc.Col(html.H4(id='count'), width=9),
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
            dbc.Col(dcc.Loading(loading_state={'is_loading':True}, 
                                className='loading-component1', id="loader1", color="#b3aaaa"), width=12),
            dbc.Col(html.Div(id='hosts-table', className="data-table"), width=12)
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col(html.H5("Selected host: "), width='auto', id="selected", className='selected-text'),
                    dbc.Col(html.H5(html.Div(id='selected-host')), width='auto')
                    ]),
                dbc.Row(html.Div(html.P('Packet loss, delay, number of tests for the past hour.'), className='desc-text', id="description"))
            ])
        ], justify="around", className="header"),
        dbc.Row([
            dbc.Col(dcc.Loading(loading_state={'is_loading':True}, className='loading-component', id="loader", color="#b3aaaa"))
        ]),
        html.Div(
                dash_table.DataTable(
                    id='datatable-details', 
                    style_data_conditional= tmpl.host_table_cond,
                    style_cell_conditional=tmpl.host_table_cell,
                    style_header=tmpl.host_table_header,
                    style_cell={'font-family':'sans-serif'},
                    sort_action="native",
                    sort_mode="multi",
                    page_action="native",
                    page_current= 0,
                    page_size= 20
                ),
                className="data-table"),
    ]
)


<<<<<<< HEAD
@app.callback([Output('count', 'children'),
               Output('hosts-table', 'children'),
               Output('loader1', 'is_loading')],
              [Input(component_id='radioitems-period', component_property='value')])
def update_hostsTable(period):
    df_tab = pd.read_csv("data/LossDelayTestCountGroupedbyHost-"+str(period)+".csv")
#     dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
#     dateFrom = datetime.strftime(datetime.now() - timedelta(hours = period), '%Y-%m-%d %H:%M')
#     df_tab = build.LossDelayTestCountGroupedbyHost(dateFrom, dateTo)
    df_tab['id'] = df_tab['host']
    df_tab.set_index('host', inplace=True, drop=False)
    return u'''Number of hosts for the period: {}'''.format(len(df_tab)), hosts_table(df_tab), False

@app.callback(
    [Output('selected-host', 'children'),
     Output(component_id='selected', component_property='style'),
     Output(component_id='description', component_property='style')],
    [Input('datatable-row-ids', 'active_cell')])
def get_host(active_cell):
    active_row_id = active_cell['row_id'] if active_cell else None
    if active_row_id:
        show_selected_style = {'display': 'block', 'padding': '0 5px'} 
    return active_row_id, show_selected_style, show_selected_style

@app.callback(
    [Output('datatable-details', 'data'),
     Output('datatable-details', 'columns'),
     Output('loader', 'is_loading')],
    [Input('selected-host', 'children')])
def get_details(host):
#     print('>>>>>', host, type(host))
    if (host is not None) or (host != '') or (host != 'None'):
        print('--------------------')
        dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
        dateFrom = datetime.strftime(datetime.now() - timedelta(hours = 1), '%Y-%m-%d %H:%M')
        df_tab = build.SrcDestLossDelayNTP(host, dateFrom, dateTo)
        columns=[{"name": i, "id": i} for i in df_tab.columns]
        return df_tab.to_dict('records'), columns, False
    else:
        return '', '', False


app.config.suppress_callback_exceptions = True
app.enable_dev_tools(debug=False, dev_tools_props_check=False)

app.run_server(debug=False)
=======
app.run_server(port=80, debug=True)
>>>>>>> ff56085066db17e1c9eeb24f688e2d8df01e8538
