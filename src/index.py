import dash
import dash_table
import dash_daq as daq
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc

import templates as tmpl

layout = html.Div(
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
            dbc.Col(html.P('Click on a row to select a host', className="data-table"), width=12),
            dbc.Col(html.Div(
                            dash_table.DataTable(
                                id='datatable-row-ids',
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
                    className="data-table"
            ), width=12)
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col(html.H5("Selected host: "), width='auto', id="selected", className='selected-text'),
                    dbc.Col(html.H5(html.Div(id='selected-host')), width='auto')
                    ]),
                dbc.Row(
                    html.Div(
                        html.P('Tested pairs in the past 24 hours. Click on a row to select a pair.'),
                        className='desc-text', id="description"
                    )
                )
            ])
        ], justify="around", className="header"),
        dbc.Row([
            dbc.Col(dcc.Loading(className='loading-component', id="loader", color="#b3aaaa"))
        ]),
        html.Div(id='test'),
        html.Div(id='page-content'),
        dcc.Location(id='change-url'),
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