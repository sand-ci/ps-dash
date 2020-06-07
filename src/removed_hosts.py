import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_table

import DatasetBuilder as build
import templates as tmpl


df = build.RemovedHosts()

layout = html.Div([
            dbc.Row([
                dbc.Col([
                    html.H5(children="The following list describes the hosts which data was removed due to an error",
                            className='removed-hosts-title'
                           ),
                    dash_table.DataTable(
                        id='table',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict("rows"),
                        style_data_conditional= tmpl.host_table_cond,
                        style_cell_conditional=tmpl.host_table_cell,
                        style_header=tmpl.host_table_header,
                        style_cell={'font-family':'sans-serif'},
                        sort_action="native",
                        sort_mode="multi",
                        page_action="native",
                        editable=True,
                        export_format='csv',
                        export_headers='display'
                    )
                ], className="base")
            ])
        ])