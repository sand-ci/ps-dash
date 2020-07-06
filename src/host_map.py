import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_table
import plotly.graph_objects as go
import pandas as pd

import templates as tmpl
import queries as qrs
from DataLoader import GeneralDataLoader

gobj = GeneralDataLoader()

all_df = gobj.all_df[['ip', 'is_ipv6', 'host', 'site', 'admin_email', 'admin_name', 'ip_in_ps_meta',
                 'host_in_ps_meta', 'host_index', 'site_index', 'host_meta', 'site_meta']].sort_values(by=['ip_in_ps_meta', 'host_in_ps_meta', 'ip'], ascending=False)



fig = go.Figure()
fig.add_trace(go.Histogram(x=all_df['site'],
                    y=all_df[all_df['is_ipv6']==False]['ip'],
                    name='IPv4',
                    marker_color='rgb(55, 83, 109)'
                    ))
fig.add_trace(go.Histogram(x=all_df['site'],
                    y=all_df[all_df['is_ipv6']==True]['ip'],
                    name='IPv6',
                    marker_color='rgb(26, 118, 255)'
                    ))
fig.layout.template = 'plotly_white'
fig.update_layout(
        title_text='Number of IPs for each site', # title of plot
        template = 'plotly_white'
    )
    
fig1 = go.Figure()
fig1.add_trace(go.Histogram(x=all_df['site'],
                    y=all_df['host'],
                    name='hosts',
                    marker_color='#1985a1'
                    ))
fig1.update_layout(
        title_text='Number of hosts for each site', # title of plot
        template = 'plotly_white'
    )

layout_all = html.Div([
            dbc.Row(
                dbc.Col(
                    html.Div(id='datatable-interactivity-container', children=dcc.Graph(figure=fig))
                )
            ),
            dbc.Row(
                dbc.Col(
                    html.Div(id='datatable-interactivity-container1', children=dcc.Graph(figure=fig1))
                )
            ),
            dbc.Row(
                dbc.Col(
                    html.H3(className="gi-title", children="The following dataset provides information for all hosts in the main indices: ps_packetloss, ps_owd, ps_retransmits, and ps_throughput. "
                           )
                , width=12), justify="around",
            ),
            dbc.Row(
                dbc.Col(
                    dash_table.DataTable(
                        id='datatable-interactivity',
                        columns=[{"name": i, "id": i} for i in all_df.columns],
                        data=all_df.to_dict("rows"),
                        style_header=tmpl.host_table_header,
                        style_cell={'font-family':'sans-serif'},
                        style_cell_conditional=tmpl.gen_info_table_cell,
                        style_table={'overflowX': 'scroll'},
                    )
                , width=12), justify="around",
            )
        ])

