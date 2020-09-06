import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_table
import plotly.graph_objects as go
import pandas as pd

import view.templates as tmpl
import model.queries as qrs
import utils.helpers
from model.DataLoader import GeneralDataLoader
from model.DataLoader import HostDataLoader

gobj = GeneralDataLoader()


all_df = gobj.all_df_related_only[['ip', 'is_ipv6', 'host', 'site', 'admin_email', 'admin_name', 'ip_in_ps_meta',
                 'host_in_ps_meta', 'host_index', 'site_index', 'host_meta', 'site_meta']].sort_values(by=['ip_in_ps_meta', 'host_in_ps_meta', 'ip'], ascending=False)
# obj = HostDataLoader()


import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_table

import view.templates as tmpl


gobj = GeneralDataLoader()
all_df = gobj.all_df_related_only[['ip', 'is_ipv6', 'host', 'site', 'admin_email', 'admin_name', 'ip_in_ps_meta',
                 'host_in_ps_meta', 'host_index', 'site_index', 'host_meta', 'site_meta']].sort_values(by=['ip_in_ps_meta', 'host_in_ps_meta', 'ip'], ascending=False)

obj = HostDataLoader()

indeces = ['all', 'ps_packetloss', 'ps_owd', 'ps_retransmits', 'ps_throughput']

indx_dict = {'ps_packetloss': 'Packet loss', 'ps_owd': 'One-way delay',
             'ps_retransmits': 'Retransmits', 'ps_throughput': 'Throughput'}

PROBLEM_LIST = {'high_sigma':'The most problematic pairs', 'has_bursts':'Pairs showing bursts',
            'all_packets_lost': 'Pairs with 100% packets lost'}

problems = obj.df.copy()
# problems = pd.read_csv('df.csv')
problems = problems.round(3)

high_sigma = problems[(problems['high_sigma'] == 1) & ((problems['src_not_in'] == 0) | (problems['dest_not_in'] == 0))].sort_values(by=['zscore', 'doc_count'], ascending=False)
has_bursts = problems[(problems['has_bursts'] == 1) & ((problems['src_not_in'] == 0) | (problems['dest_not_in'] == 0))].sort_values(by=['max_hash_zscore', 'doc_count'], ascending=False)
all_packets_lost = problems[(problems['all_packets_lost'] == 1) & ((problems['src_not_in'] == 0) | (problems['dest_not_in'] == 0))].sort_values(by=['all_packets_lost', 'doc_count'], ascending=False)



def getPairs(pair_type, row):
    if pair_type == 'host':
        source = row[row[['host_src', 'src']].first_valid_index()]
        destination = row[row[['host_dest', 'dest']].first_valid_index()]
        return str(source +' --> '+ destination)
    elif pair_type == 'site':
        src_site = row['site_src']
        dest_site = row['site_dest']
        if src_site != src_site:
            src_site = 'N/A'
        if dest_site != dest_site:
            dest_site = 'N/A'
        return src_site+' --> '+dest_site


def generateTable(item, df):
    print(item, len(df) )
    df = df[df['idx']==item][['host_src', 'src', 'site_src', 'host_dest', 'dest', 'site_dest', 'measures', 'value']]
    return  dash_table.DataTable(
                id=str(item+'prob'),
                columns=[{"name": i, "id": i} for i in df.columns],
                data=df.to_dict("rows"),
                style_header=tmpl.host_table_header,
                style_cell={'font-family':'sans-serif', 'font-size': '11px'},
                style_cell_conditional=tmpl.gen_info_table_cell,
                sort_action='native',
                filter_action='native',
                page_size=10
            )


def showAll(df):
    return dbc.Row(
                [ 
                dbc.Col([html.H2(indx_dict[item], className="index-title"),
                            generateTable(item, df)], width=12) for item in indeces[1:]
                ], justify="center") 


def showIndex(idx, df):
    return dbc.Row([
            dbc.Col(generateTable(idx, df))
            ], justify="center")



problems_layout = html.Div([
                dbc.Row([
                     dbc.Col(
                        dcc.Tabs(id='tabs-prob-types', className='prob-types', value='high_sigma',
                                 children=[
                                    dcc.Tab(label=v.upper(), value=k) for k, v in PROBLEM_LIST.items()
                                ])
                     , width={"size": 8, "offset": 2})
                ]),
                dbc.Row([
                    dbc.Col([
                        dcc.Tabs(
                            id='tabs-indeces', className='indeces-types', value='all', 
                            children=[
                                    dcc.Tab(label=item.upper(), value=item) for item in indeces],
                                             vertical=True, style={'float': 'left'}),
                        dbc.Row([
                            dbc.Col([
                                    html.P(f'Last updated: {gobj.lastUpdated}', id="last-update", className="last-update")
                            ], align="center"),
                            dbc.Col([
                                dcc.Interval(
                                        id='update',
                                        interval=1*450, # in milliseconds
                                        n_intervals=0,
                                        max_intervals=0,
                                    ),
                            ], align="center"),
                        ], align="center"),
                    ], width={"size": 2}),
                    dbc.Col([
                        html.Div(id='tabs-content')
                    ], width=9)
                ])
            ])